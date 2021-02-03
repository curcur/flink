/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.runtime.state.proxy.ProxyStateBackend;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class contains utility methods to load state backends from configurations. */
public class StateBackendLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateBackendLoader.class);
    // ------------------------------------------------------------------------
    //  Configuration shortcut names
    // ------------------------------------------------------------------------

    private static final Set<String> STATE_BACKEND_CAN_BE_PROXIED =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.contrib.streaming.state.RocksDBStateBackend",
                            "org.apache.flink.runtime.state.memory.MemoryStateBackend",
                            "org.apache.flink.runtime.state.filesystem.FsStateBackend"));

    /**
     * The shortcut configuration name for the MemoryState backend that checkpoints to the
     * JobManager
     */
    public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

    /** The shortcut configuration name for the FileSystem State backend */
    public static final String FS_STATE_BACKEND_NAME = "filesystem";

    /** The shortcut configuration name for the RocksDB State Backend */
    public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

    // ------------------------------------------------------------------------
    //  Loading the state backend from a configuration
    // ------------------------------------------------------------------------

    /**
     * Loads the state backend from the configuration, from the parameter 'state.backend', as
     * defined in {@link CheckpointingOptions#STATE_BACKEND}.
     *
     * <p>The state backends can be specified either via their shortcut name, or via the class name
     * of a {@link StateBackendFactory}. If a StateBackendFactory class name is specified, the
     * factory is instantiated (via its zero-argument constructor) and its {@link
     * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are '{@value StateBackendLoader#MEMORY_STATE_BACKEND_NAME}',
     * '{@value StateBackendLoader#FS_STATE_BACKEND_NAME}', and '{@value
     * StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}'.
     *
     * @param config The configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    public static StateBackend loadStateBackendFromConfig(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        checkNotNull(config, "config");
        checkNotNull(classLoader, "classLoader");

        final String backendName = config.get(CheckpointingOptions.STATE_BACKEND);
        if (backendName == null) {
            return null;
        }

        // by default the factory class is the backend name
        String factoryClassName = backendName;

        switch (backendName.toLowerCase()) {
            case MEMORY_STATE_BACKEND_NAME:
                MemoryStateBackend memBackend =
                        new MemoryStateBackendFactory().createFromConfig(config, classLoader);

                if (logger != null) {
                    Path memExternalized = memBackend.getCheckpointPath();
                    String extern =
                            memExternalized == null
                                    ? ""
                                    : " (externalized to " + memExternalized + ')';
                    logger.info(
                            "State backend is set to heap memory (checkpoint to JobManager) {}",
                            extern);
                }
                return memBackend;

            case FS_STATE_BACKEND_NAME:
                FsStateBackend fsBackend =
                        new FsStateBackendFactory().createFromConfig(config, classLoader);
                if (logger != null) {
                    logger.info(
                            "State backend is set to heap memory (checkpoints to filesystem \"{}\")",
                            fsBackend.getCheckpointPath());
                }
                return fsBackend;

            case ROCKSDB_STATE_BACKEND_NAME:
                factoryClassName =
                        "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory";
                // fall through to the 'default' case that uses reflection to load the backend
                // that way we can keep RocksDB in a separate module

            default:
                if (logger != null) {
                    logger.info("Loading state backend via factory {}", factoryClassName);
                }

                StateBackendFactory<?> factory;
                try {
                    @SuppressWarnings("rawtypes")
                    Class<? extends StateBackendFactory> clazz =
                            Class.forName(factoryClassName, false, classLoader)
                                    .asSubclass(StateBackendFactory.class);

                    factory = clazz.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new DynamicCodeLoadingException(
                            "Cannot find configured state backend factory class: " + backendName,
                            e);
                } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                    throw new DynamicCodeLoadingException(
                            "The class configured under '"
                                    + CheckpointingOptions.STATE_BACKEND.key()
                                    + "' is not a valid state backend factory ("
                                    + backendName
                                    + ')',
                            e);
                }

                return factory.createFromConfig(config, classLoader);
        }
    }

    /**
     * Checks if an application-defined state backend is given, and if not, loads the state backend
     * from the configuration, from the parameter 'state.backend', as defined in {@link
     * CheckpointingOptions#STATE_BACKEND}. If no state backend is configured, this instantiates the
     * default state backend (the {@link MemoryStateBackend}).
     *
     * <p>If an application-defined state backend is found, and the state backend is a {@link
     * ConfigurableStateBackend}, this methods calls {@link
     * ConfigurableStateBackend#configure(ReadableConfig, ClassLoader)} on the state backend.
     *
     * <p>Refer to {@link #loadStateBackendFromConfig(ReadableConfig, ClassLoader, Logger)} for
     * details on how the state backend is loaded from the configuration.
     *
     * @param config The configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    public static StateBackend fromApplicationOrConfigOrDefault(
            @Nullable StateBackend fromApplication,
            Configuration config,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        checkNotNull(config, "config");
        checkNotNull(classLoader, "classLoader");

        final StateBackend backend;

        // (1) the application defined state backend has precedence
        if (fromApplication != null) {

            checkArgument(
                    !(fromApplication instanceof ProxyStateBackend),
                    "Proxy state backend can not be proxied!");

            // see if this is supposed to pick up additional configuration parameters
            if (fromApplication instanceof ConfigurableStateBackend) {
                // needs to pick up configuration
                if (logger != null) {
                    logger.info(
                            "Using job/cluster config to configure application-defined state backend: {}",
                            fromApplication);
                }

                backend =
                        ((ConfigurableStateBackend) fromApplication).configure(config, classLoader);
            } else {
                // keep as is!
                backend = fromApplication;
            }

            if (logger != null) {
                logger.info("Using application-defined state backend: {}", backend);
            }
        } else {
            // (2) check if the config defines a state backend
            final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, logger);
            if (fromConfig != null) {
                backend = fromConfig;
            } else {
                // (3) use the default
                backend = new MemoryStateBackendFactory().createFromConfig(config, classLoader);
                if (logger != null) {
                    logger.info(
                            "No state backend has been configured, using default (Memory / JobManager) {}",
                            backend);
                }
            }
        }

        return backend;
    }

    public static StateBackend loadStateBackend(
            @Nullable StateBackend fromApplication,
            Configuration config,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        final StateBackend backend =
                fromApplicationOrConfigOrDefault(fromApplication, config, classLoader, logger);

        if (needToProxyStateBackend(backend, config)) {
            LOG.info(
                    "Proxy State Backend used, and the root State Backend is {}",
                    backend.getClass().getSimpleName());
            return new ProxyStateBackend(backend);
        } else {
            LOG.info(
                    "Proxy State Backend not used, and the State Backend is {}",
                    backend.getClass().getSimpleName());
            return backend;
        }
    }

    private static boolean needToProxyStateBackend(StateBackend backend, Configuration config) {
        return config.get(CheckpointingOptions.ENABLE_STATE_CHANGE_LOG)
                && STATE_BACKEND_CAN_BE_PROXIED.contains(backend.getClass().getCanonicalName());
    }

    /**
     * Checks whether state backend uses managed memory, without having to deserialize or load the
     * state backend.
     *
     * @param config Cluster configuration.
     * @param stateBackendFromApplicationUsesManagedMemory Whether the application-defined backend
     *     uses Flink's managed memory. Empty if application has not defined a backend.
     * @param classLoader User code classloader.
     * @return Whether the state backend uses managed memory.
     */
    public static boolean stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(
            Configuration config,
            Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
            ClassLoader classLoader) {

        checkNotNull(config, "config");

        // (1) the application defined state backend has precedence
        if (stateBackendFromApplicationUsesManagedMemory.isPresent()) {
            return stateBackendFromApplicationUsesManagedMemory.get();
        }

        // (2) check if the config defines a state backend
        try {
            final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, LOG);
            if (fromConfig != null) {
                return fromConfig.useManagedMemory();
            }
        } catch (IllegalConfigurationException | DynamicCodeLoadingException | IOException e) {
            LOG.warn(
                    "Cannot decide whether state backend uses managed memory. Will reserve managed memory by default.",
                    e);
            return true;
        }

        // (3) use the default MemoryStateBackend
        return false;
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated */
    private StateBackendLoader() {}
}
