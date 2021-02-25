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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.delegate.DelegateStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Verify Changelog StateBackend is properly loaded. */
public class ChangelogStateBackendLoadingTest {
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

    //    @Test
    //    public void testCheckpointStorageWithMemoryStateBackend() throws Exception {
    //
    //        Configuration config = new Configuration();
    //        config.set(ENABLE_STATE_CHANGE_LOG, true);
    //        config.setString("state.backend", "jobmanager");
    //
    //        StateBackend stateBackend = StateBackendLoader.loadStateBackend(null, config, cl,
    // null);
    //
    //        CheckpointStorage configured =
    //                CheckpointStorageLoader.load(null, null, stateBackend, config, cl, null);
    //    }

    @Test
    public void testLoadingDefault() throws Exception {
        final StateBackend backend = StateBackendLoader.loadStateBackend(null, config(), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, HashMapStateBackend.class, storage, JobManagerCheckpointStorage.class);
    }

    @Test
    public void testLoadingMemoryStateBackendFromConfig() throws Exception {
        final Configuration config = config("jobmanager");
        final StateBackend backend = StateBackendLoader.loadStateBackend(null, config, cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config, cl, null);

        assertDelegateStateBackend(
                backend, MemoryStateBackend.class, storage, MemoryStateBackend.class);
    }

    @Test
    public void testLoadingFsStateBackendFromConfig() throws Exception {
        final Configuration config = config("filesystem");
        final StateBackend backend = StateBackendLoader.loadStateBackend(null, config, cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config, cl, null);

        assertDelegateStateBackend(
                backend, HashMapStateBackend.class, storage, JobManagerCheckpointStorage.class);
    }

    @Test
    public void testLoadingHashMapStateBackendFromConfig() throws Exception {
        final Configuration config = config("hashmap");
        final StateBackend backend = StateBackendLoader.loadStateBackend(null, config, cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, HashMapStateBackend.class, storage, JobManagerCheckpointStorage.class);
    }

    @Test
    public void testLoadingRocksDBStateBackendFromConfig() throws Exception {
        final Configuration config = config("rocksdb");
        final StateBackend backend = StateBackendLoader.loadStateBackend(null, config, cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend,
                EmbeddedRocksDBStateBackend.class,
                storage,
                JobManagerCheckpointStorage.class);
    }

    @Test
    public void testApplicationDefinedHasPrecedence() throws Exception {
        final StateBackend appBackend = new MockStateBackend();
        // "rocksdb" should not take effect
        final StateBackend backend =
                StateBackendLoader.loadStateBackend(appBackend, config("rocksdb"), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, MockStateBackend.class, storage, MockStateBackend.class);
    }

    private Configuration config(String stateBackend) {
        final Configuration config = config();
        config.setString(backendKey, stateBackend);

        return config;
    }

    private Configuration config() {
        final Configuration config = new Configuration();
        config.setBoolean(CheckpointingOptions.ENABLE_STATE_CHANGE_LOG, true);

        return config;
    }

    private void assertDelegateStateBackend(
            StateBackend backend,
            Class<?> delegatedStateBackendClass,
            CheckpointStorage storage,
            Class<?> storageClass) {
        assertTrue(backend instanceof ChangelogStateBackend);
        assertSame(
                ((DelegateStateBackend) backend).getDelegatedStateBackend().getClass(),
                delegatedStateBackendClass);
        assertSame(storage.getClass(), storageClass);
    }

    private static class MockStateBackend extends AbstractStateBackend
            implements CheckpointStorage {

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws IOException {
            return null;
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return null;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return null;
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return null;
        }
    }
}
