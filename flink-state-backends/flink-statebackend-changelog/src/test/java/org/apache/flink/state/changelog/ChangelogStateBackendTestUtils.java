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
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.util.Collections;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Test Utilities for Changelog StateBackend. */
public class ChangelogStateBackendTestUtils {

    public static <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            AbstractStateBackend stateBackend,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return new ChangelogStateBackend(stateBackend)
                .createKeyedStateBackend(
                        env,
                        new JobID(),
                        "test_op",
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());
    }

    public static void assertValueState(
            CheckpointableKeyedStateBackend<?> backend, Class<?> stateClass) throws Exception {
        try {
            ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            assertTrue(state instanceof ChangelogValueState);
            assertSame(
                    ((ChangelogValueState<?, ?, ?>) state).getDelegatedValueState().getClass(),
                    stateClass);
        } finally {
            backend.dispose();
            backend.close();
        }
    }

    public static void assertMapState(
            CheckpointableKeyedStateBackend<?> backend, Class<?> stateClass) throws Exception {
        try {
            MapStateDescriptor<Integer, String> kvId =
                    new MapStateDescriptor<>("id", Integer.class, String.class);

            MapState<Integer, String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            assertTrue(state instanceof ChangelogMapState);
            assertSame(
                    ((ChangelogMapState<?, ?, ?, ?>) state).getDelegatedMapState().getClass(),
                    stateClass);
        } finally {
            backend.dispose();
            backend.close();
        }
    }

    public static void assertListState(
            CheckpointableKeyedStateBackend<?> backend, Class<?> stateClass) throws Exception {
        try {
            ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

            ListState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            assertTrue(state instanceof ChangelogListState);
            assertSame(
                    ((ChangelogListState<?, ?, ?>) state).getDelegatedListState().getClass(),
                    stateClass);
        } finally {
            backend.dispose();
            backend.close();
        }
    }

    public static void assertReducingState(
            CheckpointableKeyedStateBackend<?> backend, Class<?> stateClass) throws Exception {
        try {
            ReducingStateDescriptor<String> kvId =
                    new ReducingStateDescriptor<>(
                            "id", (value1, value2) -> value1 + "," + value2, String.class);

            ReducingState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            assertTrue(state instanceof ChangelogReducingState);
            assertSame(
                    ((ChangelogReducingState<?, ?, ?>) state)
                            .getDelegatedReducingState()
                            .getClass(),
                    stateClass);
        } finally {
            backend.dispose();
            backend.close();
        }
    }

    public static void assertAggregatingState(
            CheckpointableKeyedStateBackend<?> backend, Class<?> stateClass) throws Exception {
        try {
            final AggregatingStateDescriptor<Long, StateBackendTestBase.MutableLong, Long>
                    stateDescr =
                            new AggregatingStateDescriptor<>(
                                    "my-state",
                                    new StateBackendTestBase.MutableAggregatingAddingFunction(),
                                    StateBackendTestBase.MutableLong.class);

            AggregatingState<Long, Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

            assertTrue(state instanceof ChangelogAggregatingState);
            assertSame(
                    ((ChangelogAggregatingState<?, ?, ?, ?, ?>) state)
                            .getDelegatedAggregatingState()
                            .getClass(),
                    stateClass);
        } finally {
            backend.dispose();
            backend.close();
        }
    }
}
