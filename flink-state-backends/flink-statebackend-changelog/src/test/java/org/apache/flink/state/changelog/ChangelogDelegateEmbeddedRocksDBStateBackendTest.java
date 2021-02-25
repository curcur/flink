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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendTest;
import org.apache.flink.contrib.streaming.state.RocksDBAggregatingState;
import org.apache.flink.contrib.streaming.state.RocksDBListState;
import org.apache.flink.contrib.streaming.state.RocksDBMapState;
import org.apache.flink.contrib.streaming.state.RocksDBReducingState;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBValueState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.junit.Test;

import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.assertAggregatingState;
import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.assertListState;
import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.assertMapState;
import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.assertReducingState;
import static org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.assertValueState;

/** Tests for {@link ChangelogStateBackend} delegating {@link RocksDBStateBackend}. */
public class ChangelogDelegateEmbeddedRocksDBStateBackendTest
        extends EmbeddedRocksDBStateBackendTest {
    @Override
    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return ChangelogStateBackendTestUtils.createKeyedBackend(
                getStateBackend(), keySerializer, numberOfKeyGroups, keyGroupRange, env);
    }

    @Test
    public void testDelegatedValueState() throws Exception {
        assertValueState(createKeyedBackend(IntSerializer.INSTANCE), RocksDBValueState.class);
    }

    @Test
    public void testDelegatedMapState() throws Exception {
        assertMapState(createKeyedBackend(IntSerializer.INSTANCE), RocksDBMapState.class);
    }

    @Test
    public void testDelegatedListState() throws Exception {
        assertListState(createKeyedBackend(IntSerializer.INSTANCE), RocksDBListState.class);
    }

    @Test
    public void testDelegatedReducingState() throws Exception {
        assertReducingState(createKeyedBackend(IntSerializer.INSTANCE), RocksDBReducingState.class);
    }

    @Test
    public void testDelegatedAggregatingState() throws Exception {
        assertAggregatingState(
                createKeyedBackend(IntSerializer.INSTANCE), RocksDBAggregatingState.class);
    }
}
