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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

/**
 * Delegated partitioned {@link ListState} that forwards changes to {@link StateChange} upon {@link
 * ListState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class ChangelogListState<K, N, V> implements InternalListState<K, N, V> {
    private final InternalListState<K, N, V> listState;

    ChangelogListState(InternalListState<K, N, V> listState) {
        this.listState = listState;
    }

    @Override
    public void update(List<V> values) throws Exception {
        listState.update(values);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        listState.addAll(values);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        listState.updateInternal(valueToStore);
    }

    @Override
    public void add(V value) throws Exception {
        listState.add(value);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        listState.mergeNamespaces(target, sources);
    }

    @Override
    public List<V> getInternal() throws Exception {
        return listState.getInternal();
    }

    @Override
    public Iterable<V> get() throws Exception {
        return listState.get();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return listState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return listState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return listState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        listState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer)
            throws Exception {
        return listState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return listState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public void clear() {
        listState.clear();
    }

    public InternalListState<K, N, V> getDelegatedListState() {
        return listState;
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> listState) {
        return (IS) new ChangelogListState<>((InternalListState<K, N, SV>) listState);
    }
}
