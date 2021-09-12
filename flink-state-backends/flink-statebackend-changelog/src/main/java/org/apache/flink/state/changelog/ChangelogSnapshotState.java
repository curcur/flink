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

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;

import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Snapshot State for ChangelogKeyedStatebackend, IMPORTANT: only accessed by Task Thread.
 *
 * <p>It includes three parts: - materialized snapshot from the underlying delegated state backend -
 * non-materialized part in the current changelog - non-materialized changelog, from previous logs
 * (before failover or rescaling)
 */
public class ChangelogSnapshotState {
    /**
     * Materialized snapshot from the underlying delegated state backend. Set initially on restore
     * and later upon materialization.
     */
    private final List<KeyedStateHandle> materializedSnapshot;

    /**
     * The {@link SequenceNumber} up to which the state is materialized, exclusive. This indicates
     * the non-materialized part of the current changelog.
     */
    private final SequenceNumber materializedTo;

    /**
     * Non-materialized changelog, from previous logs. Set initially on restore and later cleared
     * upon materialization.
     */
    private final List<ChangelogStateHandle> restoredNonMaterialized;

    public ChangelogSnapshotState(
            List<KeyedStateHandle> materializedSnapshot,
            List<ChangelogStateHandle> restoredNonMaterialized,
            SequenceNumber materializedTo) {
        this.materializedSnapshot = checkNotNull(unmodifiableList((materializedSnapshot)));
        this.restoredNonMaterialized = checkNotNull(unmodifiableList(restoredNonMaterialized));
        this.materializedTo = materializedTo;
    }

    public List<KeyedStateHandle> getMaterializedSnapshot() {
        return materializedSnapshot;
    }

    public SequenceNumber lastMaterializedTo() {
        return materializedTo;
    }

    public List<ChangelogStateHandle> getRestoredNonMaterialized() {
        return restoredNonMaterialized;
    }
}
