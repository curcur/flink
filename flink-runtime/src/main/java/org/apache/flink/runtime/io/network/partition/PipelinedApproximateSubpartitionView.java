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

package org.apache.flink.runtime.io.network.partition;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * View over a pipelined in-memory only subpartition allowing reconnecting.
 */
public class PipelinedApproximateSubpartitionView extends PipelinedSubpartitionView {

	PipelinedApproximateSubpartitionView(PipelinedApproximateSubpartition parent, BufferAvailabilityListener listener) {
		super(parent, listener);
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			checkState(parent instanceof PipelinedApproximateSubpartition);
			((PipelinedApproximateSubpartition) parent).releaseView();
		}
	}

	@Override
	public String toString() {
		return String.format("PipelinedApproximateSubpartition(index: %d) of ResultPartition %s",
			parent.getSubPartitionIndex(),
			parent.parent.getPartitionId());
	}
}
