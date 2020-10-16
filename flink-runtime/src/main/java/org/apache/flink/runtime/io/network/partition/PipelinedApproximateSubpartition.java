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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which allows to reconnecting after failure.
 * Only one view is allowed at a time to read teh subpartition.
 */
public class PipelinedApproximateSubpartition extends PipelinedSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedApproximateSubpartition.class);

	private boolean isPartialBuffer = false;

	PipelinedApproximateSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) {
		synchronized (buffers) {
			checkState(!isReleased);

			// if the view is not released yet
			if (readView != null) {
				LOG.info("{} ReadView for Subpartition {} of {} has not been released!",
					parent.getOwningTaskName(), getSubPartitionIndex(), parent.getPartitionId());
				releaseView();
			}

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), getSubPartitionIndex(), parent.getPartitionId());

			readView = new PipelinedApproximateSubpartitionView(this, availabilityListener);
		}

		return readView;
	}

	@Override
	Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
		if (isPartialBuffer) {
			isPartialBuffer = !buffer.cleanupPartialRecord();
		}

		return buffer.build();
	}

	void releaseView() {
		LOG.info("Releasing view of subpartition {} of {}.", getSubPartitionIndex(), parent.getPartitionId());
		readView = null;
		isPartialBuffer = true;
		isBlockedByCheckpoint = false;
		sequenceNumber = 0;
	}

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedApproximateSubpartition#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
			getSubPartitionIndex(), numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}

	@VisibleForTesting
	public boolean isPartialBuffer() {
		return isPartialBuffer;
	}
}
