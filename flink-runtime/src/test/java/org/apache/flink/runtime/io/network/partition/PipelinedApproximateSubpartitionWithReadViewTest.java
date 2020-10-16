package org.apache.flink.runtime.io.network.partition;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Additional tests for {@link PipelinedApproximateSubpartitionView} which require an availability listener and a
 * read view.
 *
 * @see PipelinedSubpartitionTest
 */
public class PipelinedApproximateSubpartitionWithReadViewTest extends PipelinedSubpartitionWithReadViewTest {

	@Before
	@Override
	public void before() throws IOException {
		setup(ResultPartitionType.PIPELINED_APPROXIMATE);
		subpartition = new PipelinedApproximateSubpartition(0, resultPartition);
		availablityListener = new AwaitableBufferAvailablityListener();
		readView = subpartition.createReadView(availablityListener);
	}

	@Test
	@Override
	public void testRelease() {
		readView.releaseAllResources();
		assertTrue(
			resultPartition.getPartitionManager().getUnreleasedPartitions().contains(resultPartition.getPartitionId()));
	}
}
