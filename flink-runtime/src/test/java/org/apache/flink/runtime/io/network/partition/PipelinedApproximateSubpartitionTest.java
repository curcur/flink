package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;

import org.junit.Test;

import java.io.IOException;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest.assertContent;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest.toByteBuffer;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PipelinedApproximateSubpartition}.
 */
public class PipelinedApproximateSubpartitionTest extends PipelinedSubpartitionTest {
	private static final int BUFFER_SIZE = 4 * Integer.BYTES;

	@Override
	PipelinedSubpartition createSubpartition() throws Exception {
		return createPipelinedApproximateSubpartition();
	}

	@Test
	@Override
	public void testIllegalReadViewRequest() {
		// This is one of the main differences between PipelinedApproximateSubpartition and PipelinedSubpartition
		// PipelinedApproximateSubpartition allows to recreate a view (release the old view first)
	}

	@Test
	public void testRecreateReadView() throws Exception {
		final PipelinedApproximateSubpartition subpartition = createPipelinedApproximateSubpartition();

		// first request
		assertNotNull(subpartition.createReadView(() -> {}));
		assertFalse(subpartition.isPartialBuffer());

		// reconnecting request
		assertNotNull(subpartition.createReadView(() -> {}));
		assertTrue(subpartition.isPartialBuffer());
	}

	@Test
	public void testSkipPartialDataEndsInBufferWithNoMoreData() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 42), 0);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);

		subpartition.releaseView();
		assertNull(subpartition.pollBuffer());

		writer.emitRecord(toByteBuffer(8, 9), 0);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 8, 9);
	}

	@Test
	public void testSkipPartialDataEndsInBufferWithMoreData() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 42), 0);
		writer.emitRecord(toByteBuffer(8, 9), 0);

		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);

		subpartition.releaseView();
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 8, 9);
	}

	@Test
	public void testSkipPartialDataStartWithFullRecord() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 42), 0);
		writer.emitRecord(toByteBuffer(8, 9), 0);

		subpartition.releaseView();

		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 42, 8, 9);
	}

	@Test
	public void testSkipPartialDataStartWithinBuffer() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 42), 0);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 42);

		writer.emitRecord(toByteBuffer(8, 9), 0);
		writer.emitRecord(toByteBuffer(10, 11), 0);

		subpartition.releaseView();
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 8, 9, 10);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 11);
	}

	@Test
	public void testSkipPartialDataLongRecordOccupyEntireBuffer() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 42), 0);

		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);

		subpartition.releaseView();
		assertNull(subpartition.pollBuffer());
	}

	@Test
	public void testSkipPartialDataLongRecordOccupyEntireBufferWithMoreData() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 42), 0);
		writer.emitRecord(toByteBuffer(100, 101, 102), 0);

		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);

		subpartition.releaseView();
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 100, 101);

		// release again
		subpartition.releaseView();
		// 102 is cleaned up
		assertNull(subpartition.pollBuffer());

		writer.emitRecord(toByteBuffer(200, 201, 202, 203), 0);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 200, 201, 202);
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 203);
	}

	@Test
	public void testSkipPartialDataLongRecordEndWithBuffer() throws Exception {
		final BufferWritingResultPartition writer = createResultPartition();
		final PipelinedApproximateSubpartition subpartition = getPipelinedApproximateSubpartition(writer);

		writer.emitRecord(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 42), 0);
		writer.emitRecord(toByteBuffer(100, 101, 102), 0);

		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 0, 1, 2, 3);

		subpartition.releaseView();
		assertContent(requireNonNull(subpartition.pollBuffer()).buffer(), null, 100, 101, 102);
	}

	private static PipelinedApproximateSubpartition createPipelinedApproximateSubpartition() throws IOException {
		final BufferWritingResultPartition parent = createResultPartition();
		return (PipelinedApproximateSubpartition) parent.subpartitions[0];
	}

	private static PipelinedApproximateSubpartition getPipelinedApproximateSubpartition(
			BufferWritingResultPartition resultPartition) {
		return (PipelinedApproximateSubpartition) resultPartition.subpartitions[0];
	}

	private static BufferWritingResultPartition createResultPartition() throws IOException {
		NettyShuffleEnvironment network = new NettyShuffleEnvironmentBuilder()
			.setNumNetworkBuffers(10)
			.setBufferSize(BUFFER_SIZE)
			.build();
		ResultPartition resultPartition =
			createPartition(network, NoOpFileChannelManager.INSTANCE, ResultPartitionType.PIPELINED_APPROXIMATE, 2);
		resultPartition.setup();
		return (BufferWritingResultPartition) resultPartition;
	}
}
