package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.SuccessException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.test.util.TestUtils.tryExecute;

/**
 * Tests for failover.
 */
public class ApproximateLocalRecoveryITCase {
	private static MiniClusterWithClientResource cluster;

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "individual");
		configuration.setString(HighAvailabilityOptions.HA_MODE, RegionFailoverITCase.TestingHAFactory.class.getName());

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(2).build());
		cluster.before();
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	@Test
	public void localTaskFailureRecovery() throws Exception {
		int numElementsPerTask = 1000;
		int producerParallelism = 1;
		int failAfterElements = numElementsPerTask / 3;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0));
		env.setBufferTimeout(0);
		env.disableOperatorChaining();
		// env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

		DataStream<Tuple3<Integer, Long, Integer>> source =
			env.addSource(new AppSourceFunction(numElementsPerTask)).setParallelism(producerParallelism);

		source.map(new FailingMapper<>(failAfterElements)).setParallelism(1)
			.map(new ToInteger(producerParallelism)).setParallelism(1)
			.addSink(new ValidatingExactlyOnceSink(numElementsPerTask * producerParallelism)).setParallelism(1);

		FailingMapper.failedBefore = false;
		tryExecute(env, "test");
	}

	// Schema: (key, timestamp, source instance Id).
	private static class AppSourceFunction extends RichParallelSourceFunction<Tuple3<Integer, Long, Integer>>
		implements ListCheckpointed<Integer> {
		private static final Logger LOG = LoggerFactory.getLogger(AppSourceFunction.class);
		private volatile boolean running = true;
		private final int numElementsPerProducer;

		private int index = 0;

		AppSourceFunction(int numElementsPerProducer) {
			this.numElementsPerProducer = numElementsPerProducer;
		}

		@Override
		public void run(SourceContext<Tuple3<Integer, Long, Integer>> ctx) throws Exception{
			long timestamp = 1593575900000L;
			int sourceInstanceId = getRuntimeContext().getIndexOfThisSubtask();
			for (; index < numElementsPerProducer && running; index++) {
				synchronized (ctx.getCheckpointLock()) {
					if (index % (numElementsPerProducer / 10) == 0) {
						Thread.sleep(500);
					}
					System.out.println("Source : [" + index + "," + timestamp + "," + sourceInstanceId + "]");
					ctx.collect(new Tuple3<>(index, timestamp++, sourceInstanceId));
				}
			}

			while (running) {
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of Source index " + index + " at checkpoint " + checkpointId);
			return Collections.singletonList(index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			index = state.get(0);
			LOG.info("restore Source index to " + index);
		}
	}

	private static class FailingMapper<T> extends RichMapFunction<T, T> implements
		ListCheckpointed<Integer>, CheckpointListener, Runnable {

		private static final Logger LOG = LoggerFactory.getLogger(FailingMapper.class);

		private static final long serialVersionUID = 6334389850158707313L;

		public static volatile boolean failedBefore;

		private final int failCount;
		private int numElementsTotal;
		private int numElementsThisTime;

		private boolean failer;
		private boolean hasBeenCheckpointed;

		private Thread printer;
		private volatile boolean printerRunning = true;

		public FailingMapper(int failCount) {
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
			printer = new Thread(this, "FailingIdentityMapper Status Printer");
			printer.start();
		}

		@Override
		public T map(T value) throws Exception {
			System.out.println("Failing mapper: numElementsThisTime=" + numElementsThisTime + " totalCount=" + numElementsTotal);
			numElementsTotal++;
			numElementsThisTime++;

			if (!failedBefore) {
				Thread.sleep(10);

				if (failer && numElementsTotal >= failCount) {
					failedBefore = true;
					throw new Exception("Artificial Test Failure");
				}
			}
			return value;
		}

		@Override
		public void close() throws Exception {
			printerRunning = false;
			if (printer != null) {
				printer.interrupt();
				printer = null;
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			this.hasBeenCheckpointed = true;
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of FailingMapper numElementsTotal " + numElementsTotal + " at checkpoint " + checkpointId);
			return Collections.singletonList(numElementsTotal);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			this.numElementsTotal = state.get(0);
			LOG.info("restore FailingMapper numElementsTotal to " + numElementsTotal);
		}

		@Override
		public void run() {
			while (printerRunning) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// ignore
				}
				LOG.info("============================> Failing mapper  {}: count={}, totalCount={}",
					getRuntimeContext().getIndexOfThisSubtask(),
					numElementsThisTime, numElementsTotal);
			}
		}
	}

	private static class ToInteger extends RichMapFunction<Tuple3<Integer, Long, Integer>, Integer> implements Runnable {
		private static final Logger LOG = LoggerFactory.getLogger(ToInteger.class);
		private final int producerParallelism;
		private int count;

		private Thread printer;
		private volatile boolean printerRunning = true;

		ToInteger(int producerParallelism) {
			this.producerParallelism = producerParallelism;
		}

		@Override
		public void open(Configuration parameters) {
			printer = new Thread(this, "ToInteger Status Printer");
			printer.start();
		}

		@Override
		public Integer map(Tuple3<Integer, Long, Integer> element) throws Exception {
			count++;
			System.out.println("ToInteger : [" + element.f0 + "," + element.f1 + "," + element.f2 + "]");
			return element.f0 * producerParallelism + element.f2;
		}

		@Override
		public void run() {
			while (printerRunning) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// ignore
				}
				LOG.info("============================> ToInteger mapper  {}: count={}",
					getRuntimeContext().getIndexOfThisSubtask(), count);
			}
		}

		@Override
		public void close() throws Exception {
			printerRunning = false;
			if (printer != null) {
				printer.interrupt();
				printer = null;
			}
		}
	}

	private static class ValidatingExactlyOnceSink extends RichSinkFunction<Integer> implements ListCheckpointed<Tuple2<Integer, BitSet>> {

		private static final Logger LOG = LoggerFactory.getLogger(ValidatingExactlyOnceSink.class);

		private static final long serialVersionUID = 1748426382527469932L;

		private final int numElementsTotal;

		private BitSet duplicateChecker = new BitSet();  // this is checkpointed

		private int numElements; // this is checkpointed

		public ValidatingExactlyOnceSink(int numElementsTotal) {
			this.numElementsTotal = numElementsTotal;
		}

		@Override
		public void invoke(Integer value) throws Exception {
			numElements++;

			System.out.println("ValidatingExactlyOnceSink : [" + value + "]");

			if (duplicateChecker.get(value)) {
				throw new Exception("Received a duplicate: " + value);
			}
			duplicateChecker.set(value);
			if (numElements == numElementsTotal) {
				// validate
				if (duplicateChecker.cardinality() != numElementsTotal) {
					throw new Exception("Duplicate checker has wrong cardinality");
				}
				else if (duplicateChecker.nextClearBit(0) != numElementsTotal) {
					throw new Exception("Received sparse sequence");
				}
				else {
					throw new SuccessException();
				}
			}
		}

		@Override
		public List<Tuple2<Integer, BitSet>> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of ValidatingExactlyOnceSink numElements " + numElements + " at checkpoint " + checkpointId);
			return Collections.singletonList(new Tuple2<>(numElements, duplicateChecker));
		}

		@Override
		public void restoreState(List<Tuple2<Integer, BitSet>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			Tuple2<Integer, BitSet> s = state.get(0);
			LOG.info("restoring ValidatingExactlyOnceSink num elements to {}", s.f0);
			this.numElements = s.f0;
			this.duplicateChecker = s.f1;
		}
	}
}
