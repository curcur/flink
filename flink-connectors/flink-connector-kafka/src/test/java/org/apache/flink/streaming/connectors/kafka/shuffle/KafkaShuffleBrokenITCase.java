/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.IngestionTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;
import static org.apache.flink.streaming.connectors.kafka.shuffle.KafkaShuffleITCase.testRecordSerDe;


/**
 * Simple End to End Broken Test for Kafka.
 */
public class KafkaShuffleBrokenITCase extends KafkaShuffleTestBase {

	@Rule
	public final Timeout timeout = Timeout.millis(600000L);

	/**
	 * To test value serialization and deserialization with time characteristic: ProcessingTime.
	 *
	 * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
	 */
	@Test
	public void testSerDeProcessingTime() throws Exception {
		testRecordSerDe(ProcessingTime);
	}

	/**
	 * To test value and watermark serialization and deserialization with time characteristic: IngestionTime.
	 *
	 * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
	 */
	@Test
	public void testSerDeIngestionTime() throws Exception {
		testRecordSerDe(IngestionTime);
	}

	/**
	 * To test value and watermark serialization and deserialization with time characteristic: EventTime.
	 *
	 * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
	 */
	@Test
	public void testSerDeEventTime() throws Exception {
		testRecordSerDe(EventTime);
	}
}
