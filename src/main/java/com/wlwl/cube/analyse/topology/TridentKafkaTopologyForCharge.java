/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */
package com.wlwl.cube.analyse.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;

import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.spout.TridentKafkaSpout;
import com.wlwl.cube.ananlyse.spout.TridentKafkaSpoutForCharge;

/**
 * A sample word count trident topology using transactional kafka spout that has
 * the following components.
 * <ol>
 * <li>{@link KafkaBolt} that receives random sentences from
 * {@link RandomSentenceSpout} and publishes the sentences to a kafka "test"
 * topic.</li>
 * <li>{@link TransactionalTridentKafkaSpout} that consumes sentences from the
 * "test" topic, splits it into words, aggregates and stores the word count in a
 * {@link MemoryMapState}.</li>
 * <li>DRPC query that returns the word counts by querying the trident state
 * (MemoryMapState).</li>
 * </ol>
 * <p>
 * For more background read the <a href=
 * "https://storm.apache.org/documentation/Trident-tutorial.html">trident
 * tutorial</a>,
 * <a href="https://storm.apache.org/documentation/Trident-state">trident
 * state</a> and
 * <a href="https://github.com/apache/storm/tree/master/external/storm-kafka">
 * Storm Kafka </a>.
 * </p>
 */
public class TridentKafkaTopologyForCharge {

	/**
	 * <p>
	 * To run this topology ensure you have a kafka broker running.
	 * </p>
	 * Create a topic 'test' with command line,
	 * 
	 * <pre>
	 * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
	 * </pre>
	 * 
	 * To run in local mode,
	 * 
	 * <pre>
	 * storm jar storm-starter-topologies-{version}.jar org.apache.storm.starter.trident.TridentKafkaWordCount
	 * </pre>
	 * 
	 * This will also run a local DRPC query and print the word counts.
	 * <p>
	 * To run in distributed mode, run it with a topology name. You will also
	 * need to start a drpc server and specify the drpc server details
	 * storm.yaml before submitting the topology.
	 * </p>
	 * 
	 * <pre>
	 * storm jar storm-starter-topologies-{version}.jar org.apache.storm.starter.trident.TridentKafkaWordCount zkhost:port broker:port wordcount
	 * </pre>
	 * 
	 * This will submit two topologies, one for the producer and another for the
	 * consumer. You can query the results (word counts) by running an external
	 * drpc query against the drpc server.
	 */
	public static void main(String[] args) throws Exception {

		String zkUrl =  Conf.ZKURL;// "master:2181,node1:2181";
																																			// //
																																			// the
																																			// defaults.
		String brokerUrl = Conf.BROKERURL; // "node3:9092,node1:9092,node2:9092";//

//		if (args.length > 3 || (args.length == 1 && args[0].matches("^-h|--help$"))) {
//			System.out.println("Usage: TridentKafkaWordCount [kafka zookeeper url] [kafka broker url] [topology name]");
//			System.out.println("   E.g TridentKafkaWordCount [" + zkUrl + "]" + " [" + brokerUrl + "] [wordcount]");
//			System.exit(1);
//		} else if (args.length == 1) {
//			zkUrl = args[0];
//		} else if (args.length == 2) {
//			zkUrl = args[0];
//			brokerUrl = args[1];
//		}

		System.out.println("Using Kafka zookeeper url: " + zkUrl + " broker url: " + brokerUrl);
		TridentKafkaSpoutForCharge kafkaInstance = new TridentKafkaSpoutForCharge(zkUrl, brokerUrl, "test");
		if (args.length == 1) {
			Config conf = new Config();
			conf.setMaxSpoutPending(20);
			conf.setMessageTimeoutSecs(60);
			conf.setNumWorkers(1);
			// submit the consumer topology.
			StormSubmitter.submitTopology(args[0] + "-consumer", conf, kafkaInstance.buildConsumerTopology(null));
			// submit the producer topology.
//			StormSubmitter.submitTopology(args[2] + "-producer", conf,
//					kafkaInstance.buildProducerTopology(kafkaInstance.getProducerConfig()));
		} else {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();

			// submit the consumer topology.
			cluster.submitTopology("VehicleCounter", kafkaInstance.getConsumerConfig(),
					kafkaInstance.buildConsumerTopology(drpc));
//			Config conf = new Config();
//			conf.setMaxSpoutPending(20);
//		    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);// 设置本Bolt定时发射数据  
			// submit the producer topology.
//			cluster.submitTopology("kafkaBolt", conf,
//					kafkaInstance.buildProducerTopology(kafkaInstance.getProducerConfig()));

			// keep querying the word counts for a minute.
			for (int i = 0; i < 60; i++) {
//				System.out.println(
//						"##################DRPC RESULT: " + drpc.execute("words", "the and apple snow jumped"));
				Thread.sleep(1000);
			}

			cluster.killTopology("kafkaBolt");
			cluster.killTopology("wordCounter");
			cluster.shutdown();
		}
	}
}
