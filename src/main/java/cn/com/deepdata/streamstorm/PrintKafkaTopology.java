/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.com.deepdata.streamstorm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import cn.com.deepdata.streamstorm.bolt.ParserBolt;
import cn.com.deepdata.streamstorm.bolt.PrinterBolt;

/**
 * This is a basic example of a Storm topology.
 */
public class PrintKafkaTopology {
	private static final String id = "e789ec38-6e44-45eb-8d0e-5b31af41fc3c";

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		BrokerHosts hosts = new ZkHosts("slave1:2181");
		String topicName = "flume-event-topic";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/"
				+ topicName, id);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		builder.setSpout("kafka", kafkaSpout, 8);
		builder.setBolt("parser", new ParserBolt("http://192.168.1.208:5160", "api/v1/task/finish_content"), 4).shuffleGrouping("kafka");
		builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("parser");

		Config conf = new Config();
		conf.setDebug(false);

		conf.setNumWorkers(3);

		StormSubmitter.submitTopologyWithProgressBar("PrintKafkaTopology",
				conf, builder.createTopology());
	}
}
