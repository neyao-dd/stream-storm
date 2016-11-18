package cn.com.deepdata.streamstorm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import cn.com.deepdata.streamstorm.bolt.EsBolt;
import cn.com.deepdata.streamstorm.bolt.ParserBolt;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ESTopology {

	private static final String id = "e789ec38-6e44-45eb-8d0e-5b31af41fc3c";
	final static int tickTupleFreqSecs = 10;

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub
		TopologyBuilder builder = new TopologyBuilder();

		BrokerHosts hosts = new ZkHosts("slave1:2181");
		String topicName = "flume-event-topic";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/"
				+ topicName, id);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		builder.setSpout("kafka", kafkaSpout, 8);
		builder.setBolt("parser", new ParserBolt(), 4).shuffleGrouping("kafka");

		Map esConf = new HashMap();
		esConf.put("es.batch.size.bytes", "20mb");
		esConf.put("es.storm.bolt.flush.entries.size", "50");
		builder.setBolt("es-bolt-month",
				new EsBolt("storm-test2/flumetype", esConf))
				.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
						tickTupleFreqSecs).shuffleGrouping("parser");
		builder.setBolt("es-bolt",
				new EsBolt("storm-test1/flumetype", esConf))
				.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
						tickTupleFreqSecs).shuffleGrouping("es-bolt-month");

		Config conf = new Config();
		conf.setDebug(false);

		conf.setNumWorkers(8);

		StormSubmitter.submitTopologyWithProgressBar("ES-Index", conf,
				builder.createTopology());
	}
}
