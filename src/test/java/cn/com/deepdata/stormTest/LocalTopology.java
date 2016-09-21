package cn.com.deepdata.stormTest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import cn.com.deepdata.streamstorm.bolt.PrinterBolt;
import cn.com.deepdata.streamstorm.spout.RandomSentenceSpout;

public class LocalTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("out", new RandomSentenceSpout());
		builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("out");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TestTopology", new Config(),
				builder.createTopology());
		Utils.sleep(10000);
		cluster.shutdown();
	}

}
