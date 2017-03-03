package org.oursight.neyao.learning.storm.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by neyao on 2017/3/3.
 */
public class MyTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("words", new MySpout(), 1);
        builder.setBolt("bolt1", new MyBolt(), 1).shuffleGrouping("words");
        builder.setBolt("bolt2", new MyBolt(), 1).shuffleGrouping("bolt1");

        Config conf = new Config();
        conf.setDebug(true);

        // Run as local
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("MyTestTopology", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("MyTestTopology");
        cluster.shutdown();

    }
}
