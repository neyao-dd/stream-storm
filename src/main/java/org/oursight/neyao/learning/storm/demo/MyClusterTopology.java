package org.oursight.neyao.learning.storm.demo;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by neyao on 2017/3/3.
 */
public class MyClusterTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("words", new MySpout(), 1);
        builder.setBolt("bolt1", new MyBolt(), 1).shuffleGrouping("words");
        builder.setBolt("bolt2", new MyBolt(), 1).shuffleGrouping("bolt1");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);



        // Run as local
//        conf.setDebug(true);
//        String topologyName = "my-test-local-topo";
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(topologyName, conf, builder.createTopology());
//        Utils.sleep(10000); //KILL the topology
//        cluster.killTopology(topologyName);
//        cluster.shutdown();

        // Run as cluster
        String topologyName = args[0];
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());




    }
}
