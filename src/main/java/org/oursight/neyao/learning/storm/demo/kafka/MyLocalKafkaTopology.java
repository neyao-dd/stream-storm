package org.oursight.neyao.learning.storm.demo.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by neyao on 2017/4/26.
 */
public class MyLocalKafkaTopology {

    private static final String id = "e789ec38-6e44-45eb-8d0e-5b31af41fc3d";

    private static Log logger = LogFactory.getLog(MyLocalKafkaTopology.class);

    public MyLocalKafkaTopology() {
        super();
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
//        builder

        BrokerHosts hosts = new ZkHosts("192.168.0.200:30011");
        String topicName = "mytest-topic-2";

        // 需要事先在zookeeper中创建/{zkRoot}/{topicName}/{id} 这个节点
        // 例如本例子中应该创建：/storm_kafka/mytest-topic-2/e789ec38-6e44-45eb-8d0e-5b31af41fc3c
        String zkRoot = "/storm_kafka";


        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, zkRoot + "/" + topicName, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("kafka", kafkaSpout, 1);
//        builder.setBolt("parser", new ParserBolt("http://192.168.1.208:5160", "api/v1/task/finish_content", 0), 4).shuffleGrouping("kafka");
        builder.setBolt("printer", new MyPrinterBolt()).shuffleGrouping("kafka");

        Config conf = new Config();
        conf.setDebug(false);

        conf.setNumWorkers(3);

        try {

            System.out.println();
            System.out.println();
            System.out.println("--------------------------");
            System.out.println("will submit to storm");

            logger.error("============================================");
            logger.error("WILL SUBMIT TO STORM");


            //StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

            conf.setDebug(true);
            String topologyName = "my-local-topo";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());

//            Utils.sleep(3600*1000L); //KILL the topology
//            cluster.killTopology(topologyName);
//            cluster.shutdown();

            System.out.println("submit to storm done");
            logger.error("SUBMIT TO STORM DONE");
            logger.error("============================================");
            System.out.println("--------------------------");
            System.out.println();
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

