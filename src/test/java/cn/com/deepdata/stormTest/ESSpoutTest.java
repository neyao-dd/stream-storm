package cn.com.deepdata.stormTest;

import cn.com.deepdata.streamstorm.bolt.PrinterBolt;
import cn.com.deepdata.streamstorm.spout.ESSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by hdd on 1/13/17.
 */
public class ESSpoutTest {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("out",
                new ESSpout("119.254.86.82:19200", "flume-2016-11-24-content-news/flumetype", "?q=sns_monthId:AViU4nO90PVhP5ldKatA"));
        builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("out");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TestTopology", new Config(),
                builder.createTopology());
        Utils.sleep(50000);
        cluster.shutdown();
    }
}
