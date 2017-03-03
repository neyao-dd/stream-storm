package org.oursight.neyao.learning.storm.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by neyao on 2017/3/3.
 */
public class MyBolt extends BaseRichBolt {

    public static Logger logger = LoggerFactory.getLogger(MyBolt.class);

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("stormConf = [" + stormConf + "], context = [" + context + "], collector = [" + collector + "]");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        logger.info("MyBolt.execute, input Tuple: " + input );
        collector.emit(new Values(input.getString(0) +"!!!"));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        logger.info("MyBolt.declareOutputFields, declarer: " + declarer);
        declarer.declare(new Fields("word"));
    }
}
