package org.oursight.neyao.learning.storm.demo;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by neyao on 2017/3/3.
 */
public class MySpout extends BaseRichSpout {

    public static Logger logger = LoggerFactory.getLogger(MySpout.class);

    boolean isDistributed;
    SpoutOutputCollector collector;

    public MySpout() {
        this(false);
    }



    public MySpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
        logger.info("MySpout.declareOutputFields: Field declared");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        logger.info("MySpout.open, conf: " + conf + "; context: " + context + "; collector: " + collector);
        this.collector = collector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(false == isDistributed) {
            return null;
        }

        Map<String, Object> configuration = new HashMap<String, Object>();
        configuration.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return configuration;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
        final Random rand = new Random();
        final String wordToEmit = words[rand.nextInt(words.length)];
        collector.emit(new Values(wordToEmit));
        logger.info("MySpout.nextTuple: word emitted: " + wordToEmit);

    }

    @Override
    public void close() {
        super.close();
        logger.info("MySpout.close done");
    }
}
