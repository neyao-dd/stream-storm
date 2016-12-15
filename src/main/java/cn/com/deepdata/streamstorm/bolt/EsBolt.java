package cn.com.deepdata.streamstorm.bolt;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by yukh on 2016/12/14
 */
public class EsBolt extends org.elasticsearch.storm.EsBolt {
    private static transient Logger logger = LoggerFactory.getLogger(EsBolt.class);
    private transient int tickFreq;

    public EsBolt(String target, Map configuration, int tickFreq) {
        super(target, configuration);
        this.tickFreq = tickFreq;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreq);
        return config;
    }
}
