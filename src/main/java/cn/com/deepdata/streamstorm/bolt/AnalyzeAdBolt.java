package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.TermFrequencyInfo;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yukh on 2016/10/19
 */
public class AnalyzeAdBolt extends BaseRichBolt {
    private transient static Log log = LogFactory.getLog(AnalyzeAdBolt.class);
    private transient DeepRichBoltHelper helper;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        helper = new DeepRichBoltHelper(outputCollector);
    }

    @Override
    public void execute(Tuple tuple) {
        Gson gson = new Gson();
        Map<String, Object> attach = helper.getAttach(tuple);
        Set<String> adWords = new HashSet<>();
        if (attach.containsKey("titleTermInfo") && attach.containsKey("contentTermInfo")) {
            TermFrequencyInfo titleTfi = gson.fromJson(attach.get("titleTermInfo").toString(), TermFrequencyInfo.class);
            TermFrequencyInfo contentTfi = gson.fromJson(attach.get("contentTermInfo").toString(), TermFrequencyInfo.class);
            String result = adFilter(adWords, titleTfi.termNature, contentTfi.termNature);
            if (result.length() > 2) {
                attach.put("isAd", true);
                attach.put("adWords", adWords);
            } else
                attach.put("isAd", false);
        }
        // TODO: 2016/10/20 tag
        helper.emitAttach(tuple, attach, true);
        helper.ack(tuple);
    }

    private String adFilter(Set<String> adWords, Map<String, String>...nature) {
        for (Map<String, String> m : nature) {
            m.forEach((k, v) -> {
                if ("gg".equals(k))
                    adWords.add(v);
            });
        }
        return adWords.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}