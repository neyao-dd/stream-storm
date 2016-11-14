package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.commonutil.SimHash;
import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.commonutil.TokensInfo;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import com.google.gson.Gson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yukh on 2016/11/11
 */
public class SimHashBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(SimHashBolt.class);
    private transient DeepRichBoltHelper helper;
    String[] uselessLineHead = { "相关新闻：", "分享到：", "免责声明：" };
    TermFrequencyInfo titleTfi;
    List<TermFrequencyInfo> contentTfi;
    List<List<String>> content;

    String getSite(Map<String, Object> source) {
        String site = "";
        if (source.containsKey("snp_site")) {
            site = source.get("snp_site").toString().trim();
        }
        return site;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            helper = new DeepRichBoltHelper(outputCollector);
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Map<String, Object> source = helper.getDoc(tuple);
            Map<String, Object> attach = helper.getAttach(tuple);
            init(attach);
            String simhash = Analyze(source);
            source.put("sna_simhash", simhash);
            helper.emitDoc(tuple, source, true);
        } catch (Exception e) {
            logger.error("calc simhash error...\n" + CommonUtil.getExceptionString(e));
            helper.emitAttach(tuple, helper.getAttach(tuple), true);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

    private void init(Map<String, Object> attach) {
        titleTfi = (TermFrequencyInfo) attach.get("titleTermInfo");
        contentTfi = (List<TermFrequencyInfo>) attach.get("contentTermInfo");
        content = (List<List<String>>) attach.get("contentRaw");
    }

    private boolean validSegment(boolean filter, String site, List<String> segment) {
        return !filter && CommonUtil.validString(site) && !contains(segment, site);
    }

    private boolean contains(List<String> segment, String s) {
        for (String sentence : segment)
            if (sentence.contains(s))
                return true;
        return false;
    }

    public String Analyze(Map<String, Object> source) {
        AnsjTermAnalyzer analyzer = new AnsjTermAnalyzer();
        String site = getSite(source);
        TokensInfo tokensInfo = new TokensInfo();
        tokensInfo.tokens = new ArrayList<>(0);
        TermFrequencyInfo tfi;
        for (int i = 0; i < content.size(); i++) {
            List<String> segment = content.get(i);
            boolean filter = false;
            if (segment.isEmpty())
                continue;
            for (String useless : uselessLineHead) {
                if (segment.get(0).startsWith(useless)) {
                    filter = true;
                    break;
                }
            }
            if (validSegment(filter, site, segment)) {
                tfi = contentTfi.get(i);
                for (int x = 0; x < tfi.tokens.size(); x++) {
                    List<String> line = new ArrayList<>(0);
                    for (int y = 0; y < tfi.tokens.get(x).size(); y++) {
                        String name = tfi.tokens.get(x).get(y);
                        if (!analyzer.isStopTerm(tfi.termNature.get(name), name)) {
                            line.add(name);
                        }
                    }
                    tokensInfo.tokens.add(line);
                }
            }
        }
        try {
            return new SimHash(new Gson().toJson(tokensInfo), 64).toString();
        } catch (IOException e) {
            logger.error("calc simhash error...\n" + CommonUtil.getExceptionString(e));
            return "";
        }
    }
}
