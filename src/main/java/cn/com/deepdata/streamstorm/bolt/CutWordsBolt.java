package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.streamstorm.controller.RedisKeys;
import cn.com.deepdata.streamstorm.controller.UsrDefineWordsController;
import cn.com.deepdata.streamstorm.entity.RiskFields;
import cn.com.deepdata.streamstorm.util.StormUtil;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"serial", "rawtypes"})
public class CutWordsBolt extends AbstractRedisBolt {
    private transient static Log log = LogFactory.getLog(CutWordsBolt.class);
    private transient DeepRichBoltHelper helper;
    private transient static AnsjTermAnalyzer ansjAnalyzer;
    private transient static byte[] syncWordsLock;
    private transient static Long lastSyncTime;
    private transient static UsrDefineWordsController clientWordsCtrl;
    private transient static UsrDefineWordsController riskWordsCtrl;
    private transient static UsrDefineWordsController indRegRiskWordsCtrl;
    private transient static UsrDefineWordsController adWordsCtrl;

    public CutWordsBolt(JedisPoolConfig config) {
        super(config);
    }

    public CutWordsBolt(JedisClusterConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        helper = new DeepRichBoltHelper(collector);
        ansjAnalyzer = new AnsjTermAnalyzer();
        syncWordsLock = new byte[0];
        lastSyncTime = 0L;
        clientWordsCtrl = new UsrDefineWordsController(
                RedisKeys.CreateRedisKey(RedisKeys.Type.kClient), "CT");
        riskWordsCtrl = new UsrDefineWordsController(
                RedisKeys.CreateRedisKey(RedisKeys.Type.kNetRisk), "RT");
        indRegRiskWordsCtrl = new UsrDefineWordsController(
                RedisKeys.CreateRedisKey(RedisKeys.Type.kIndRegRisk), "risk");
        adWordsCtrl = new UsrDefineWordsController(
                RedisKeys.CreateRedisKey(RedisKeys.Type.kAd), "gg");
        LoadWords();
    }

    private void LoadWords() {
        synchronized (syncWordsLock) {
            if (System.currentTimeMillis() - lastSyncTime < 60 * 1000)
                return;
            DoLoadWords();
        }
    }

    public void DoLoadWords() {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            clientWordsCtrl.load(jedisCommands);
            riskWordsCtrl.load(jedisCommands);
            indRegRiskWordsCtrl.load(jedisCommands);
            adWordsCtrl.loadRedisWordsWithNoVersion(jedisCommands,
                    RiskFields.adTokenSetKey);

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
        }
        lastSyncTime = System.currentTimeMillis();
    }

    private String deleteStartSpace(String content) {
        content = content.trim();
        int len = content.length();
        int st = 0;
        // 空格 12288 160 32
        while ((st < len)
                && ((content.charAt(st) == ('　')) || content.charAt(st) == ' ' || content
                .charAt(st) == ' ')) {
            st++;
        }
        return st > 0 ? content.substring(st, len) : content;
    }

    @Override
    public void execute(Tuple input) {
        Gson gson = new Gson();
        LoadWords();
        String titleRaw = deleteStartSpace(helper.getDocTitle(input));
        TermFrequencyInfo titleTermInfo = gson.fromJson(ansjAnalyzer.getTermFrequencyInfo(titleRaw,
                AnsjTermAnalyzer.CutType.kCutTo), TermFrequencyInfo.class);
        String content = helper.getDocContent(input);
        List<List<String>> contentRaw = Lists.newArrayList();
        List<List<String>> contentTermInfo = Lists.newArrayList();
        String[] segments = content.split("\r|\n");
        if (segments.length > 0) {
            Arrays.asList(segments)
                    .stream()
                    .forEach(
                            seg -> {
                                String segRaw = deleteStartSpace(seg);
                                String[] sentences = segRaw
                                        .split("。|；|！|!|？|\\?");
                                List<String> segmentRaw = Lists.newArrayList();
                                List<String> segmentTermInfo = Lists
                                        .newArrayList();
                                if (sentences.length > 0) {
                                    Arrays.asList(sentences)
                                            .stream()
                                            .forEach(
                                                    sent -> {
                                                        segmentRaw.add(sent);
                                                        segmentTermInfo
                                                                .add(ansjAnalyzer
                                                                        .getTermFrequencyInfo(
                                                                                sent,
                                                                                AnsjTermAnalyzer.CutType.kCutTo));
                                                    });
                                }
                                contentRaw.add(segmentRaw);
                                contentTermInfo.add(segmentTermInfo);
                            });
        }
        Map<String, Object> attach = helper.getAttach(input);
        attach.put("titleRaw", titleRaw);
        attach.put("titleTermInfo", titleTermInfo);
        attach.put("contentRaw", contentRaw);
        attach.put("contentTermInfo", combine(contentTermInfo, contentRaw));
        helper.emitAttach(input, attach, true);
        helper.ack(input);
    }

    private TermFrequencyInfo combine(List<List<String>> cut, List<List<String>> segments) {
        Gson gson = new Gson();
        int position = 0;
        HashMap<String, Integer> frequency = new HashMap<>();
        HashMap<String, String> nature = new HashMap<>();
        HashMap<String, List<Integer>> offset = new HashMap<>();
        TermFrequencyInfo tfiR = new TermFrequencyInfo(1);
        for (int i = 0; i < cut.size(); i++) {
            List<String> tempSeg = cut.get(i);
            List<String> sentences = segments.get(i);
            for (int j = 0; j < tempSeg.size(); j++) {
                TermFrequencyInfo tfi = gson.fromJson(tempSeg.get(j), TermFrequencyInfo.class);
                nature.putAll(tfi.termNature);
                StormUtil.parseMap(frequency, tfi.termFrequency[0]);
                StormUtil.parseMap(offset, tfi.termOffsets, position);
                position += sentences.get(j).length();
                StormUtil.addMap(offset, "。", position);
                ++position;
            }
        }
        tfiR.totalTermCount = frequency.size();
        tfiR.termFrequency[0] = frequency;
        tfiR.termNature = nature;
        tfiR.termOffsets = offset;
        return tfiR;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

}
