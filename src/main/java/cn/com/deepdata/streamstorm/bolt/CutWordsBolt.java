package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.commonutil.UsrLibraryController;
import cn.com.deepdata.streamstorm.util.CommonUtil;
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
import java.util.*;
import static cn.com.deepdata.streamstorm.entity.RiskFields.*;

@SuppressWarnings({"serial", "rawtypes"})
public class CutWordsBolt extends AbstractRedisBolt {
    private transient static Log logger = LogFactory.getLog(CutWordsBolt.class);
    private transient DeepRichBoltHelper helper;
    private transient static AnsjTermAnalyzer ansjAnalyzer;
    private transient static final byte[] syncWordsLock = new byte[0];
    private long lastSyncTime = 0L;
    private String riskLastUpdate = "";
    private String clientLastUpdate = "";
    private String regionLastUpdate = "";

    public CutWordsBolt(JedisPoolConfig config) {
        super(config);
    }

    public CutWordsBolt(JedisClusterConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        helper = new DeepRichBoltHelper(collector);
        ansjAnalyzer = new AnsjTermAnalyzer();
        LoadWords();
    }

    private String loadWords(String key, String updateKey, String nature) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            addWordProperty(jedisCommands.smembers(key), nature);
            return jedisCommands.get(updateKey);
        } catch (Exception e) {
            logger.error("load word error..." + CommonUtil.getExceptionString(e));
        } finally {
            if (null != jedisCommands)
                returnInstance(jedisCommands);
        }
        return null;
    }

    // TODO: 2016/11/7 更新后有个比较的过程!!!
    private void addWordProperty(Set<String> words, String nature) {
        if (validSet(words) || validString(nature)) {
            logger.error("word nature error, add word fail...");
        }
        Map<String, String> wordsNature = new HashMap<>();
        words.stream().forEach(word -> wordsNature.put(word, nature));
        UsrLibraryController.ChangeNature(wordsNature, UsrLibraryController.EChangeOprationType.kAddOpration);
    }

    private boolean validSet(Set set) {
        return !(null == set || !set.isEmpty());
    }

    private boolean validString(String str) {
        return !(null == str || str.length() == 0);
    }

    private void LoadWords() {
        synchronized (syncWordsLock) {
            if (System.currentTimeMillis() - lastSyncTime < 60 * 1000)
                return;
            DoLoadWords();
        }
    }

    // TODO: 2016/11/7
//    public boolean needUpdate(String key, long lastUpdate) {
//
//    }

    public void DoLoadWords() {
        riskLastUpdate = validTime(riskLastUpdate, loadWords(RISK_TERMS_SET, RISK_LAST_UPDATE_TIME, "RT"));
        clientLastUpdate = validTime(clientLastUpdate, loadWords(CLIENT_TERMS_SET, CLIENT_LAST_UPDATE_TIME, "CT"));
        regionLastUpdate = validTime(regionLastUpdate, loadWords(REGION_TERMS_SET, REGION_LAST_UPDATE_TIME, "DS"));
        lastSyncTime = System.currentTimeMillis();
    }

    private String validTime(String oldTime, String newTime) {
        return newTime == null ? oldTime : newTime;
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

    private List<TermFrequencyInfo> combine (List<List<String>> tokens, List<List<String>> content) {
        Gson gson = new Gson();
        int position = 0;
        List<TermFrequencyInfo> list = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            HashMap<String, Integer> frequency = new HashMap<>();
            HashMap<String, String> nature = new HashMap<>();
            HashMap<String, List<Integer>> offset = new HashMap<>();
            TermFrequencyInfo tfiCopy = new TermFrequencyInfo(1);
            List<String> tempSeg = tokens.get(i);
            List<String> sentences = content.get(i);
            for (int j = 0; j < tempSeg.size(); j++) {
                TermFrequencyInfo tfi = gson.fromJson(tempSeg.get(j), TermFrequencyInfo.class);
                nature.putAll(tfi.termNature);
                putMap(frequency, tfi.termFrequency[0]);
                putMap(offset, tfi.termOffsets, position);
                position += sentences.get(j).length();
                addMap(offset, "。", position);
                ++position;
            }
            tfiCopy.totalTermCount = frequency.size();
            tfiCopy.termFrequency[0] = frequency;
            tfiCopy.termNature = nature;
            tfiCopy.termOffsets = offset;
            list.add(tfiCopy);
        }
        return list;
    }

    private void addMap(Map<String, List<Integer>> map, String key, int value) {
        if (map.containsKey(key)) {
            List<Integer> li = map.get(key);
            li.add(value);
        } else {
            List<Integer> li = new ArrayList<>();
            li.add(value);
            map.put(key, li);
        }
    }

    public static void putMap(Map<String, Integer> map, Map<String, Integer> old) {
        old.forEach((k, v) -> {
            if (map.containsKey(k))
                map.put(k, v + map.get(k));
            else
                map.put(k, v);
        });
    }

    public static void putMap(Map<String, List<Integer>> map, Map<String, List<Integer>> old, int position) {
        for (String word : old.keySet()) {
            if (map.containsKey(word)) {
                List<Integer> pos = map.get(word);
                old.get(word).stream().forEach(p -> pos.add(p + position));
            }
            else {
                List<Integer> pos = new ArrayList<>();
                old.get(word).stream().forEach(p -> pos.add(p + position));
                map.put(word, pos);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

}
