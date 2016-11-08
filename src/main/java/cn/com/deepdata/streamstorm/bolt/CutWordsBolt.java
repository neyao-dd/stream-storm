package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.commonutil.UsrLibraryController;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.*;

import static cn.com.deepdata.streamstorm.entity.RiskFields.*;
import static cn.com.deepdata.streamstorm.util.CommonUtil.validString;

@SuppressWarnings({"serial", "rawtypes"})
public class CutWordsBolt extends AbstractRedisBolt {
    private transient static Logger logger = LoggerFactory.getLogger(CutWordsBolt.class);
    private transient DeepRichBoltHelper helper;
    private transient static AnsjTermAnalyzer ansjAnalyzer;
    private transient static final byte[] syncWordsLock = new byte[0];
    private long lastSyncTime = 0L;
    private String riskLastUpdate;
    private String clientLastUpdate;
    private String regionLastUpdate;
    private Set<String> riskWords = new HashSet<>();
    private Set<String> clientWords = new HashSet<>();
    private Set<String> regionWords = new HashSet<>();

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

    private String loadWords(Set<String> localWords, String key, String updateTimeKey, String lastUpdateTime, String nature) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            String redisLastUpdate = jedisCommands.get(updateTimeKey);
            if (inValidTime(lastUpdateTime)) {
                localWords.addAll(jedisCommands.smembers(key));
                changeWordNature(localWords, nature, UsrLibraryController.EChangeOprationType.kAddOpration);
            } else if (needUpdate(redisLastUpdate, lastUpdateTime)) {
                Set<String> remoteWords = jedisCommands.smembers(key);
                Set<String> remoteMore = diffSet(remoteWords, localWords);
                Set<String> remoteLess = diffSet(localWords, remoteWords);
                changeWordNature(remoteMore, nature, UsrLibraryController.EChangeOprationType.kAddOpration);
                changeWordNature(remoteLess, nature, UsrLibraryController.EChangeOprationType.kDeleteOpration);
            }
            return redisLastUpdate;
        } catch (Exception e) {
            logger.error("load word error..." + CommonUtil.getExceptionString(e));
        } finally {
            if (null != jedisCommands)
                returnInstance(jedisCommands);
        }
        return lastUpdateTime;
    }

    private Set<String> diffSet(Set<String> s1, Set<String> s2) {
        Set<String> diff = new HashSet<>();
        diff.addAll(s1);
        diff.removeAll(s2);
        return diff;
    }

    private boolean needUpdate(String remote, String local) {
        return !remote.equals(local);
    }

    private boolean inValidTime(String time) {
        return !validString(time);
    }

    private void changeWordNature(Set<String> words, String nature, UsrLibraryController.EChangeOprationType type) {
        if (!validSet(words))
            logger.error("words set is null, add word fail...");
        if (!validString(nature)) {
            logger.error("word nature error, add word fail...");
        }
        Map<String, String> wordsNature = new HashMap<>();
        words.stream().forEach(word -> wordsNature.put(word, nature));
        UsrLibraryController.ChangeNature(wordsNature, type);
    }

    private boolean validSet(Set set) {
        return !(null == set || set.isEmpty());
    }

    private void LoadWords() {
        synchronized (syncWordsLock) {
            if (System.currentTimeMillis() - lastSyncTime < 60 * 1000)
                return;
            DoLoadWords();
        }
    }

    public void DoLoadWords() {
        loadClientWords();
        loadRiskWords();
        loadRegionWords();
        lastSyncTime = System.currentTimeMillis();
    }

    private void loadClientWords() {
        clientLastUpdate = loadWords(clientWords, CLIENT_TERMS_SET, CLIENT_LAST_UPDATE_TIME, clientLastUpdate, "CT");
    }

    private void loadRiskWords() {
        riskLastUpdate = loadWords(riskWords, RISK_TERMS_SET, RISK_LAST_UPDATE_TIME, riskLastUpdate, "RT");
    }

    private void loadRegionWords() {
        regionLastUpdate = loadWords(regionWords, REGION_TERMS_SET, REGION_LAST_UPDATE_TIME, regionLastUpdate, "DS");
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

    private List<TermFrequencyInfo> combine(List<List<String>> tokens, List<List<String>> content) {
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
            } else {
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
