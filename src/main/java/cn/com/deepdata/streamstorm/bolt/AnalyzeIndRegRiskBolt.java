package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.streamstorm.entity.*;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import cn.com.deepdata.streamstorm.util.TypeProvider;

import com.google.gson.Gson;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
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

@SuppressWarnings({"serial", "rawtypes"})
public class AnalyzeIndRegRiskBolt extends AbstractRedisBolt {
    private transient static Logger logger = LoggerFactory.getLogger(AnalyzeIndRegRiskBolt.class);
    private transient DeepRichBoltHelper helper;
    Map<String, Double> regionInfo;
    List<TermFrequencyInfo> contentTfi;
    TermFrequencyInfo titleTfi;
    String indRegCtrlVersion;
    private List<Map<Integer, Map<String, Set<String>>>> regionInfoList = new ArrayList<>();
    private List<Map<Integer, Map<String, Set<String>>>> industryInfoList = new ArrayList<>();

    public AnalyzeIndRegRiskBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, collector);
        try {
            helper = new DeepRichBoltHelper(outputCollector);
            regionInfo = new HashMap<>();
        }catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String title = helper.getDocTitle(input);
            String content = helper.getDocContent(input);
            Map<String, Object> source = helper.getDoc(input);
            Map<String, Object> attach = helper.getAttach(input);
            if (validTokens(attach)) {
                init(attach);
            } else {
                helper.emit(input, true);
                helper.ack(input);
                return;
            }
            IndRegRisk indRegRisk = Analyze(title, content);
            if (indRegRisk.regionRisk > 0) {
                source.put("dna_regionRisk", indRegRisk.regionRisk);
                source.put("sna_regionRiskDebugInfo", indRegRisk.regionDebugInfo);
            }

//            logger.info(new Gson().toJson(indRegRisk));
            // TODO: 2016/10/25
            helper.emitDoc(input, source, true);
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        } finally {
            helper.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

    private boolean validTokens(Map<String, Object> attach) {
        return attach.containsKey("titleRaw")
                && attach.containsKey("titleTermInfo")
                && attach.containsKey("contentRaw")
                && attach.containsKey("contentTermInfo");
    }

    private void init(Map<String, Object> attach) {
        titleTfi = (TermFrequencyInfo) attach.get("titleTermInfo");
        contentTfi = (List<TermFrequencyInfo>) attach.get("contentTermInfo");
        indRegCtrlVersion = (String) attach.get("indRegCtrlVersion");
    }

    private void addRiskInfo(TermFrequencyInfo tfi, Map<Integer, Map<String, Set<String>>> riskInfo, Map<String, String> info) {
        String raw = info.get("raw");
        Map<String, Integer> frequency = tfi.termFrequency[0];
        if (raw.contains("*")) {
            String[] words = raw.split("\\*");
            for (String word : words) {
                if (!frequency.containsKey(word))
                    return ;
            }
        } else {
            if(!frequency.containsKey(raw))
                return ;
        }

        Integer id = Integer.parseInt(info.get("id"));
        if (riskInfo.containsKey(id)) {
            Map<String, Set<String>> match = riskInfo.get(id);
            if (match.containsKey(info.get("property"))) {
                Set<String> property = match.get(info.get("property"));
                property.add(raw);
            } else {
                Set<String> property = new HashSet<>();
                property.add(raw);
                match.put(info.get("property"), property);
            }
        } else {
            Map<String, Set<String>> match = new HashMap<>();
            Set<String> property = new HashSet<>();
            property.add(raw);
            match.put(info.get("property"), property);
            riskInfo.put(id, match);
        }
    }

    /**
     * 采用百分制  暂时算最大的一个
     */
    private double calcRegionRisk(Map<Integer, Map<String, Set<String>>> regionRiskInfo,
                                  Map<String, List<Integer>> offset) {
        Gson gson = new Gson();
        double value = 0;
//        logger.info("regionInfo:{}", gson.toJson(regionRiskInfo));
//        logger.info("offset:{}", gson.toJson(offset));
        Iterator<Map.Entry<Integer, Map<String, Set<String>>>> it = regionRiskInfo.entrySet().iterator();
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            while (it.hasNext()) {
                Map.Entry<Integer, Map<String, Set<String>>> entry = it.next();
                Map<String, Set<String>> match = entry.getValue();
                if (match.containsKey("object") && match.containsKey("risk")) {
                    Set<String> mapObj = match.get("object");
                    Set<String> mapRisk = match.get("risk");
//                    logger.info("~~~~obj:{}", gson.toJson(mapObj));
                    mapObj = splitSet(mapObj);
//                    logger.info("obj:{}", gson.toJson(mapObj));
//                    logger.info("risk:{}", gson.toJson(mapRisk));
                    int minDistance = Integer.MAX_VALUE;
                    try {
                        for (String wordObj : mapObj) {
                            for (int posObj : offset.get(wordObj)) {
                                for (String wordRisk : mapRisk) {
                                    for (int posRisk : offset.get(wordRisk)) {
                                        minDistance = Math.min(minDistance, getDistance(posObj, posRisk, wordObj, wordRisk));
//                                        logger.info("obj:{}, risk:{}, distance:{}", posObj, posRisk, getDistance(posObj, posRisk, wordObj, wordRisk));
                                    }
                                }
                            }
                        }
                    } catch (NullPointerException e) {
                        logger.error(CommonUtil.getExceptionString(e));
                    }
                    value = calScoreByDistance((double) minDistance, 70.0, 70.0, 35.0);
//                    logger.info("value: {}, minDistance: {}", String.valueOf(value), String.valueOf(minDistance));
                    String strRegionRisk = jedisCommands.get(RiskFields.REGION_RISK_ITEM_INFO_PREFIX + entry.getKey());
                    RegionRiskInfo info = gson.fromJson(strRegionRisk, RegionRiskInfo.class);
//                    logger.info("value : {}, weight: {}", String.valueOf(value), String.valueOf(info.weight));
                    value *= info.weight;
                } else {
                    it.remove();
                }
            }
        } catch (Exception e) {

        } finally {
            if (jedisCommands != null)
                returnInstance(jedisCommands);
        }
        if (!regionRiskInfo.isEmpty())
            regionInfoList.add(regionRiskInfo);
        return value < 0 ? 0. : (value *= 20);
    }

    private Set<String> splitSet(Set<String> set) {
        Set<String> newSet = new HashSet<>();
        for (String word : set) {
            if (word.contains("*")) {
                String[] sl = word.split("\\*");
                newSet.addAll(Arrays.asList(sl));
            } else
                newSet.add(word);
        }
        return newSet;
    }

    /**
     * 最小距离
     */
    public int getDistance(int pos1, int pos2, String word1, String word2) {
        return Math.min(Math.abs(pos1 - pos2 - word2.length()), Math.abs(pos2 - pos1 - word1.length()));
    }

    /**
     * 计算分数
     *
     * @param distance    词的距离
     * @param maxDistance 两词的最大距离，超过距离分数为小于0
     */
    private double calScoreByDistance(double distance, double maxDistance, double c1, double c2) {
        return distance > maxDistance ? 0.0 : Math.log(c1 / c2 - distance / c2 + 1) / Math.log(c1 / c2 + 1);
    }

    /**
     * 按距离计算分数， 将行业信息存入industryRiskInfo中，然后存入industryDebugInfo
     */
    private Map<Integer, Double> calcIndustryRisk(Map<Integer, Map<String, Set<String>>> industryRiskInfo,
                                                  Map<String, List<Integer>> offset) {
//        logger.info("@@@@@@@@@@@@@@@@: {}", new Gson().toJson(industryRiskInfo));
        // 风险词的id为1
        Map<Integer, Double> result = new HashMap<>();
        if (!industryRiskInfo.containsKey(1)) {
            industryRiskInfo.clear();
            return result;
        }

        Map<String, Set<String>> riskWordMatch = industryRiskInfo.get(1);
        if (!riskWordMatch.containsKey("analyze") || !riskWordMatch.containsKey("bad")) {
            industryRiskInfo.clear();
            return result;
        }

        int minDistanceAB = Integer.MAX_VALUE;
        int pos_ana = Integer.MAX_VALUE;
        int pos_bad = Integer.MAX_VALUE;
        double pos_avg;
        double minDistance = Double.MAX_VALUE;
        Set<String> analyzeWords = riskWordMatch.get("analyze");
        Set<String> badWords = riskWordMatch.get("bad");
        for (String wordAnalyze : analyzeWords) {
            for (int posAnalyze : offset.get(wordAnalyze)) {
                for (String wordBad : badWords) {
                    for (int posBad : offset.get(wordBad)) {
                        minDistanceAB = Math.min(minDistanceAB, getDistance(posAnalyze, posBad, wordAnalyze, wordBad));
                        pos_ana = posAnalyze;
                        pos_bad = posBad;
                    }
                }
            }
        }

        pos_avg = (pos_ana + pos_bad) / 2.0;
        double risk = calScoreByDistance(minDistanceAB, 70.0, 70.0, 35.0);
        if (risk <= 0) {
            industryRiskInfo.clear();
            return result;
        }

        Iterator<Map.Entry<Integer, Map<String, Set<String>>>> it = industryRiskInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Map<String, Set<String>>> entry = it.next();
            int id = entry.getKey();
            Map<String, Set<String>> match = entry.getValue();
            if (match.containsKey("classify1")) {
                Set<String> set = match.get("classify1");
                for (String wordInd : set) {
                    for (int posInd : offset.get(wordInd)) {
                        minDistance = Math.min((posInd - pos_avg - 4), (pos_avg - posInd - 4));
                    }
                }
                minDistance = Math.max(0.0, minDistance);
                risk *= calScoreByDistance(minDistance, 70.0, 70.0, 35.0);
                result.put(id, risk * 60);
                if (id != 1) {
                    match.put("analyze", riskWordMatch.get("analyze"));
                    match.put("bad", riskWordMatch.get("bad"));
                }
            } else {
                it.remove();
            }
        }
        if (!industryRiskInfo.isEmpty())
            industryInfoList.add(industryRiskInfo);
//        logger.info("################: {}", new Gson().toJson(industryRiskInfo));
//        logger.info("$$$$$$$$$$$$$$$$: {}", new Gson().toJson(industryInfoList));
        return result;
    }

    public IndRegRisk AnalyzeSegment(TermFrequencyInfo tfi, double weight) {
        Map<Integer, Map<String, Set<String>>> regionRiskInfo = new HashMap<>();
        Map<Integer, Map<String, Set<String>>> industryRiskInfo = new HashMap<>();
        IndRegRisk riskResult = new IndRegRisk();
        Map<String, Integer> riskTerm;
        getRegion(tfi, regionInfo, weight);
        try {
            riskTerm = getWordByNature(tfi, "RR");              // region type: object analyze
            calcPrepare(tfi, riskTerm, regionRiskInfo, RiskFields.REGION_RISK_TERM_INFOS_PREFIX);
            riskTerm = getWordByNature(tfi, "IR");              // industry type: classify analyze bad
            calcPrepare(tfi, riskTerm, industryRiskInfo, RiskFields.INDUSTRY_RISK_TERM_INFOS_PREFIX);
            riskTerm = getWordByNature(tfi, "IGR");
            calcPrepare(tfi, riskTerm, industryRiskInfo, RiskFields.INDUSTRY_GEN_RISK_TERM_INFOS_PREFIX);

            double regionRisk = calcRegionRisk(regionRiskInfo, tfi.termOffsets);
//            logger.info("regionRisk:{}", regionRisk);
            riskResult.industryRisk = calcIndustryRisk(industryRiskInfo, tfi.termOffsets);
            if (regionRisk > riskResult.regionRisk)
                riskResult.regionRisk = regionRisk;
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
        return riskResult;
    }

    private void calcPrepare(TermFrequencyInfo tfi, Map<String, Integer> riskTerm,
                             Map<Integer, Map<String, Set<String>>> info, String key) {
        Gson gson = new Gson();
        riskTerm.forEach((k, v) -> getInfo(key, k)
                .stream().forEach(s -> addRiskInfo(tfi, info, gson.fromJson(s, TypeProvider.type_mss))));
    }

    private Map<String, Integer> getWordByNature(TermFrequencyInfo tfi, String nature) {
        Map<String, Integer> riskTerm = new HashMap<>();
        tfi.termFrequency[0].keySet().stream().filter(word -> tfi.termNature.get(word).contains(nature)).forEach(
                word -> riskTerm.put(word, tfi.termFrequency[0].get(word)));
        return riskTerm;
    }

    private Set<String> getInfo(String key, String word) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            String info = jedisCommands.get(key + word);
            return new Gson().fromJson(info ,TypeProvider.type_ss);
        } catch (Exception e) {
            logger.error("get info from redis error..." + CommonUtil.getExceptionString(e));
        } finally {
            if (jedisCommands != null)
                returnInstance(jedisCommands);
        }
        return new HashSet<>();
    }

    private void getRegion(TermFrequencyInfo tfi, Map<String, Double> map, double weight) {
        Map<String, String> nature = tfi.termNature;
        Map<String, Integer> frequency = tfi.termFrequency[0];
        if (nature == null || nature.isEmpty())
            return;
        nature.entrySet().stream().forEach(e -> {
            if (e.getValue().contains("RE")) {
                String region = e.getKey();
                try {
                    if (map.containsKey(region))
                        map.put(region, frequency.get(region) * weight + map.get(region));
                    else
                        map.put(region, frequency.get(region) * weight);
                } catch (Exception e1) {
                    logger.error(CommonUtil.getExceptionString(e1));
                }
            }
        });
    }

    private void calcFrequency(Map<Integer, Double> result, Set<String> analyzedRegion, String regionName, int id) {
        try {
            int pid;
            double son = regionInfo.get(regionName);
            while ((pid = getParentId(id)) != 0) {
                if (result.containsKey(pid))
                    result.put(pid, son / 2 + result.get(pid));
                else {
                    result.put(pid, son / 2);
                    analyzedRegion.add(getRegionName(pid));
                }
                id = pid;
                son /= 2;
            }
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
    }

    // TODO: 2016/11/3 考虑并发修改
    private String getRegionName(int id) {
        Map<String, Object> info = getRegionDetail(id);
        return info.get("name").toString();
    }

    private int getParentId (int id) {
        Map<String, Object> info = getRegionDetail(id);
        int pid;
        try {
            pid = Integer.parseInt(info.get("parent_id").toString());
        } catch (Exception e) {
            pid = 0;
        }
        return pid;
    }

    private List<Integer> getId(String name) {
        return getRegionIdByAlias(name);
    }

    private Map<String, Object> getRegionDetail(int id) {
        JedisCommands jedisCommands = getInstance();
        String sInfo = jedisCommands.get(RiskFields.REGION_ITEM_INFO_PREFIX + id);
        Map<String, Object> info = new Gson().fromJson(sInfo, TypeProvider.type_mso);
        returnInstance(jedisCommands);
        return info;
    }

    private List<Integer> getRegionIdByAlias(String alias) {
        JedisCommands jedisCommands = getInstance();
//        logger.info(RiskFields.REGION_TERM_INFOS_PREFIX + alias);
        String sInfo = jedisCommands.get(RiskFields.REGION_TERM_INFOS_PREFIX + alias);
        List<String> ids = new Gson().fromJson(sInfo, TypeProvider.type_ls);
        returnInstance(jedisCommands);
        return convertList(ids);
    }

    // TODO: 2016/11/3 try catch
    private List<Integer> convertList(List<String> list) {
        List<Integer> newList = new ArrayList<>();
        list.stream().forEach(s -> newList.add(Integer.parseInt(s)));
        return newList;
    }

    private List<Region> calcRegionRelevancy(Map<String, Double> regionInfo) {
        Map<Integer, Double> result = new HashMap<>();
        Set<String> analyzedRegion = new HashSet<>();
        List<Region> list = new ArrayList<>();
        double total = 0.;
        double denominator;

        for (Map.Entry<String, Double> ri : regionInfo.entrySet()) {
            String regionName = ri.getKey();
            List<Integer> ids = getId(regionName);
            for (int id : ids) {
                try {
                    int pid = getParentId(id);
                    String pRegion;
                    result.put(id, ri.getValue());
                    if (pid != 0)
                        pRegion = getRegionName(pid);
                    else
                        continue;
                    if (ids.size() > 1) {
                        if (regionInfo.containsKey(pRegion))
                            calcFrequency(result, analyzedRegion, regionName, id);
                    } else
                        calcFrequency(result, analyzedRegion, regionName, id);
                } catch (NullPointerException e) {
                    logger.error(CommonUtil.getExceptionString(e));
                }
            }
        }

        for (double s : result.values())
            total += Math.pow(s, 2);
        if (total == 0)
            return null;
        denominator = Math.sqrt(total);

        for (Map.Entry<Integer, Double> r : result.entrySet()) {
            try {
                Region region = new Region(getRegionDetail(r.getKey()));
                if (analyzedRegion.contains(region.sca_region))
                    region.bna_analyze = true;
                region.dna_score = r.getValue() / denominator;
                list.add(region);
            } catch (Exception e) {
                logger.error("error id:" + r.getKey());
                e.printStackTrace();
            }
        }
        Collections.sort(list, (Region o1, Region o2) -> (o2.dna_score > o1.dna_score ? 1 : (o2.dna_score == o1.dna_score ? 0 : -1)));
        return list;
    }

    public IndRegRisk Analyze(String title, String body) {
        Gson gson = new Gson();
        IndRegRisk riskInfo = new IndRegRisk();
        try {
            if (title.length() > 0) {
                IndRegRisk titleInfo = AnalyzeSegment(titleTfi, 3);
                riskInfo.addMax(titleInfo);
            }
            String[] segments = body.split("。|； |\r|\n");
            for (int i = 0; i < contentTfi.size(); i++) {
                if (segments[i].length() > 0) {
                    IndRegRisk segInfo = AnalyzeSegment(contentTfi.get(i), 1);
                    riskInfo.addMax(segInfo);
                }
            }
            if (!regionInfoList.isEmpty())
                riskInfo.regionDebugInfo = gson.toJson(regionInfoList);
            if (!industryInfoList.isEmpty())
                riskInfo.industryDebugInfo = gson.toJson(industryInfoList);
            if (!regionInfo.isEmpty())
                riskInfo.regionList = calcRegionRelevancy(regionInfo);
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
        return riskInfo;
    }
}