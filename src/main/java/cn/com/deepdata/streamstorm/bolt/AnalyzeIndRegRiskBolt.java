package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.streamstorm.entity.*;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import cn.com.deepdata.streamstorm.util.RegionUtil;
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
    private static final String keystoneRegionApi = "/keystone/api/v1/geo/area/_query";
    private transient DeepRichBoltHelper helper;
    private Map<String, Map<Integer, Integer>> regionAlias;
    private Map<Integer, Region> regionDetail;
    Map<String, Double> regionInfo;
    private RegionUtil regionUtil;
    private final String host;
    List<TermFrequencyInfo> contentTfi;
    TermFrequencyInfo titleTfi;
    String indRegCtrlVersion;

    public AnalyzeIndRegRiskBolt(JedisPoolConfig config, String keystoneUrl) {
        super(config);
        this.host = keystoneUrl + keystoneRegionApi;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, collector);
        try {
            regionUtil = new RegionUtil(host);
            regionAlias = regionUtil.getRegionAlias();
            regionDetail = regionUtil.getRegionDetail();
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
            Map<String, Object> doc = helper.getDoc(input);
            Map<String, Object> attach = helper.getAttach(input);
            if (validTokens(attach)) {
                init(attach);
            } else {
                helper.emit(input, true);
                helper.ack(input);
                return;
            }
            IndRegRisk indRegRisk = Analyze(title, content);
            logger.debug(new Gson().toJson(indRegRisk));
            // TODO: 2016/10/25
            helper.emitDoc(input, doc, true);
//            helper.emitAttach(input, attach, true);
            helper.ack(input);
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
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

    private void addRiskInfo(Map<Integer, Map<String, Set<String>>> riskInfo, Map<String, String> info,
                             Map<String, Integer> riskTerm) {
        String raw = info.get("raw");
        if (raw.contains("*")) {
            String[] words = raw.split("\\*");
            for (String word : words) {
                if (!riskTerm.containsKey(word))
                    return ;
            }
        } else {
            if(!riskTerm.containsKey(raw))
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
                    mapObj = splitSet(mapObj);
                    int minDistance = Integer.MAX_VALUE;
                    try {
                        for (String wordObj : mapObj) {
                            for (int posObj : offset.get(wordObj)) {
                                for (String wordRisk : mapRisk) {
                                    for (int posRisk : offset.get(wordRisk)) {
                                        minDistance = Math.min(minDistance, getDistance(posObj, posRisk, wordObj, wordRisk));
                                    }
                                }
                            }
                        }
                    } catch (NullPointerException e) {
                        logger.error(CommonUtil.getExceptionString(e));
                    }
                    value = calScoreByDistance((double) minDistance, 70.0, 70.0, 35.0);
                    String strRegionRisk = jedisCommands.get(RiskFields.regRiskItemPrefixKey.replace("%v%", indRegCtrlVersion) + entry.getKey());
                    RegionRiskInfo info = gson.fromJson(strRegionRisk, RegionRiskInfo.class);
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
        return value < 0 ? 0. : (value *= 20);
    }

    private Set<String> splitSet(Set<String> set) {
        Set<String> newSet = new HashSet<>();
        for (String word : set) {
            if (word.contains("*")) {
                String[] sl = word.split("\\*");
                set.addAll(Arrays.asList(sl));
            } else
                set.add(word);
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
        return result;
    }

    public IndRegRisk AnalyzeSegment(TermFrequencyInfo tfi, double weight, Map<Integer, Map<String, Set<String>>> regionRiskInfo,
                                     Map<Integer, Map<String, Set<String>>> industryRiskInfo) {
        Gson gson = new Gson();
        IndRegRisk riskResult = new IndRegRisk();
        Map<String, Integer> riskTerm = new HashMap<>();
        getRegion(tfi, regionInfo, weight);
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            // TODO: 2016/10/18 取消riskInfo
            tfi.termFrequency[0].keySet().stream().filter(word -> tfi.termNature.get(word).contains("risk")).forEach(
                    word -> riskTerm.put(word, tfi.termFrequency[0].get(word))
            );

            for (String word : riskTerm.keySet()) {
                String riskWordInfo = jedisCommands.get(RiskFields.indRegRiskTokenItemPrefixKey
                        .replace("%v%", indRegCtrlVersion) + word);
                List<String> riskWordInfoList = gson.fromJson(riskWordInfo, TypeProvider.type_ls);
                for (String s_info : riskWordInfoList) {
//					["{\"raw\":\"农业市场\",\"id\":\"32\",\"property\":\"classify1\",\"type\":\"2\"}","{\"raw\":\"农业市场\",\"id\":\"31\",\"property\":\"classify1\",\"type\":\"2\"}"]
                    Map<String, String> info = gson.fromJson(s_info, TypeProvider.type_mss);
                    int riskType = Integer.parseInt(info.get("type"));
                    if (riskType == 1)
                        addRiskInfo(regionRiskInfo, info, riskTerm);
                    else if (riskType == 2 || riskType == 3)
                        addRiskInfo(industryRiskInfo, info, riskTerm);
                }
            }

            double regionRisk = calcRegionRisk(regionRiskInfo, tfi.termOffsets);
            riskResult.industryRisk = calcIndustryRisk(industryRiskInfo, tfi.termOffsets);
            if (regionRisk > riskResult.regionRisk)
                riskResult.regionRisk = regionRisk;
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        } finally {
            if (jedisCommands != null)
                returnInstance(jedisCommands);
        }
        return riskResult;
    }

    private void getRegion(TermFrequencyInfo tfi, Map<String, Double> map, double weight) {
        Map<String, String> nature = tfi.termNature;
        Map<String, Integer> frequency = tfi.termFrequency[0];
        if (nature == null || nature.isEmpty())
            return;
        nature.entrySet().stream().forEach(e -> {
            if (e.getValue().contains("DS")) {
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
            while ((pid = regionUtil.getParentId(id)) != 0) {
                if (result.containsKey(pid))
                    result.put(pid, son / 2 + result.get(pid));
                else {
                    result.put(pid, son / 2);
                    analyzedRegion.add(regionUtil.getRegionName(pid));
                }
                id = pid;
                son /= 2;
            }
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
    }

    private List<Region> calcRegionRelevancy(Map<String, Double> regionInfo) {
        Map<Integer, Double> result = new HashMap<>();
        Set<String> analyzedRegion = new HashSet<>();
        List<Region> list = new ArrayList<>();
        double total = 0.;
        double denominator;

        for (Map.Entry<String, Double> ri : regionInfo.entrySet()) {
            String regionName = ri.getKey();
            Map<Integer, Integer> idMapping = regionAlias.get(regionName);
            for (Map.Entry<Integer, Integer> idm : idMapping.entrySet()) {
                try {
                    int pid = idm.getValue();
                    String pRegion;
                    int id = idm.getKey();
                    result.put(id, ri.getValue());
                    if (pid != 0)
                        pRegion = regionUtil.getRegionName(pid);
                    else
                        continue;
                    if (idMapping.size() > 1) {
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
                Region region = regionDetail.get(r.getKey()).clone();
                if (analyzedRegion.contains(region.sca_region))
                    region.bna_analyze = true;
                region.dna_score = r.getValue() / denominator;
                list.add(region);
            } catch (Exception e) {
                logger.error(r.getKey() + ", " + regionDetail.containsKey(r.getKey()));
                e.printStackTrace();
            }
        }
        Collections.sort(list, (Region o1, Region o2) -> (o2.dna_score > o1.dna_score ? 1 : (o2.dna_score == o1.dna_score ? 0 : -1)));
        return list;
    }

    public IndRegRisk Analyze(String title, String body) {
        Gson gson = new Gson();
        Map<Integer, Map<String, Set<String>>> regionRiskInfo = new HashMap<>();
        Map<Integer, Map<String, Set<String>>> industryRiskInfo = new HashMap<>();
        IndRegRisk riskInfo = new IndRegRisk();
        try {
            if (title.length() > 0) {
                IndRegRisk titleInfo = AnalyzeSegment(titleTfi, 3, regionRiskInfo, industryRiskInfo);
                riskInfo.addMax(titleInfo);
            }
            String[] segments = body.split("。|； |\r|\n");
            for (int i = 0; i < contentTfi.size(); i++) {
                if (segments[i].length() > 0) {
                    IndRegRisk segInfo = AnalyzeSegment(contentTfi.get(i), 1, regionRiskInfo, industryRiskInfo);
                    riskInfo.addMax(segInfo);
                }
            }
            if (regionRiskInfo.isEmpty())
                riskInfo.regionDebugInfo = gson.toJson(regionRiskInfo);
            if (industryRiskInfo.isEmpty())
                riskInfo.industryDebugInfo = gson.toJson(industryRiskInfo);
            if (!regionInfo.isEmpty())
                riskInfo.regionList = calcRegionRelevancy(regionInfo);
        } catch (Exception e) {
            logger.error(CommonUtil.getExceptionString(e));
        }
        return riskInfo;
    }
}