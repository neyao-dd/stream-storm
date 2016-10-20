package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.streamstorm.entity.*;
import cn.com.deepdata.streamstorm.util.StormUtil;
import com.google.gson.Gson;
import org.apache.commons.configuration.PropertiesConfiguration;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.util.*;

/**
 * Created by yukh on 2016/10/19
 */
public class AnalyzeInnerRiskBolt extends AbstractRedisBolt {
    private static final Logger logger = LoggerFactory.getLogger(AnalyzeInnerRiskBolt.class);
    private String calcType;
    private Gson gson = new Gson();
    private JedisCommands jedisCommands = getInstance();

    static volatile double brandScore = 0.9;
    static volatile double brandScore2 = 0.25;
    static volatile double productScore = 0.8;
    static volatile double productScore2 = 0.2;
    static volatile double peopleScore = 0.8;
    static volatile double peopleScore2 = 0.2;
    static volatile double otherScore = 0.6;
    static volatile double otherScore2 = 0.15;
    private transient DeepRichBoltHelper helper;

    public AnalyzeInnerRiskBolt(JedisPoolConfig config) {
        super(config);
    }

    public AnalyzeInnerRiskBolt(JedisClusterConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        helper = new DeepRichBoltHelper(collector);
        JedisCommands jedis = null;
        try {
            jedis = getInstance();
            calcType = "paragraph";
            String configCalcType = StormUtil.getCalcType();
            if (configCalcType != null && configCalcType.length() > 0)
                calcType = configCalcType;
            if (jedis.get(RiskFields.brandScoreKey) != null) {
                // TODO: 2016/10/20 redis or 配置文件
                brandScore = Double.parseDouble(jedis.get(RiskFields.brandScoreKey));
                brandScore2 = Double.parseDouble(jedis.get(RiskFields.brandScore2Key));
                productScore = Double.parseDouble(jedis.get(RiskFields.productScoreKey));
                productScore2 = Double.parseDouble(jedis.get(RiskFields.productScore2Key));
                peopleScore = Double.parseDouble(jedis.get(RiskFields.peopleScoreKey));
                peopleScore2 = Double.parseDouble(jedis.get(RiskFields.peopleScore2Key));
                otherScore = Double.parseDouble(jedis.get(RiskFields.otherScoreKey));
                otherScore2 = Double.parseDouble(jedis.get(RiskFields.otherScore2Key));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        } finally {
            if (null != jedis)
                returnInstance(jedis);
        }
    }

    @Override
    public void execute(Tuple input) {
        helper = new DeepRichBoltHelper(collector);
        Map<String, Object> attach = helper.getAttach(input);
        Map<String, Object> source = helper.getDoc(input);
        try {
            InnerRiskValue irv = new InnerRiskValue();
            int segNum = 1;
            String result;
            List<DescRiskScore> desRiskScore = new ArrayList<>();
            // ct info 频率
            Map<String, Integer> clientDebugInfo = new HashMap<>();
            // ct name [id]
            Map<String, HashSet<String>> client = new HashMap<>();
            // 风险id 客户id 得分
            HashMap<Integer, HashMap<Integer, ArrayList<Double>>> desCli = new HashMap<>();
            StringBuilder sb = new StringBuilder();
            Map<Integer, Double> totalCliScore = new HashMap<>();
            // cId+raw:score
            Map<String, Double> clientScore = new HashMap<>();
            if (!attach.get("inp_type").equals("2")) {
                //微博的inp_type == 2  微博没有标题
                String title = helper.getDocTitle(input);
                result = analyzeSegment(title, 0, clientDebugInfo, client, clientScore, desCli);
                sb.append(result);
            }
            String content = source.get("scc_content").toString();
            String[] segments = content.split("\r|\n");
            if (content.length() < 2 && content.length() > 500) {
                // TODO: 2016/10/20 返回都需要处理
                helper.ack(input);
                return ;
            }
            for (String segment : segments) {
                if (!segment.isEmpty()) {
                    // TODO: 2016/10/20 参数
                    result = analyzeSegment(segment, segNum++, clientDebugInfo, client, clientScore, desCli);
                    sb.append(result);
                }
            }
            Map<Integer, HashMap<String, HashSet<String>>> cliDebugInfo = new HashMap<>();
            convertClientDebugInfo(clientDebugInfo, cliDebugInfo);
            getTotalCliScore(totalCliScore, clientScore);
            // 加入list按分值排序
            mapToList(desCli, totalCliScore, desRiskScore);
            desSort(desRiskScore);
            if (desRiskScore.size() > 0) {
                irv.maxRiskScore = desRiskScore.get(0).getDna_score();
                irv.riskScore = desRiskScore;
                for (DescRiskScore drs : desRiskScore) {
                    irv.totalRiskScore += drs.getDna_score();
                }
            }
            for (int id : totalCliScore.keySet()) {
                double maxRiskScore = 0.;
                double maxRiskScoreV2 = 0.;
                ClientScore cs = new ClientScore();
                cs.setIna_id(id);
                if (StormUtil.existUuid())
                    cs.setSnc_uuid(StormUtil.getUUID(id));
                cs.setDna_score(totalCliScore.get(id));
                if (cs.getDna_score() >= 0.9) {
                    for (DescRiskScore drs : desRiskScore) {
                        if (drs.getIna_client_id() == id) {
                            if (maxRiskScore == 0)
                                maxRiskScore = drs.getDna_score();
                            maxRiskScoreV2 = Math.max(drs.getDna_score_v2(), maxRiskScoreV2);
                        }
                    }
                    cs.setDna_risk_score(maxRiskScore);
                    cs.setDna_risk_score_v2(maxRiskScoreV2);
                }
                irv.clientScore.add(cs);
            }
            irv.clientDebugInfo2 = gson.toJson(cliDebugInfo);
            irv.riskDebugInfo += sb.toString();
        } catch (Exception e) {
            logger.error("analyze inner risk error...");
            logger.error("Exception " + StormUtil.getExceptionString(e));
        } finally {
            if (null != jedisCommands)
                returnInstance(jedisCommands);
        }
    }

    private String analyzeSegment(String segment, int segNum, Map<String, Integer> clientDebugInfo, Map<String, HashSet<String>> client,
                                  Map<String, Double> clientScore, HashMap<Integer, HashMap<Integer, ArrayList<Double>>> desCli) {
        StringBuilder sb = new StringBuilder();
        if (!segment.isEmpty()) {
            double segWeight = segNum > 1 ? 1. : segNum == 1 ? 1.2 : 1.5;
            HashMap<String, HashSet<String>> ctByCate = analyzeSegmentCT(clientDebugInfo, client, clientScore);
            HashMap<Integer, ConcreteRisk> anaResult = analyzeSegmentRT(ctByCate, segWeight);
            if (!anaResult.isEmpty()) {
                List<ConcreteRisk> result = riskItemByClient(anaResult, desCli, client, tfi.termOffsets, clientScore);
                if (!result.isEmpty())
                    sb.append("segment:").append(segNum).append("\n").append(convertResult(result));
            }
        }
        return sb.toString();
    }

    private HashMap<String, HashSet<String>> analyzeSegmentCT(Map<String, Integer> clientDebugInfo,
                                                              Map<String, HashSet<String>> client,
                                                              Map<String, Double> clientScore) {
        HashMap<String, String> tNature = tfi.termNature;
        Map<String, String> infoCT;
        HashMap<String, HashSet<String>> ctByCate = new HashMap<>();
        // TODO 考虑优化 查询2次
        for (String key : tNature.keySet()) {
            if (tNature.get(key).contains("CT") || tNature.get(key).contains("nr")) {
                String information = getItemByRedis(jedis, clientTokenItemPrefixKey, key, clientWords.version());
                if (null != information) {
                    HashSet<String> infoSet = gson.fromJson(information, typeSet);
                    for (String str : infoSet) {
                        infoCT = gson.fromJson(str, typeSS);
                        String cid = infoCT.get("id");
                        String craw = infoCT.get("raw");
                        String ctype = infoCT.get("type");
//						if (ctype.equals("irrelevants"))
//							System.out.println("~~~~~~~~~~~~~~~~~~~" + craw);
//							continue;
                        if (!wordExisted(craw, tfi))
                            continue;
                        addMap(ctByCate, ctype, craw);
                        addMap(client, craw, cid);
                        clientScore.put(cid + " " + craw, getClientScore(infoCT));
                        addMap(clientDebugInfo, gson.toJson(infoCT), numOfWord(craw, tfi));
                    }
                }
            }
        }
        clientFilter(ctByCate);
        return ctByCate;
    }

    private HashMap<Integer, ConcreteRisk> analyzeSegmentRT(HashMap<String, HashSet<String>> ctByCate, double weight) {
        HashMap<Integer, ConcreteRisk> riskInfo = new HashMap<>();
        rtRecognisor(riskInfo);
        rtHandler(riskInfo, ctByCate, weight);
        return riskInfo;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DeepRichBoltHelper.fields));
    }
}
