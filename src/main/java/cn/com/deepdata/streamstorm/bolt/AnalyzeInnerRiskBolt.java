package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.TermFrequencyInfo;
import cn.com.deepdata.streamstorm.entity.*;
import cn.com.deepdata.streamstorm.util.ClientUuidUtil;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import cn.com.deepdata.streamstorm.util.RegionUtil;
import cn.com.deepdata.streamstorm.util.TypeProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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

import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by yukh on 2016/10/19
 */
public class AnalyzeInnerRiskBolt extends AbstractRedisBolt {
	private static final Logger logger = LoggerFactory.getLogger(AnalyzeInnerRiskBolt.class);
	// private static final String keystoneRegionApi = "/keystone/api/v1/geo/area/_query";
	// private static final String keystoneIdApi = "/keystone/api/v1/company/_query?page=0&size=20000";
	private String calcType;
	private Map<Integer, Double> rWeight = new HashMap<>();
	private TermFrequencyInfo titleTfi;
	private List<TermFrequencyInfo> contentTfi;
	private Map<Integer, String> idMapping = new HashMap<>();
	// private String clientWordsCtrlVersion;
	// private String riskWordsCtrlVersion;

	double brandScore = 0.9;
	double brandScore2 = 0.25;
	double productScore = 0.8;
	double productScore2 = 0.2;
	double peopleScore = 0.8;
	double peopleScore2 = 0.2;
	double otherScore = 0.6;
	double otherScore2 = 0.15;
	private DeepRichBoltHelper helper;

	final double INVALID_WEIGHT = Double.MAX_VALUE;
	final String INVALID_CLIENT_TYPE = "longName";
	final String[] CT_CATE_CN = { "[品牌+]", "[品牌-]", "[产品+]", "[产品-]", "[人名+]", "[人名-]", "[其它+]", "[其它-]" };
	final String[] CT_CATE_EN = { "brand", "product", "people", "other" };
	final String[] SYMBOL = { "(", ")", "（", "）", "《", "》" };

	public AnalyzeInnerRiskBolt(JedisPoolConfig config) {
		super(config);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		super.prepare(map, topologyContext, collector);
		helper = new DeepRichBoltHelper(collector);
		JedisCommands jedisCommands = null;
		try {
			jedisCommands = getInstance();
			calcType = "paragraph";
			if (jedisCommands.get(RiskFields.brandScoreKey) != null) {
				brandScore = Double.parseDouble(jedisCommands.get(RiskFields.brandScoreKey));
				brandScore2 = Double.parseDouble(jedisCommands.get(RiskFields.brandScore2Key));
				productScore = Double.parseDouble(jedisCommands.get(RiskFields.productScoreKey));
				productScore2 = Double.parseDouble(jedisCommands.get(RiskFields.productScore2Key));
				peopleScore = Double.parseDouble(jedisCommands.get(RiskFields.peopleScoreKey));
				peopleScore2 = Double.parseDouble(jedisCommands.get(RiskFields.peopleScore2Key));
				otherScore = Double.parseDouble(jedisCommands.get(RiskFields.otherScoreKey));
				otherScore2 = Double.parseDouble(jedisCommands.get(RiskFields.otherScore2Key));
			}
		} catch (Exception e) {
			logger.error(e.toString());
		} finally {
			if (null != jedisCommands)
				returnInstance(jedisCommands);
		}
	}

	@Override
	public void execute(Tuple input) {
		Gson gson = new Gson();
		Map<String, Object> attach = helper.getAttach(input);
		Map<String, Object> source = helper.getDoc(input);
		init(attach);
		try {
			InnerRiskValue irv = new InnerRiskValue();
			int segNum = 1;
			String result;
			List<DescRiskScore> desRiskScore = new ArrayList<>();
			Map<String, Integer> clientInfo = new HashMap<>(); // ct info 频率
			Map<String, Set<String>> client = new HashMap<>(); // ct name [id]
			HashMap<Integer, Map<Integer, List<Double>>> desCli = new HashMap<>(); // 风险id 客户id 得分
			StringBuilder sb = new StringBuilder();
			Map<Integer, Double> totalCliScore = new HashMap<>();
			Map<String, Double> clientScore = new HashMap<>(); // cId+raw:score
			if (!source.get("inp_type").equals("2")) {
				String title = helper.getDocTitle(input); // 微博的inp_type == 2 微博没有标题
				result = analyzeSegment(title, 0, clientInfo, client, clientScore, desCli);
				sb.append(result);
			}
			String content = source.get("scc_content").toString();
			String[] segments = content.split("\r|\n");
			if (content.length() < 2 && content.length() > 500) {
				// TODO: 2016/10/20 返回都需要处理 ok
				helper.emitAttach(input, attach, true);
				helper.ack(input);
				return;
			}
			for (String segment : segments) {
				if (!segment.isEmpty()) {
					// TODO: 2016/10/20 参数
					result = analyzeSegment(segment, segNum++, clientInfo, client, clientScore, desCli);
					sb.append(result);
				}
			}
			Map<Integer, Map<String, Set<String>>> cliDebugInfo = new HashMap<>();
			convertClientDebugInfo(clientInfo, cliDebugInfo);
			getTotalCliScore(totalCliScore, clientScore);
			mapToList(desCli, totalCliScore, desRiskScore); // 加入list按分值排序
			desSort(desRiskScore);
			if (desRiskScore.size() > 0) {
				irv.maxRiskScore = desRiskScore.get(0).getDna_score();
				irv.riskScore = desRiskScore;
				for (DescRiskScore drs : desRiskScore) {
					irv.totalRiskScore += drs.getDna_score();
				}
			}
			for (int id : totalCliScore.keySet()) {
				double maxRiskScore = 0;
				double maxRiskScoreV2 = 0;
				ClientScore cs = new ClientScore();
				cs.setIna_id(id);
				cs.setSnc_uuid(getUuid(id));
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

//			logger.info("~~~~~~~~~~~~~~~~~~" + gson.toJson(irv));

			source.put("dna_max_risk", irv.maxRiskScore);
			source.put("dna_total_risk", irv.totalRiskScore);
			source.put("ina_risk_version", 3);
			source.put("nna_risks", Entity.getMap(irv.riskScore));
			if (irv.riskDebugInfo.length() > 0)
				source.put("sna_riskDebugInfo", irv.riskDebugInfo);
			source.put("nna_clients", Entity.getMap(irv.clientScore));
			if (irv.clientDebugInfo2.length() > 0)
				source.put("sna_clientDebugInfo2", irv.clientDebugInfo2);
			helper.emitDoc(input, source, true);
		} catch (Exception e) {
			logger.error("analyze inner risk error...");
			logger.error("Exception " + CommonUtil.getExceptionString(e));
		} finally {
			helper.ack(input);
		}
	}

	private void init(Map<String, Object> attach) {
		// clientWordsCtrlVersion = (String) attach.get("clientCtrlVersion");
		// riskWordsCtrlVersion = (String) attach.get("riskCtrlVersion");
		titleTfi = (TermFrequencyInfo) attach.get("titleTermInfo");
		contentTfi = (List<TermFrequencyInfo>) attach.get("contentTermInfo");
	}

	private void convertClientDebugInfo(Map<String, Integer> clientDebugInfo, Map<Integer, Map<String, Set<String>>> cliDebugInfo) {
		for (Map.Entry<String, Integer> entry : clientDebugInfo.entrySet()) {
			Gson gson = new Gson();
			Map<String, String> info = gson.fromJson(entry.getKey(), TypeProvider.type_mss);
			int id = Integer.parseInt(info.get("id"));
			String ctype = info.get("type");
			if (info.get("certain").equals("0"))
				ctype += "_un";
			// TODO: 2016/10/24 简化
			if (cliDebugInfo.containsKey(id)) {
				Map<String, Set<String>> map = cliDebugInfo.get(id);
				if (map.containsKey(ctype)) {
					Set<String> set = map.get(ctype);
					set.add(info.get("raw") + " " + entry.getValue());
				} else {
					Set<String> set = new HashSet<>();
					set.add(info.get("raw") + " " + entry.getValue());
					map.put(ctype, set);
				}
			} else {
				Map<String, Set<String>> map = new HashMap<>();
				Set<String> set = new HashSet<>();
				set.add(info.get("raw") + " " + entry.getValue());
				map.put(ctype, set);
				cliDebugInfo.put(id, map);
			}
		}
	}

	private void getTotalCliScore(Map<Integer, Double> totalCliScore, Map<String, Double> clientScore) {
		for (Map.Entry<String, Double> entry : clientScore.entrySet()) {
			int id = Integer.parseInt(entry.getKey().split(" ")[0]);
			double score = entry.getValue();
			addMap(totalCliScore, id, score);
		}
	}

	private void mapToList(Map<Integer, Map<Integer, List<Double>>> desCli, Map<Integer, Double> totalCliScore, List<DescRiskScore> desRiskScore) {
		for (int rid : desCli.keySet()) {
			for (int cid : desCli.get(rid).keySet()) {
				DescRiskScore drs = new DescRiskScore();
				drs.setIna_id(rid);
				drs.setIna_client_id(cid);
				drs.setDna_client_score(totalCliScore.get(cid));
				List<Double> scoreList = desCli.get(rid).get(cid);
				Iterator<Double> it = scoreList.iterator();
				double maxScore = 0;
				while (it.hasNext()) {
					double score = it.next();
					maxScore = Math.max(maxScore, score);
				}
				drs.setDna_score(Math.min(100, maxScore));
				drs.setSnc_uuid(getUuid(cid));
				if (!rWeight.isEmpty() && rWeight.containsKey(rid))
					drs.setDna_score_v2(Math.min(100, maxScore) * rWeight.get(rid));
				desRiskScore.add(drs);
			}
		}
	}

	private void desSort(List<DescRiskScore> desRiskScore) {
		Collections.sort(desRiskScore,
				(DescRiskScore o1, DescRiskScore o2) -> (o2.getDna_score() > o1.getDna_score() ? 1 : (o2.getDna_score() == o1.getDna_score() ? 0 : -1)));
	}

	private String analyzeSegment(String segment, int segNum, Map<String, Integer> clientInfo, Map<String, Set<String>> client,
			Map<String, Double> clientScore, Map<Integer, Map<Integer, List<Double>>> desCli) {
		StringBuilder sb = new StringBuilder();
		if (!segment.isEmpty()) {
			Map<String, Set<String>> ctByCate = analyzeSegmentCT(clientInfo, client, clientScore, segNum);
			Map<Integer, ConcreteRisk> rtInfo = analyzeSegmentRT(ctByCate, segNum);
			if (!rtInfo.isEmpty()) {
				List<ConcreteRisk> result = riskItemByClient(rtInfo, desCli, client, clientScore, segNum);
				// if (segNum == 8)
//				logger.info("@@@@@@@@@" + new Gson().toJson(result));
				if (!result.isEmpty())
					sb.append("segment:").append(segNum).append("\n").append(convertResult(result));
			}
		}
		return sb.toString();
	}

	private Map<String, Set<String>> analyzeSegmentCT(Map<String, Integer> clientInfo, Map<String, Set<String>> client, Map<String, Double> clientScore,
			int segNum) {
		Gson gson = new Gson();
		TermFrequencyInfo tfi = getTFI(segNum);
		Map<String, String> nature = tfi.termNature;
		Map<String, String> infoCT;
		// TODO: 2016/10/21 multiset
		Map<String, Set<String>> ctByCate = new HashMap<>();
		for (String word : nature.keySet()) {
			if (nature.get(word).contains("CT") || nature.get(word).contains("nr")) {
				Set<String> infoSet = getItemSet(RiskFields.CLIENT_TERM_INFOS_PREFIX, word);
				if (infoSet == null)
					continue;
				for (String str : infoSet) {
					infoCT = gson.fromJson(str, TypeProvider.type_mss);
					String id = infoCT.get("id");
					String raw = infoCT.get("raw");
					String type = infoCT.get("type");
					String uuid = getClientInfo(RiskFields.CLIENT_ITEM_INFO_PREFIX, Integer.parseInt(id)).get("uuid").toString();
					if (!wordExisted(raw, tfi) || !validClientType(type))
						continue;
					// TODO: 2016/10/21 结构简单处理
					idMapping.put(Integer.parseInt(id), uuid);
					addMap(ctByCate, type, raw);
					addMap(client, raw, id);
					clientScore.put(id + " " + raw, getClientScore(infoCT));
					addMap(clientInfo, gson.toJson(infoCT), numOfWord(raw, tfi));
				}
			}
		}
		clientFilter(ctByCate);
		return ctByCate;
	}

	private boolean validClientType(String type) {
		return !type.equals(INVALID_CLIENT_TYPE);
	}

	private TermFrequencyInfo getTFI(int segNum) {
		return segNum == 0 ? titleTfi : contentTfi.get(segNum - 1);
	}

	private void clientFilter(Map<String, Set<String>> ctByCate) {
		if (ctByCate.isEmpty() || !ctByCate.containsKey("irrelevants"))
			return;
		Set<String> irrelevant = new HashSet<>();
		ctByCate.get("irrelevants").stream().filter(irr -> irr.contains("*")).forEach(irr -> irrelevant.add(irr.split("\\*")[0]));
		ctByCate.remove("irrelevants");
		for (Set<String> set : ctByCate.values())
			set.removeAll(irrelevant);
	}

	private double getClientScore(Map<String, String> map) {
		double score;
		String type = map.get("type");
		switch (type) {
		case "brand":
			if (map.get("certain").equals("1"))
				score = brandScore;
			else
				score = brandScore2;
			break;
		case "products":
			if (map.get("certain").equals("1"))
				score = productScore;
			else
				score = productScore2;
			break;
		case "people":
			if (map.get("certain").equals("1"))
				score = peopleScore;
			else
				score = peopleScore2;
			break;
		case "other":
			if (map.get("certain").equals("1"))
				score = otherScore;
			else
				score = otherScore2;
			break;
		default:
			score = 0;
		}
		return score;
	}

	private Map<Integer, ConcreteRisk> analyzeSegmentRT(Map<String, Set<String>> ctByCate, int segNum) {
		Map<Integer, ConcreteRisk> riskInfo = new HashMap<>();
		rtRecogniser(riskInfo, segNum);
		rtHandler(riskInfo, ctByCate, segNum);
		return riskInfo;
	}

	private void rtRecogniser(Map<Integer, ConcreteRisk> riskInfo, int segNum) {
		Gson gson = new Gson();
		TermFrequencyInfo tfi = getTFI(segNum);
		Map<String, String> infoRT;
		Map<String, String> nature = tfi.termNature;
		for (String word : nature.keySet()) {
			if (nature.get(word).contains("RT")) {
				Set<String> infoSet = getItemSet(RiskFields.RISK_TERM_INFOS_PREFIX, word);
				for (String str : infoSet) {
					infoRT = gson.fromJson(str, TypeProvider.type_mss);
					int id = Integer.parseInt(infoRT.get("id"));
					String raw = infoRT.get("raw");
					String type = infoRT.get("type");
					int count = numOfWord(raw, tfi);
					if (0 == count)
						continue;
					if (type.equals("risk")) {
						try {
							double weight = Double.parseDouble(infoRT.get("weight"));
							rWeight.put(id, weight);
							addRisk(riskInfo, type, raw, count, id, weight);
						} catch (Exception e) {
							addRisk(riskInfo, type, raw, count, id, INVALID_WEIGHT);
							logger.error("raw:{}, type:{}, count:{}, id:{}", raw, type, count, id);
						}
					} else
						addRisk(riskInfo, type, raw, count, id, INVALID_WEIGHT);
				}
			}
		}
		// TODO: 2016/10/28 to delete
//		 logger.info("######riskInfo:{}" , gson.toJson(riskInfo));
	}

	private void addRisk(Map<Integer, ConcreteRisk> riskInfo, String type, String raw, int count, int id, double weight) {
		ConcreteRisk cr;
		if (riskInfo.containsKey(id))
			cr = riskInfo.get(id);
		else {
			cr = new ConcreteRisk();
			cr.setId(id);
			riskInfo.put(id, cr);
		}
		if (validWeight(weight))
			cr.setRiskWeight(weight);
		switch (type) {
		case "risk":
			cr.addRisk(raw, count);
			break;
		case "behavior":
			cr.addBehavior(raw, count);
			break;
		case "location":
			cr.addLocation(raw, count);
			break;
		case "rerisk":
			cr.addRerisk(raw, count);
			break;
		}
	}

	private boolean validWeight(double weight) {
		return weight != INVALID_WEIGHT;
	}

	private boolean validScore(double score) {
		return score > 0 && score <= 200;
	}

	private void rtHandler(Map<Integer, ConcreteRisk> riskInfo, Map<String, Set<String>> ctByCate, int segNum) {
		double weight = segNum > 1 ? 1. : segNum == 1 ? 1.2 : 1.5;
		Iterator<Map.Entry<Integer, ConcreteRisk>> it = riskInfo.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, ConcreteRisk> entry = it.next();
			int id = entry.getKey();
			ConcreteRisk cr = entry.getValue();
			String riskItem = getItemByRedis(RiskFields.RISK_ITEM_INFO_PREFIX, String.valueOf(id));
			RiskInfo item = new Gson().fromJson(riskItem, RiskInfo.class);
			addCTByCategory(cr, item.object, ctByCate, segNum);
			if (!validConcreteRisk(riskItem, cr)) {
				it.remove();
				continue;
			}
			reriskFilter(cr);
			cr.setWeight(weight);
			cr.setName(item.name);
			riskInfo.put(id, cr);
		}
	}

	private boolean validConcreteRisk(String riskItem, ConcreteRisk cr) {
		return !(null == riskItem || null == cr || cr.getRisk().isEmpty() || cr.getBehavior().isEmpty()
				|| cr.getObject().isEmpty()) && validLocation(riskItem, cr);
	}

	private boolean validLocation(String riskItem, ConcreteRisk cr) {
		RiskInfo info = new Gson().fromJson(riskItem, RiskInfo.class);
		return !(!info.location.isEmpty() && cr.getLocation().isEmpty());
	}

	private void addCTByCategory(ConcreteRisk cr, List<String> ctCate, Map<String, Set<String>> ct, int segNum) {
		TermFrequencyInfo tfi = getTFI(segNum);
		for (int i = 0; i < CT_CATE_EN.length; i++) {
			if (ctCate.contains(CT_CATE_CN[i * 2]) || ctCate.contains(CT_CATE_CN[i * 2 + 1]) && ct.containsKey(CT_CATE_EN[i])) {
				Set<String> set = ct.get(CT_CATE_EN[i]);
				// TODO: 2016/10/28 为什么Null
				if (set == null)
					continue;
				for (String word : set)
					cr.addObject(word, numOfWord(word, tfi));
			}
		}
	}

	// TODO: 2016/10/21 data cleaning
	public void concreteFilter(ConcreteRisk cr, int segNum) {
		TermFrequencyInfo tfi = getTFI(segNum);
		for (int i = 0; i < SYMBOL.length; i += 2) {
			symbolFilter(tfi, SYMBOL[i], SYMBOL[i + 1], cr.getRisk());
			symbolFilter(tfi, SYMBOL[i], SYMBOL[i + 1], cr.getBehavior());
			symbolFilter(tfi, SYMBOL[i], SYMBOL[i + 1], cr.getLocation());
		}
	}

	private void reriskFilter(ConcreteRisk cr) {
		cr.getRerisk().keySet().stream().filter(reword -> reword.contains("*")).forEach(reword -> cr.getRisk().remove(reword.split("\\*")[1]));
	}

	public void symbolFilter(TermFrequencyInfo tfi, String symLeft, String symRight, Map<String, Integer> riskWord) {
		Map<String, List<Integer>> offset = tfi.termOffsets;
		if (offset.containsKey(symLeft) && offset.containsKey(symRight)) {
			List<Integer> left = offset.get(symLeft);
			List<Integer> right = offset.get(symRight);
			int symCount = Math.min(left.size(), right.size());
			for (int i = 0; i < symCount; i++) {
				if (left.get(i) > right.get(i))
					continue;
				for (String word : riskWord.keySet())
					deleteWordInSymbol(offset, word, left.get(i), right.get(i));
			}
		}
	}

	/**
	 * 改变分词结果，文本分析放到最后，否则可能对其他分析造成影响
	 */
	private void deleteWordInSymbol(Map<String, List<Integer>> offset, String word, int left, int right) {
		List<Integer> wordPos = offset.get(word);
		Iterator<Integer> it = wordPos.iterator();
		while (it.hasNext()) {
			int pos = it.next();
			if (pos < right && pos > left) {
				it.remove();
			}
		}
		if (wordPos.isEmpty())
			offset.remove(word);
	}

	private List<ConcreteRisk> riskItemByClient(Map<Integer, ConcreteRisk> rtInfo, Map<Integer, Map<Integer, List<Double>>> desCli,
			Map<String, Set<String>> client, Map<String, Double> clientScore, int segNum) {
		Map<String, ConcreteRisk> merge = new HashMap<>();
		// 每个风险信息只对应一个客户id, 一个客户id可能有多个people brand等
		for (int rid : rtInfo.keySet()) {
			Map<String, Integer> mapObj = rtInfo.get(rid).getObject();
			for (String wordObj : mapObj.keySet()) {
				Set<String> idSet = client.get(wordObj);
				if (!idSet.isEmpty()) {
					for (String cid : idSet) {
						// ConcreteRisk cr;
						// try{
						// cr = (ConcreteRisk) rtInfo.get(rid).clone();
						// } catch (CloneNotSupportedException e) {
						// logger.error("ConcreteRisk clone() fail...");
						// return null;
						// }
						// cr.setId(rid);
						// cr.setClientId(Integer.parseInt(cid));
						// cr.setObject(wordObj, mapObj.get(wordObj));
						// TODO: 2016/10/28 简化
						ConcreteRisk cr = new ConcreteRisk();
						cr.setId(rid);
						cr.setClientId(Integer.parseInt(cid));
						cr.setObject(wordObj, mapObj.get(wordObj));
						cr.setName(rtInfo.get(rid).getName());
						cr.setBehavior(rtInfo.get(rid).getBehavior());
						cr.setLocation(rtInfo.get(rid).getLocation());
						cr.setRisk(rtInfo.get(rid).getRisk());
						cr.setWeight(rtInfo.get(rid).getWeight());
						if (!rWeight.isEmpty() && rWeight.containsKey(rid))
							cr.setRiskWeight(rWeight.get(rid));
						cr.setUuid(getUuid(Integer.parseInt(cid)));
						double score = clientScore.get(cr.getClientId() + " " + wordObj);
						cr.setClientScore(score);

						// TODO: 2016/10/24 client same ids
						String idKey = String.valueOf(cr.getId()) + cid;
						if (merge.containsKey(idKey)) {
							ConcreteRisk crInMap = merge.get(idKey);
							Map<String, Integer> tempObj = crInMap.getObject();
							crInMap.setClientScore(Math.min(crInMap.getClientScore() + cr.getClientScore(), 1.));
							tempObj.putAll(cr.getObject());
						} else
							merge.put(idKey, cr);
					}
				}
			}
		}
		// if(segNum == 8)
//		logger.info("@@@@@@@merge:" + new Gson().toJson(merge));
		return riskItem(merge, desCli, segNum);
	}

	private String getUuid(int id) {
		if (idMapping.containsKey(id))
			return idMapping.get(id);
		return "";
	}

	private List<ConcreteRisk> riskItem(Map<String, ConcreteRisk> merge, Map<Integer, Map<Integer, List<Double>>> desCli, int segNum) {
		TermFrequencyInfo tfi = getTFI(segNum);
		List<ConcreteRisk> riskItem = new ArrayList<>();
		for (Map.Entry<String, ConcreteRisk> entry : merge.entrySet()) {
			ConcreteRisk cr = entry.getValue();
			double score = calRiskScoreBySentence(cr, tfi.termOffsets);
			// TODO: 2016/11/8 delete
//			logger.info("%%%%%:" + new Gson().toJson(cr));
			score = numberFormat(score);
			score *= cr.getClientScore();
//			logger.info("score3 :" + score + ", client: " + cr.getClientScore() + ", segNum:" + segNum);
			cr.setScore(Math.min(100, score));
			cr.setScore_v2(Math.min(100, score * cr.getRiskWeight()));
			if (!validScore(cr.getScore()))
				continue;
//			logger.info("CCCCCCCCC{}", new Gson().toJson(cr));
			riskItem.add(cr);
			int cid = cr.getClientId();
			int rid = cr.getId();
			addMap(desCli, rid, cid, score);
		}
		return riskItem;
	}

	private double numberFormat(double n) {
		try (Formatter fm = new Formatter()) {
			n = Double.parseDouble(fm.format("%.5f", n).toString());
		} catch (Exception e) {
			logger.error("number cast error." + CommonUtil.getExceptionString(e));
		}
		return n;
	}

	private String convertResult(List<ConcreteRisk> lc) {
		StringBuilder sb = new StringBuilder();
		for (ConcreteRisk cr : lc) {
			ResultRisk rr = cr.parseRR();
			sb.append("risk:").append(new Gson().toJson(rr)).append("\n");
		}
		return sb.toString();
	}

	public double calRiskScoreBySentence(ConcreteRisk cr, Map<String, List<Integer>> offset) {
		Gson gson = new Gson();
		double score;
		int minDistanceBR = Integer.MAX_VALUE;
		int minDistance = Integer.MAX_VALUE;
		int avgPosition = Integer.MAX_VALUE;
		List<Integer> avgPosList = new ArrayList<>();
		boolean isList = false;
		Type type = new TypeToken<List<Integer>>() {
		}.getType();

		Map<String, Integer> mapBehavior = cr.getBehavior();
		Map<String, Integer> mapRisk = cr.getRisk();
		Map<String, Integer> mapObject = cr.getObject();
		initCRpos(cr, offset);

		int frequency = cr.getRisk().size() + cr.getBehavior().size();
		for (String wordBeh : mapBehavior.keySet()) {
			for (String wordRisk : mapRisk.keySet()) {
				if (isInTheSameSen(wordBeh, wordRisk, offset)) {
					Map<String, String> map = minDistance(wordBeh, wordRisk, offset);
					int tmpMinDis = Integer.parseInt(map.get("min"));
					if (tmpMinDis < minDistanceBR) {
						minDistanceBR = tmpMinDis;
						if (map.containsKey("avgInt")) {
							avgPosition = Integer.parseInt(map.get("avgInt"));
							isList = false;
						} else {
							avgPosList = gson.fromJson(map.get("avgArray"), type);
							isList = true;
						}
					}
				}
			}
		}
		score = calScoreByDistance(minDistanceBR, 70, 70, 35);
		score *= (Math.min(Math.log(frequency + Math.E), 1));
		for (String wordObj : mapObject.keySet()) {
			if (wordObj.contains("*")) {
				String[] words = wordObj.split("\\*");
				if (!isInTheSameSen(words, offset))
					continue;
				wordObj = words[0].length() > words[1].length() ? words[0] : words[1];
			}
			for (int posObj : offset.get(wordObj)) {
				if (isList) {
					for (int avgpos : avgPosList) {
						if (calcType.equals("sentence"))
							if (!isInTheSameSen(posObj, avgpos, offset))
								continue;
						int tmpDistance = getDistance(posObj, avgpos, wordObj, "平均");
						if (tmpDistance < minDistance) {
							minDistance = tmpDistance;
							cr.addWordpos(wordObj, posObj);
						}
					}
				} else {
					if (calcType.equals("sentence"))
						if (isInTheSameSen(posObj, avgPosition, offset))
							continue;
					int tmpDistance = getDistance(posObj, avgPosition, wordObj, "平均");
					if (tmpDistance < minDistance) {
						minDistance = tmpDistance;
						cr.addWordpos(wordObj, posObj);
					}
				}
			}
		}
		score *= calScoreByDistance(minDistance, 140, 140, 70);
		return score < 0 ? 0 : score * cr.getWeight() * 100;
	}

	private Map<String, String> minDistance(String word1, String word2, Map<String, List<Integer>> offset) {
		Map<String, String> result = new HashMap<>();
		Gson gson = new Gson();
		int minDistance = Integer.MAX_VALUE;
		int avgPosition = Integer.MAX_VALUE;
		Map<Integer, String> wordpos = new HashMap<>();
		List<Integer> position = new ArrayList<>();
		if (offset.containsKey("。"))
			position.addAll(offset.get("。"));
		if (word2.contains("*")) {
			if (word1.equals(word2)) {
				String[] sl = word1.split("\\*");
				return twoWordsminDistance(sl[0], sl[1], offset);
			} else {
				String[] sarray = new String[4];
				String[] list2 = word2.split("\\*");
				String[] list1 = word1.split("\\*");
				System.arraycopy(list1, 0, sarray, 0, list1.length);
				System.arraycopy(list2, 0, sarray, 2, list2.length);
				for (String word : sarray) {
					for (int pos : offset.get(word)) {
						wordpos.put(pos, word);
					}
					position.addAll(offset.get(word));
				}
				Collections.sort(position);
				for (int i = 0; i < position.size() - 1; i++) {
					int pos1 = position.get(i);
					int pos2 = position.get(i + 1);
					if (wordpos.containsKey(pos1) && wordpos.containsKey(pos2)) {
						int bp1 = convertInt(word1.contains(wordpos.get(pos1)));
						int bp2 = convertInt(word1.contains(wordpos.get(pos2)));
						if (bp1 + bp2 == 1) {
							int dis = getDistance(pos1, pos2, wordpos.get(pos1), wordpos.get(pos2));
							if (minDistance > dis) {
								minDistance = dis;
								avgPosition = (pos1 + pos2) / 2;
							}
						}
					}
				}
			}
		} else if (word1.contains("*")) {
			if (word1.contains(word2)) {
				result.put("min", "0");
				result.put("avgArray", gson.toJson(offset.get(word2)));
				return result;
			} else {
				String[] sarray = new String[3];
				String[] sl = word1.split("\\*");
				sarray[0] = sl[0];
				sarray[1] = sl[1];
				sarray[2] = word2;
				for (String word : sarray) {
					for (int pos : offset.get(word)) {
						wordpos.put(pos, word);
					}
					position.addAll(offset.get(word));
				}
				Collections.sort(position);
				for (int i = 0; i < position.size() - 1; i++) {
					int pos1 = position.get(i);
					int pos2 = position.get(i + 1);
					if (wordpos.containsKey(pos1) && wordpos.containsKey(pos2)) {
						if (convertInt(wordpos.get(pos1).equals(word2)) + convertInt(wordpos.get(pos2).equals(word2)) == 1) {
							int dis = getDistance(pos1, pos2, wordpos.get(pos1), wordpos.get(pos2));
							if (minDistance > dis) {
								minDistance = dis;
								avgPosition = (pos1 + pos2) / 2;
							}
						}
					}
				}
			}
		} else {
			return twoWordsminDistance(word1, word2, offset);
		}
		result.put("min", String.valueOf(minDistance));
		result.put("avgInt", String.valueOf(avgPosition));
		return result;
	}

	private int convertInt(boolean b) {
		if (b)
			return 1;
		else
			return 0;
	}

	private Map<String, String> twoWordsminDistance(String word1, String word2, Map<String, List<Integer>> offset) {
		Map<String, String> result = new HashMap<>();
		Map<Integer, String> wordpos = new HashMap<>();
		List<Integer> position = new ArrayList<>();
		if (offset.containsKey("。"))
			position.addAll(offset.get("。"));
		int minDistance = Integer.MAX_VALUE;
		int avgPosition = Integer.MAX_VALUE;
		if (word1.equals(word2)) {
			result.put("min", "0");
			result.put("avgArray", new Gson().toJson(offset.get(word1)));
			return result;
		}
		String[] sl = new String[2];
		sl[0] = word1;
		sl[1] = word2;
		for (String word : sl) {
			for (int pos : offset.get(word)) {
				wordpos.put(pos, word);
			}
			position.addAll(offset.get(word));
		}
		Collections.sort(position);
		for (int i = 0; i < position.size() - 1; i++) {
			int pos1 = position.get(i);
			int pos2 = position.get(i + 1);
			if (wordpos.containsKey(pos1) && wordpos.containsKey(pos2)) {
				if (!wordpos.get(pos1).equals(wordpos.get(pos2))) {
					int dis = getDistance(pos1, pos2, wordpos.get(pos1), wordpos.get(pos2));
					if (minDistance > dis) {
						minDistance = dis;
						avgPosition = (pos1 + pos2) / 2;
					}
				}
			}
		}
		result.put("min", String.valueOf(minDistance));
		result.put("avgInt", String.valueOf(avgPosition));
		return result;
	}

	private double calScoreByDistance(double distance, double maxDistance, double c1, double c2) {
		return distance > maxDistance ? 0 : Math.log(c1 / c2 - distance / c2 + 1) / Math.log(c1 / c2 + 1);
	}

	public int getDistance(int pos1, int pos2, String word1, String word2) {
		return Math.min(Math.abs(pos1 - pos2 - word2.length()), Math.abs(pos2 - pos1 - word1.length()));
	}

	private void initCRpos(ConcreteRisk cr, Map<String, List<Integer>> offset) {
		initpos(cr, offset, cr.getBehavior());
		initpos(cr, offset, cr.getRisk());
		initpos(cr, offset, cr.getObject());
		initpos(cr, offset, cr.getLocation());
	}

	private void initpos(ConcreteRisk cr, Map<String, List<Integer>> offset, Map<String, Integer> words) {
		if (offset == null || offset.isEmpty())
			return;
		Iterator<Map.Entry<String, Integer>> it = words.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry e = it.next();
			String word = (String) e.getKey();
			if (word.contains("*")) {
				String[] sl = word.split("\\*");
				if (isInTheSameSen(sl, offset)) {
					cr.addWordpos(sl[0], offset.get(sl[0]).get(0));
					cr.addWordpos(sl[1], offset.get(sl[1]).get(0));
				} else
					it.remove();
			} else {
				if (offset.containsKey(word))
					cr.addWordpos(word, offset.get(word).get(0));
				else
					it.remove();
			}
		}
	}

	// TODO: 2016/10/24
	public boolean isInTheSameSen(String[] sl, Map<String, List<Integer>> offset) {
		// 处理风险行为同词
		if (sl[0].equals(sl[1]) || !offset.containsKey("。"))
			return true;
		if (!offset.containsKey(sl[0]) || !offset.containsKey(sl[1]))
			return false;
		Map<Integer, String> wordPosition = new HashMap<>();
		List<Integer> position = new ArrayList<>();
		position.addAll(offset.get("。"));

		for (String word : sl) {
			for (int pos : offset.get(word)) {
				wordPosition.put(pos, word);
			}
			position.addAll(offset.get(word));
		}
		Collections.sort(position);
		for (int i = 0; i < position.size() - 1; i++) {
			int pos1 = position.get(i);
			int pos2 = position.get(i + 1);
			if (wordPosition.containsKey(pos1) && wordPosition.containsKey(pos2)) {
				if (!wordPosition.get(pos1).equals(wordPosition.get(pos2))) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean isInTheSameSen(String word1, String word2, Map<String, List<Integer>> offset) {
		String[] list;
		if (!word1.contains("*") && !word2.contains("*")) {
			list = new String[2];
			list[0] = word1;
			list[1] = word2;
			return isInTheSameSen(list, offset);
		}
		if (word1.equals(word2)) {
			list = word1.split("\\*");
			return isInTheSameSen(list, offset);
		} else {
			if (!word1.contains("*") && word2.contains("*")) {
				String tmp = word1;
				word1 = word2;
				word2 = tmp;
			}
			if (word2.contains("*"))
				list = new String[4];
			else
				list = new String[3];
			if (word2.contains("*")) {
				String[] list2 = word2.split("\\*");
				String[] list1 = word1.split("\\*");
				System.arraycopy(list1, 0, list, 0, list1.length);
				System.arraycopy(list2, 0, list, 2, list2.length);
			} else {
				String[] tmp = word1.split("\\*");
				list[0] = tmp[0];
				list[1] = tmp[1];
				list[2] = word2;
			}
		}
		Map<Integer, String> wordpos = new HashMap<>();
		if (!offset.containsKey("。"))
			return true;
		List<Integer> position = new ArrayList<>();
		position.addAll(offset.get("。"));
		for (String word : list) {
			for (int pos : offset.get(word)) {
				wordpos.put(pos, word);
			}
			position.addAll(offset.get(word));
		}
		Collections.sort(position);
		try {
			for (int i = 0; i < position.size();) {
				Set<String> set = new HashSet<>();
				while (i < position.size()) {
					String word = wordpos.get(position.get(i++));
					if (word == null)
						break;
					if (!word.equals("。"))
						set.add(word);
					else
						break;
				}
				if (set.size() == list.length) {
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean isInTheSameSen(String word, Map<String, List<Integer>> offset) {
		if (word.contains("*"))
			return isInTheSameSen(word.split("\\*"), offset);
		return offset.containsKey(word);
	}

	public boolean isInTheSameSen(int pos1, int pos2, Map<String, List<Integer>> offset) {
		if (!offset.containsKey("。"))
			return true;
		if (pos1 > pos2) {
			int tmp = pos2;
			pos2 = pos1;
			pos1 = tmp;
		}
		List<Integer> position = offset.get("。");
		for (int pos : position) {
			if (pos > pos1 && pos < pos2)
				return false;
		}
		return true;
	}

	// TODO: 2016/11/8 和下面统一
	private Map<String, Object> getClientInfo(String key, int id) {
		if (validString(key) && validClientId(id)) {
			String info = getItemByRedis(key, String.valueOf(id));
			if (validString(info))
				return new Gson().fromJson(info, TypeProvider.type_mso);
		}
		return new HashMap<>();
	}

	private boolean validString(String s) {
		return !(s == null || s.length() == 0);
	}

	// TODO: 2016/11/8 其他附加条件
	private boolean validClientId(int id) {
		return id >= 0;
	}

	private Set<String> getItemSet(String key, String word) {
		String info = getItemByRedis(key, word);
		if (!validString(info))
			return new HashSet<>();
		return new Gson().fromJson(info, TypeProvider.type_ss);
	}

	// // TODO: 2016/11/7 刚刚删除时访问，空指针的问题
	private String getItemByRedis(String query, String word) {
		JedisCommands jedisCommands = getInstance();
		String item = jedisCommands.get(query + word);
		returnInstance(jedisCommands);
		return item;
	}

	private boolean wordExisted(String word, TermFrequencyInfo tfi) {
		return numOfWord(word, tfi) > 0;
	}

	private int numOfWord(String word, TermFrequencyInfo tfi) {
		HashMap<String, Integer> frequency = tfi.termFrequency[0];
		HashMap<String, List<Integer>> offset = tfi.termOffsets;
		if (word.contains("*")) {
			String[] sl = word.split("\\*");
			if (!frequency.containsKey(sl[0]) || !frequency.containsKey(sl[1]))
				return 0;
			else if (!isInTheSameSen(sl, offset))
				return 0;
			else
				return Math.min(frequency.get(sl[0]), frequency.get(sl[1]));
		} else {
			try {
				return !frequency.containsKey(word) ? 0 : frequency.get(word);
			} catch (Exception e) {
				logger.error("{}, offset : {}", word, offset.toString());
				logger.error("{}, tfre : {}", word, frequency.toString());
				return 0;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

	// TODO: 2016/10/24 multimap
	public void addMap(Map<Integer, Double> map, int key, double value) {
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + value);
		} else {
			map.put(key, value);
		}
	}

	public void addMap(Map<String, Integer> map, String key, int value) {
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + value);
		} else {
			map.put(key, value);
		}
	}

	public void addMap(Map<String, Set<String>> map, String key, String value) {
		if (map.containsKey(key)) {
			Set<String> set = map.get(key);
			set.add(value);
		} else {
			Set<String> set = new HashSet<>();
			set.add(value);
			map.put(key, set);
		}
	}

	public void addMap(Map<Integer, Map<Integer, List<Double>>> map, int key, int innerKey, double innerValue) {
		if (map.containsKey(key)) {
			Map<Integer, List<Double>> inner = map.get(key);
			if (inner.containsKey(innerKey)) {
				List<Double> list = inner.get(innerKey);
				list.add(innerValue);
			} else {
				List<Double> list = new ArrayList<>();
				list.add(innerValue);
				inner.put(innerKey, list);
			}
		} else {
			Map<Integer, List<Double>> inner = new HashMap<>();
			List<Double> list = new ArrayList<>();
			list.add(innerValue);
			inner.put(innerKey, list);
			map.put(key, inner);
		}
	}
}