package cn.com.deepdata.streamstorm.bolt;

import clojure.lang.Obj;
import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.commonutil.interlayer.sha1value.GetSha1Value;
import cn.com.deepdata.streamstorm.entity.ChangeRecord;
import cn.com.deepdata.streamstorm.entity.ChangedRisk;
import cn.com.deepdata.streamstorm.entity.Company;
import cn.com.deepdata.streamstorm.entity.RiskFields;
import cn.com.deepdata.streamstorm.util.CommonUtil;

import cn.com.deepdata.streamstorm.util.TypeProvider;
import com.google.gson.Gson;

import com.google.gson.reflect.TypeToken;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by yukh on 2016/10/31
 */
public class AnalyzeBusinessInfoBolt extends AbstractRedisBolt {
	private static final Logger logger = LoggerFactory.getLogger(BaseRichBolt.class);
	private String BUSINESS_INDEX = "flume-company-info";
	private String BUSINESS_CHANGE_INDEX = "flume-business-change";
	private String COMMON_TYPE = "flumetype";
	private int UNCERTAINTY_LEVEL = 4;
	private transient DeepRichBoltHelper helper;

	public AnalyzeBusinessInfoBolt(JedisPoolConfig config) {
		super(config);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		super.prepare(map, topologyContext, outputCollector);
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple tuple) {
		Map<String, Object> source = helper.getDoc(tuple);
		Map<String, Object> attach = helper.getAttach(tuple);
		try {
			String name = getCompanyName(source);
			if (!CommonUtil.validString(name)) {
				helper.emitAttach(tuple, attach, true);
				logger.error("business info doesn't have a name, analyze error.");
				return;
			}
			analyze(tuple, helper);
			helper.emitDoc(tuple, source, true);
			helper.ack(tuple);
		} catch (Exception e) {
			logger.error("analyze business info error...\n{}", CommonUtil.getExceptionString(e));
		} finally {
			helper.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

	private String getCompanyName(Map<String, Object> source) {
		if (source.containsKey("sca_name"))
			return source.get("sca_name").toString();
		else if (source.containsKey("scc_name"))
			return source.get("scc_name").toString();
		return null;
	}

	private List<Map<String, Object>> getChangeRecords(Map<String, Object> source) {
		if (source.containsKey("nnp_changerecords")) {
			List<Map<String, Object>> records = (List<Map<String, Object>>) source.get("nnp_changerecords");
			return records;
		}
		return null;
	}

	private Map<String, String> getItemInfo(String item) {
		String itemInfo = redisGet(RiskFields.BUSINESS_TERM_INFO_PREFIX, item);
		if (null == itemInfo)
			return null;
		return new Gson().fromJson(itemInfo, TypeProvider.type_mss);
	}

	private String getCompanyUuid(int id) {
		String clientDetail = redisGet(RiskFields.CLIENT_ITEM_INFO_PREFIX, String.valueOf(id));
		if (null == clientDetail)
			return null;
		Map<String, Object> detail =  new Gson().fromJson(clientDetail, TypeProvider.type_mso);
		return detail.get("uuid").toString();
	}

	private int getCompanyId(String company) {
		String clientInfo = redisGet(RiskFields.CLIENT_TERM_INFOS_PREFIX, company);
		if (clientInfo == null)
			return 0;
		Gson gson = new Gson();
		List<String> infoList = gson.fromJson(clientInfo, TypeProvider.type_ls);
		Map<String, String> info = gson.fromJson(infoList.get(0), TypeProvider.type_mss);
		return Integer.parseInt(info.get("id"));
	}

	private String redisGet(String key, String value) {
		JedisCommands jedisCommands = null;
		try {
			jedisCommands = getInstance();
			return jedisCommands.get(key + value);
		} catch (Exception e) {
			logger.error("redis get error. key is {}, value is {}", key, value);
		} finally {
			if (null != jedisCommands)
				returnInstance(jedisCommands);
		}
		return null;
	}

	private void analyze(Tuple tuple, DeepRichBoltHelper helper) {
		Map<String, Object> source = helper.getDoc(tuple);
		String name = getCompanyName(source);
		String[] param = new String[1];
		param[0] = name;
		source.put("_index", BUSINESS_INDEX);
		source.put("_type", COMMON_TYPE);
		source.put("_id", GetSha1Value.getSha1Value(param));

		List<Map<String, Object>> records = getChangeRecords(source);
		if (null == records)
			return;
		for (Map<String, Object> record : records) {
			String item = secureGet(record, "scc_change_item");
			if (null == item)
				return;
			String type, before, after;
			before = secureGet(record, "scc_before_content");
			after = secureGet(record, "scc_after_content");
			if (item.equals("")) {
				if (before != null && after != null && before.contains("：")) {
					item = before.substring(0, before.indexOf("："));
					if (!after.startsWith(item))
						return;
				} else
					return;
			}
			Map<String, String> itemInfo = getItemInfo(item);
			if (itemInfo == null) {
				String tmpItem = item;
				if (endWithBrackets(item)) {
					tmpItem = dropContentInBrackets(item);
					itemInfo = getItemInfo(tmpItem);
				}
				if (tmpItem.endsWith("变更"))
					itemInfo = getItemInfo(item.substring(0, tmpItem.length() - 2));
				if (itemInfo == null)
					logger.info("缺少工商变更item:{}", item);
			}
			int level = getRiskLevel(record, itemInfo);
			Map<String, Object> businessChange = new HashMap<>();
			businessChange.put("_index", BUSINESS_CHANGE_INDEX);
			businessChange.put("_type", COMMON_TYPE);
			businessChange.put("snc_origin_index", source.get("_index"));
			businessChange.put("snc_origin_type", source.get("_type"));
			businessChange.put("snc_origin_id", source.get("_id"));
			businessChange.put("sca_name", getCompanyName(source));
			businessChange.put("inp_seq_no", record.get("inp_seq_no"));
			businessChange.put("sca_change_item", item);
			if (itemInfo == null)
				type = "";
			else
				type = itemInfo.get("type");
			businessChange.put("sca_risk_type", type);
			businessChange.put("tfp_save_time", source.get("tfp_save_time"));
			businessChange.put("tfp_sort_time", record.get("tfc_change_date"));
			businessChange.put("tfc_change_date", record.get("tfc_change_date"));
			businessChange.put("scc_before_content", before);
			businessChange.put("scc_after_content", after);
			businessChange.put("inp_risk_level", level);
			String[] calcId = {
								item,
								record.get("tfc_change_date").toString(),
								before,
								after
							};
			businessChange.put("_id", GetSha1Value.getSha1Value(calcId));
			List<Map<String, Object>> risks = new ArrayList<>();
			risks.add(getNnaRisk(name, type, level));
			businessChange.put("nna_risks", risks);
			helper.emitDoc(tuple, businessChange, true);
		}

	}

	private String secureGet(Map<String, Object> map, String key) {
		if (map.containsKey(key))
			return map.get(key).toString().trim();
		return null;
	}

	private boolean endWithBrackets(String item) {
		return item.endsWith("）") || item.endsWith(")");
	}

	private String dropContentInBrackets(String item) {
		if (item.contains("（"))
			item = item.replace("（", "(");
		if (item.contains("("))
			return item.substring(0, item.indexOf("("));
		return item;
	}

	private Map<String, Object> getNnaRisk(String name, String type, int level) {
		Map<String, Object> risk = new HashMap<>();
		risk.put("sca_client_name", name);
		risk.put("sca_risk_type", type);
		risk.put("inp_risk_level", level);
		int id = getCompanyId(name);
		risk.put("ina_client_id", id);
		risk.put("snc_client_uuid", getCompanyUuid(id));
		return risk;
	}

	private int getRiskLevel (Map<String, Object> record, Map<String, String> itemInfo) {
		if (itemInfo == null)
			return 0;
		int level = Integer.parseInt(itemInfo.get("level"));
		if (level > 0)
			return level;
		try {
			double before = (double) record.get("scc_before_content");
			double after = (double) record.get("scc_after_content");
			return after > before ? 3 : 5;
		} catch (Exception e) {
			return UNCERTAINTY_LEVEL;
		}
	}

}
