package cn.com.deepdata.streamstorm.bolt;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;

import cn.com.deepdata.streamstorm.util.CommonUtil;
import cn.com.deepdata.streamstorm.util.RESTUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "serial", "rawtypes" })
public class ParserBolt extends BaseRichBolt {
	private transient static Logger logger = LoggerFactory.getLogger(ParserBolt.class);
	private transient DeepRichBoltHelper helper;
	private transient List<Integer> taskIds;
	private String radarHost;
	private String taskIdPath;
	private final Type mapType = new TypeToken<Map<String, String>>() {
	}.getType();
	private transient SimpleDateFormat format;

	public ParserBolt(String host, String path) {
		radarHost = host;
		taskIdPath = path;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		helper = new DeepRichBoltHelper(collector);
		taskIds = new ArrayList<>();
		format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public boolean validDate(String str) {
		if (str == null)
			return false;

		try {
			Date date = (Date) format.parse(str);
			return str.equals(format.format(date));
		} catch (Exception e) {
			logger.error("Not Valid");
			return false;
		}
	}

	private Map<String, Object> addTime(Map<String, Object> doc) {
		boolean validTfcTime = false;
		if (doc.containsKey("tfc_time")) {
			if (!validDate((String) doc.get("tfc_time"))) {
				logger.error("failed to parse [tfc_time], doc:" + doc.toString());
				doc.put("tfc_time", "2000-01-01 08:00:00");
			}
			validTfcTime = !doc.get("tfc_time").equals("2000-01-01 08:00:00");
		}
		boolean validUrlTime = false;
		if (doc.containsKey("tfc_url_time")) {
			if (!validDate((String) doc.get("tfc_url_time"))) {
				logger.error("failed to parse [tfc_url_time], header:"
						+ doc.toString());
				doc.put("tfc_url_time", "2000-01-01 08:00:00");
			}
			validUrlTime = !doc.get("tfc_url_time").equals(
					"2000-01-01 08:00:00");
		}
		doc.put("tfp_save_time", format.format(System.currentTimeMillis()));
		String sortTime;
		if (validTfcTime)
			sortTime = (String) doc.get("tfc_time");
		else if (validUrlTime)
			sortTime = (String) doc.get("tfc_url_time");
		else
			sortTime = (String) doc.get("tfp_save_time");
		doc.put("tfp_sort_time", sortTime);

		if (doc.containsKey("lnc_forward_count")) {
			String value = doc.get("lnc_forward_count").toString().trim();
			if (value.length() == 0)
				value = "0";
			doc.put("lnc_forward_count", value);
		}
		return doc;
	}

	private Object parseValue(String key, String value)
			throws JsonSyntaxException {
		Gson gson = new Gson();
		if (key.startsWith("n")) {
			Map[] arrayValue = null;
			arrayValue = gson.fromJson(value, Map[].class);
			return Arrays.asList(arrayValue);
		} else if (key.startsWith("a")) {
			String[] arrayValue = null;
			arrayValue = gson.fromJson(value, String[].class);
			return Arrays.asList(arrayValue);
		} else if (key.startsWith("o")) {
			Map objectValue = null;
			objectValue = gson.fromJson(value, Map.class);
			return objectValue;
		} else if (key.startsWith("sn")) {
			return value.trim();
		} else {
			return value;
		}
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String json = input.getString(0);
		Gson gson = new Gson();
		try {
			Map<String, String> doc = gson.fromJson(json, mapType);
			Map<String, Object> newDoc = Maps.newHashMap();
			if (doc == null) {
				throw new JsonParseException("parse return null");
			}
			if (!doc.containsKey("action") || doc.get("action") == null
					|| doc.get("action").toString().length() == 0) {
				logger.error("docs need action. json:" + json);
				helper.ack(input);
				return;
			}
			String action = doc.get("action");
			doc.remove("action");
			doc.remove("inp_radar_id");
			if (doc.get("action").equals("addContents") && doc.containsKey("inp_task_id")) {
				taskIds.add((int) (double) Double.parseDouble(doc.get("inp_task_id")));
				if (taskIds.size() >= 50) {
					postRadar(taskIds);
					taskIds.clear();
				}
			}
//			doc.remove("inp_task_id");
			doc.keySet().stream().filter(k -> {
				return k.indexOf("_") >= 0;
			}).forEach(k -> {
				try {
					Object newValue = parseValue(k, doc.get(k));
					if (newValue != null)
						newDoc.put(k, newValue);
				} catch (JsonSyntaxException e) {
					logger.error("Json parse fail");
					logger.error(k + ":" + doc.get(k));
				}
			});
			helper.emit(input, addTime(newDoc), action, Maps.newHashMap(),
					false);
		} catch (JsonParseException e) {
			logger.error("parse json error. json:" + json);
			logger.error("error", e);
		} finally {
			helper.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

	private void postRadar(List<Integer> ids) {
		String json = String
				.format("[{\"headers\":{\"action\":\"finishContents\", \"taskIds\":%s}, \"body\":\"\"}]",
						(new Gson()).toJson(ids));
		try {
			String result =	RESTUtil.postRequest(radarHost, taskIdPath, json);
			if (!result.contains("success"))
				logger.error("post task ids return fail.");
		} catch (Exception e) {
			logger.error("post task ids error.\n{}", CommonUtil.getExceptionString(e));
		}
	}

	@Override
	public void cleanup() {
		if (!taskIds.isEmpty()) {
			postRadar(taskIds);
		}
	}
}
