package cn.com.deepdata.streamstorm.bolt;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

@SuppressWarnings({ "serial", "rawtypes" })
public class ParserBolt extends BaseRichBolt {
	private transient static Log log = LogFactory.getLog(ParserBolt.class);
	private final Type mapType = new TypeToken<Map<String, String>>() {
	}.getType();
	private transient OutputCollector _collector;
	private transient Set<String> removeKeys;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		removeKeys = Sets.newHashSet("testflumeevent", "inp_task_id", "ajax",
				"enabled", "cReplyTime", "cReplyAuthor", "cList",
				"indexNameType", "downloadTime", "parseTime", "osname",
				"needParsing", "contentSelector");
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
			if (!doc.containsKey("action")) {
				log.error("docs need action. json:" + json);
				_collector.ack(input);
				return;
			}
			String action = doc.get("action");
			doc.remove("action");
			doc.keySet().stream().filter(k -> {
				return !k.startsWith("_") && !removeKeys.contains(k);
			}).forEach(k -> {
				try {
					Object newValue = parseValue(k, doc.get(k));
					if (newValue != null)
						newDoc.put(k, newValue);
				} catch (JsonSyntaxException e) {
					log.error("Json parse fail");
					log.error(k + ":" + doc.get(k));
				}
			});
			_collector.emit(new Values(doc, action));
		} catch (JsonParseException e) {
			log.error("parse json error. json:" + json);
			log.error("error", e);
		} finally {
			_collector.ack(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("doc", "action"));
	}

}
