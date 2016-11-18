package cn.com.deepdata.streamstorm.bolt;

import java.lang.reflect.Type;
import java.util.Map;

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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class DocPreprocessBolt extends BaseRichBolt {
	private transient DeepRichBoltHelper helper;
	private transient static Log log = LogFactory.getLog(DocPreprocessBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stubx
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Gson gson = new Gson();
		Type mapType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> doc = gson.fromJson(input.getString(0), mapType);
		if (!doc.containsKey("analyzeType")) {
			log.error("need analyzeType in doc");
		} else if (!doc.containsKey("_index") || !doc.containsKey("_id")) {
			log.error("need _index and _id in doc");
		} else {
			doc.put("analyzeType", doc.get("analyzeType"));
			doc.remove("analyzeType");
			helper.emit(input, doc, "Update", Maps.newHashMap(), true);
		}
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

}
