package cn.com.deepdata.streamstorm.bolt;

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

import com.google.gson.Gson;

public class ESDataProcessBolt extends BaseRichBolt {
	private transient OutputCollector _collector;
	private transient static Log log = LogFactory.getLog(ESDataProcessBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stubx
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Gson gson = new Gson();
		Map<String, Object> doc = (Map<String, Object>) input.getValue(0);
		log.info("doc:" + gson.toJson(doc));
		doc.put("action", "addContents");
		doc.keySet().stream().forEach(k -> {
			Object value = doc.get(k);
			if (value == null) {
				log.error("value of " + k + " is null");
			} else if (value instanceof Iterable) {
				doc.put(k, gson.toJson(value));
			} else {
				doc.put(k, value.toString());
			}
		});
		_collector.emit(new Values(gson.toJson(doc)));
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("json"));
	}

}
