package cn.com.deepdata.streamstorm.bolt;

import java.lang.reflect.Type;
import java.util.List;
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

import cn.com.deepdata.streamstorm.util.RESTUtil;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ESLookUpBolt extends BaseRichBolt {
	private transient static Log log = LogFactory.getLog(ESLookUpBolt.class);
	private transient OutputCollector _collector;
	private static final List<String> removeKeys = Lists.newArrayList("dna_max_risk", "dna_total_risk", "ina_risk_version", "nna_risks", "sna_riskDebugInfo",
			"sna_clientDebugInfo2", "nna_clients", "dna_regionRisk", "sna_regionRiskDebugInfo", "nna_regions", "nna_industryRisk", "sna_industryRiskDebugInfo",
			"ina_industry", "ina_industry2");

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String json = RESTUtil.getRequest(input.getString(1));
		Type mapType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> source = new Gson().fromJson(json, mapType);
		Map<String, Object> doc = (Map<String, Object>) source.get("_source");
		removeKeys.stream().forEach(k -> doc.remove(k));
		doc.put("_request_id", input.getLong(0));
		_collector.emit(new Values(doc));
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("doc"));
	}

}
