package cn.com.deepdata.streamstorm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;

public class ESPrepareBolt extends BaseRichBolt {
	private transient DeepRichBoltHelper helper;
	private transient OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Object> source = helper.getDoc(input);
		_collector.emit(new Values(new Gson().toJson(source)));
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("json"));
	}

}
