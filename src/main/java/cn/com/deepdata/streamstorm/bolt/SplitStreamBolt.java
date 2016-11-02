package cn.com.deepdata.streamstorm.bolt;

import java.util.Arrays;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.EAnalyzeType;

public class SplitStreamBolt extends BaseRichBolt {
	private transient DeepRichBoltHelper helper;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Object> attach = helper.getAttach(input);
		if (!attach.containsKey("action")) {
			helper.ack(input);
			return;
		}

		Action actionObj = (Action) attach.get("action");
		helper.emit(input, true, actionObj.analyzeType.name());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		Arrays.asList(EAnalyzeType.values())
				.stream()
				.forEach(
						type -> declarer.declareStream(type.name(), new Fields(
								DeepRichBoltHelper.fields)));
	}

}
