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
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Object> attach = helper.getAttach(input);

		String streamId = "default";
		if (attach.containsKey("action")) {
			Action actionObj = (Action) attach.get("action");
			streamId = actionObj.analyzeType.name();
			helper.emit(input, true, streamId);
		} else if (attach.containsKey("analyzeType")) {
			streamId = (String) attach.get("analyzeType");
			helper.emit(input, true, streamId);
		}
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		Arrays.asList(EAnalyzeType.values()).stream().forEach(type -> declarer.declareStream(type.name(), new Fields(DeepRichBoltHelper.fields)));
	}

}
