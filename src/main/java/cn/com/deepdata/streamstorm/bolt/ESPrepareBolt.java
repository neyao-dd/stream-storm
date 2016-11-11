package cn.com.deepdata.streamstorm.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.EIndexType;
import cn.com.deepdata.streamstorm.util.CommonUtil;

import com.google.common.collect.Lists;
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
		Lists.newArrayList("_index", "_type", "_id").stream().filter(k -> source.containsKey(k)).forEach(k -> {
			source.put("snp" + k, source.get(k));
			source.remove(k);
		});
		if (!source.containsKey("snp_type")) {
			source.put("snp_type", "flumetype");
		}
		if (!source.containsKey("snp_index")) {
			Map<String, Object> attach = helper.getAttach(input);
			Action actionObj = (Action) attach.get("action");

			List<String> indexNameComponents = Lists.newArrayList();
			indexNameComponents.add("flume");
			String sortTime = CommonUtil.getSortTime(source);
			if (actionObj.indexType == EIndexType.ByMonth)
				indexNameComponents.add(sortTime.substring(0, 7));
			else if (actionObj.indexType == EIndexType.ByDay)
				indexNameComponents.add(sortTime);
			indexNameComponents.add(actionObj.indexName);
			if (actionObj.name.equals("addContents")) {
				String info_type = (String) source.get("inp_type");
				if (info_type.equals("1")) {
					indexNameComponents.add("news");
				} else if (info_type.equals("2")) {
					indexNameComponents.add("weibo");
				} else if (info_type.equals("3")) {
					indexNameComponents.add("weixin");
				} else if (info_type.equals("4")) {
					indexNameComponents.add("forum");
				} else if (info_type.equals("5")) {
					indexNameComponents.add("tieba");
				}
			}
			source.put("snp_index", String.join("-", indexNameComponents));
		}
		_collector.emit(new Values(new Gson().toJson(source)));
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("json"));
	}

}
