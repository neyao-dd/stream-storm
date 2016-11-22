package cn.com.deepdata.streamstorm.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.com.deepdata.streamstorm.util.RESTUtil;
import com.google.gson.Gson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.EIndexType;
import cn.com.deepdata.streamstorm.util.CommonUtil;

import com.google.common.collect.Lists;

@SuppressWarnings({ "serial", "rawtypes" })
public class ESPrepareBolt extends BaseRichBolt {
	private transient static Logger logger = LoggerFactory.getLogger(ESPrepareBolt.class);
	private transient DeepRichBoltHelper helper;
	private transient OutputCollector _collector;
	private static final String monthStream = "Month";
	private static final String upsertStream = "Upsert";
	private transient ObjectMapper objectMapper;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		helper = new DeepRichBoltHelper(collector);
		objectMapper = new ObjectMapper();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Object> source = helper.getDoc(input);
		Map<String, Object> attach = helper.getAttach(input);
		Action actionObj = (Action) attach.get("action");

		Lists.newArrayList("_index", "_type", "_id").stream().filter(k -> source.containsKey(k)).forEach(k -> {
			source.put("snp" + k, source.get(k));
			source.remove(k);
		});
		if (!source.containsKey("snp_type")) {
			source.put("snp_type", "flumetype");
		}
		if (!source.containsKey("snp_index")) {
			List<String> indexNameComponents = Lists.newArrayList();
			indexNameComponents.add("flume");
			String sortTime = CommonUtil.getSortTime(source);
			String saveTime = CommonUtil.getSaveTime(source);
			if (actionObj.indexType == EIndexType.ByMonth)
				indexNameComponents.add(saveTime.substring(0, 7));
			else if (actionObj.indexType == EIndexType.ByMonthDay)
				indexNameComponents.add(sortTime.substring(0, 7));
			else if (actionObj.indexType == EIndexType.ByDay)
				indexNameComponents.add(saveTime);
			String indexName = actionObj.indexName;
			if (indexName.startsWith("-"))
				indexName = indexName.substring(1);
			if (indexName.endsWith("-"))
				indexName = indexName.substring(0, indexName.length() - 1);
			indexNameComponents.add(indexName);
			if (actionObj.name.equals("addContents")) {
				String info_type = (String) source.get("inp_type");
				switch (info_type) {
					case "1":
						indexNameComponents.add("news");
					case "2":
						indexNameComponents.add("weibo");
					case "3":
						indexNameComponents.add("weixin");
					case "4":
						indexNameComponents.add("forum");
					case "5":
						indexNameComponents.add("tieba");
				}
			}
			source.put("snp_index", String.join("-", indexNameComponents));
		}
		try {
			Values values = new Values(objectMapper.writeValueAsString(source));
			if (source.containsKey("snp_id"))
				_collector.emit(upsertStream, values);
			else if (actionObj.indexType == EIndexType.ByMonthDay)
				_collector.emit(monthStream, values);
			else
				_collector.emit(values);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("JsonProcessingException", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("IOException", e);
		}
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(upsertStream, new Fields("json"));
		declarer.declareStream(monthStream, new Fields("json"));
		declarer.declare(new Fields("json"));
	}

	@Override
	public void cleanup() {
		objectMapper = null;
	}
}
