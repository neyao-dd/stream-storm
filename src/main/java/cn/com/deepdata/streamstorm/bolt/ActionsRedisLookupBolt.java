package cn.com.deepdata.streamstorm.bolt;

import java.util.Map;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.ActionController;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

@SuppressWarnings({ "serial", "rawtypes" })
public class ActionsRedisLookupBolt extends AbstractRedisBolt {
	private static transient Logger logger = LoggerFactory.getLogger(ActionsRedisLookupBolt.class);
	private transient DeepRichBoltHelper helper;

	public ActionsRedisLookupBolt(JedisPoolConfig config) {
		super(config);
	}

	public ActionsRedisLookupBolt(JedisClusterConfig config) {
		super(config);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		super.prepare(map, topologyContext, collector);
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		String action = helper.getAction(input);
		if (action == null || action.length() == 0) {
			logger.error("no action:");
			logger.error("doc:" + new Gson().toJson(helper.getDoc(input)));
			helper.ack(input);
			return;
		}
		JedisCommands jedisCommands = null;
		Action actionObj = null;
		try {
			jedisCommands = getInstance();
			String key = "request_action_" + action;
			Map<String, String> info = jedisCommands.hgetAll(key);
			if (info != null && !info.isEmpty()) {
				actionObj = new Action(action, info.get("dedup_type"), info.get("analyze_type"), info.get("index_name"), info.get("index_type"));
			} else if (ActionController.actions.containsKey(action)) {
				actionObj = ActionController.actions.get(action);
			} else {
				logger.error("unknown action:" + action);
			}
		} finally {
			if (jedisCommands != null) {
				returnInstance(jedisCommands);
			}
		}
		if (actionObj != null) {
			Map<String, Object> attach = helper.getAttach(input);
			attach.put("action", actionObj);
			helper.emitAttach(input, attach, true);
		} else {
			logger.error("unknown action:" + action);
			logger.error("doc:" + new Gson().toJson(helper.getDoc(input)));
		}
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

}
