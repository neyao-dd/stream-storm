package cn.com.deepdata.streamstorm.bolt;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import redis.clients.jedis.JedisCommands;

@SuppressWarnings({ "serial", "rawtypes" })
public class ActionsRedisLookupBolt extends AbstractRedisBolt {
	private transient static Log log = LogFactory
			.getLog(DuplicateFilterBolt.class);
	private transient DeepRichBoltHelper helper;

	public ActionsRedisLookupBolt(JedisPoolConfig config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public ActionsRedisLookupBolt(JedisClusterConfig config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		super.prepare(map, topologyContext, collector);
		helper = new DeepRichBoltHelper(collector);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String action = helper.getAction(input);
		if (action == null || action.length() == 0) {
			log.error("no action:");
			log.error("doc:" + new Gson().toJson(helper.getDoc(input)));
			helper.ack(input);
			return;
		}
		JedisCommands jedisCommands = null;
		Action actionObj = null;
		try {
			jedisCommands = getInstance();
			if (jedisCommands.sismember("flume_action_name", action)) {
				Map<String, String> info = jedisCommands
						.hgetAll("flume_action_" + action);
				actionObj = new Action(action, Boolean.parseBoolean(info
						.get("url_dedup")), info.get("dedup_mode"),
						Boolean.parseBoolean(info.get("need_analyze")),
						info.get("index_name"), info.get("index_type"));
			} else if (ActionController.actions.containsKey(action)) {
				actionObj = ActionController.actions.get(action);
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
			log.error("unknown action:" + action);
			log.error("doc:" + new Gson().toJson(helper.getDoc(input)));
		}
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

}
