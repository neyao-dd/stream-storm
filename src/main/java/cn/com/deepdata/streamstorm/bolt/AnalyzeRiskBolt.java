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

import redis.clients.jedis.JedisCommands;

@SuppressWarnings({ "serial", "rawtypes" })
public class AnalyzeRiskBolt extends AbstractRedisBolt {
	private transient static Log log = LogFactory.getLog(AnalyzeRiskBolt.class);
	private transient DeepRichBoltHelper helper;

	public AnalyzeRiskBolt(JedisPoolConfig config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public AnalyzeRiskBolt(JedisClusterConfig config) {
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
		String title = helper.getDocTitle(input);
		String content = helper.getDocContent(input);
		Map<String, Object> attach = helper.getAttach(input);
		if (!attach.containsKey("titleRaw")
				&& !attach.containsKey("titleTermInfo")
				&& !attach.containsKey("contentRaw")
				&& !attach.containsKey("contentTermInfo")) {
			helper.emit(input, true);
			helper.ack(input);
			return;
		}
		JedisCommands jedisCommands = null;
		Map<String, Object> doc = helper.getDoc(input);
		try {
			jedisCommands = getInstance();
		} finally {
			if (jedisCommands != null) {
				returnInstance(jedisCommands);
			}
			helper.emitDoc(input, doc, true);
			helper.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

}
