package cn.com.deepdata.streamstorm.bolt;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.google.gson.Gson;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.BloomFilterController;

@SuppressWarnings({ "serial", "rawtypes" })
public class DuplicateFilterBolt extends BaseRichBolt {
	private transient static Log log = LogFactory
			.getLog(DuplicateFilterBolt.class);
	private JedisPoolConfig jedisPoolConfig;
	private transient JedisPool pool;
	private transient DeepRichBoltHelper helper;
	private transient BloomFilterController bloomFilter;

	public DuplicateFilterBolt(JedisPoolConfig config) {
		// TODO Auto-generated constructor stub
		this.jedisPoolConfig = config;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		helper = new DeepRichBoltHelper(collector);
		pool = new JedisPool(jedisPoolConfig.getHost(),
				jedisPoolConfig.getPort());
		bloomFilter = new BloomFilterController();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		boolean needDup = true;
		Map<String, Object> doc = helper.getDoc(input);
		Map<String, Object> attach = helper.getAttach(input);
		String dupMode = "";
		if (attach.containsKey("action")) {
			Action actionObj = (Action) attach.get("action");
			needDup = actionObj.deDup;
			dupMode = actionObj.deDupMode;
		}

		boolean dup = false;
		if (needDup) {
			String strForDup = null;
			if (dupMode.equals("url") && doc.containsKey("sup_url"))
				strForDup = (String) doc.get("sup_url");
			else if (dupMode.equals("hash") && doc.containsKey("snp_hash"))
				strForDup = (String) doc.get("snp_hash");
			else {
				log.error("需要排重的event中缺少hash或url字段");
				log.error("doc:" + new Gson().toJson(doc));
			}
			if (strForDup != null) {
				try (Jedis jedis = pool.getResource()) {
					dup = bloomFilter.checkUrlExistAndInsert(jedis, strForDup);
				}
			}
		}
		if (!dup)
			helper.emit(input, true);
		helper.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}

	@Override
	public void cleanup() {
		if (pool != null)
			pool.destroy();
	}

}
