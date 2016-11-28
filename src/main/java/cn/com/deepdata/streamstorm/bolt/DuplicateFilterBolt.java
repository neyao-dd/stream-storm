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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.google.gson.Gson;

import cn.com.deepdata.streamstorm.controller.Action;
import cn.com.deepdata.streamstorm.controller.BloomFilterController;
import cn.com.deepdata.streamstorm.controller.EDeDupType;

@SuppressWarnings({ "serial", "rawtypes" })
public class DuplicateFilterBolt extends BaseRichBolt {
	private transient static Logger logger = LoggerFactory.getLogger(DuplicateFilterBolt.class);
	private JedisPoolConfig jedisPoolConfig;
	private transient JedisPool pool;
	private transient DeepRichBoltHelper helper;
	private transient BloomFilterController bloomFilter;

	private transient int dupCount;
	private transient int total;

	public DuplicateFilterBolt(JedisPoolConfig config) {
		// TODO Auto-generated constructor stub
		this.jedisPoolConfig = config;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		helper = new DeepRichBoltHelper(collector);
		pool = new JedisPool(jedisPoolConfig.getHost(), jedisPoolConfig.getPort());
		bloomFilter = new BloomFilterController();
		dupCount = 0;
		total = 0;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		EDeDupType deDupType = EDeDupType.ByUrl;
		Map<String, Object> doc = helper.getDoc(input);
		Map<String, Object> attach = helper.getAttach(input);
		if (attach.containsKey("action")) {
			Action actionObj = (Action) attach.get("action");
			deDupType = actionObj.deDupType;
		} else if (attach.containsKey("deDupType")) {
			deDupType = EDeDupType.valueOf((String) attach.get("deDupType"));
		}

		boolean dup = false;
		if (deDupType != EDeDupType.None) {
			total++;
			String strForDup = null;
			if (deDupType == EDeDupType.ByUrl && doc.containsKey("sup_url"))
				strForDup = (String) doc.get("sup_url");
			else if (deDupType == EDeDupType.ByHash && doc.containsKey("snp_hash"))
				strForDup = (String) doc.get("snp_hash");
			else {
				logger.error("需要排重的event中缺少hash或url字段");
				logger.error("doc:" + new Gson().toJson(doc));
			}
			if (strForDup != null) {
				try (Jedis jedis = pool.getResource()) {
					dup = bloomFilter.checkUrlExistAndInsert(jedis, strForDup);
				}
			}
		}
		if (!dup)
			helper.emit(input, true);
		else
			dupCount++;
		if (total % 30 == 0)
			logger.info("total count: {}, drop count: {}", total, dupCount);
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
