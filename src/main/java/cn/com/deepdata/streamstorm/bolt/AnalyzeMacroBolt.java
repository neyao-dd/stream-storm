package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.streamstorm.entity.Entity;
import cn.com.deepdata.streamstorm.entity.Tag;
import cn.com.deepdata.streamstorm.util.CommonUtil;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yukh on 2016/10/9.
 */
public class AnalyzeMacroBolt extends AbstractRedisBolt {

	private static final Logger logger = LoggerFactory.getLogger(AnalyzeMacroBolt.class);
	private transient DeepRichBoltHelper helper;

	final String macroCountryFilter = "macro_analysis_country_filter";
	final String macroRegionFilter = "macro_analysis_region_filter";
	final String countryNecessary = "macro_analysis_country_necessary";
	final String macroCommonFilter = "macro_analysis_common_filter";
	Set<String> countryFilter;
	Set<String> regionFilter;
	Set<String> countryNes;
	Set<String> macroCommon;
	private transient Boolean initialized;

	public AnalyzeMacroBolt(JedisPoolConfig config) {
		super(config);
	}

	public AnalyzeMacroBolt(JedisClusterConfig config) {
		super(config);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		super.prepare(map, topologyContext, collector);
		helper = new DeepRichBoltHelper(collector);
		initialized = false;
	}

	protected void lazyInit() {
		if (initialized)
			return;
		JedisCommands jedisCommands = null;
		try {
			jedisCommands = getInstance();
			countryFilter = jedisCommands.smembers(macroCountryFilter);
			regionFilter = jedisCommands.smembers(macroRegionFilter);
			countryNes = jedisCommands.smembers(countryNecessary);
			macroCommon = jedisCommands.smembers(macroCommonFilter);
			initialized = true;
		} catch (Exception e) {
			logger.error(e.toString());
		} finally {
			if (null != jedisCommands)
				returnInstance(jedisCommands);
		}
	}

	@Override
	public void execute(Tuple input) {
		lazyInit();
		if (initialized) {
			String content = helper.getDocTitle(input) + " " + helper.getDocContent(input);
			Map<String, Object> source = helper.getDoc(input);
			List<Tag> tags = helper.getTagList(source);
			if (containsMacroGlobal(content))
				tags.add(new Tag("macro_analysis_global"));
			if (containsMacroLocal(content))
				tags.add(new Tag("macro_analysis_local"));
			try {
				source.put("nna_tags", Entity.getMap(tags));
			} catch (Exception e) {
				logger.error("set macro tags error...\n" + CommonUtil.getExceptionString(e));
			}
			helper.emitDoc(input, source, true);
		}
		helper.ack(input);
	}

	private boolean containsMacroLocal(String art) {
		for (String word : regionFilter) {
			if (art.contains(word))
				return false;
		}

		return containsMacroCommon(art);
	}

	private boolean containsMacroGlobal(String art) {
		for (String word : countryFilter) {
			if (art.contains(word))
				return false;
		}

		for (String word : countryNes) {
			if (art.contains(word))
				break;
		}

		return containsMacroCommon(art);
	}

	private boolean containsMacroCommon(String art) {
		int count = 0;
		for (String word : macroCommon) {
			if (art.contains(word))
				count++;
			if (count == 2)
				return true;
		}
		return false;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(DeepRichBoltHelper.fields));
	}
}
