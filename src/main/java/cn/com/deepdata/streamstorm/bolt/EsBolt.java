package cn.com.deepdata.streamstorm.bolt;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_FLUSH_MANUAL;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_SIZE_ENTRIES;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.storm.TupleUtils;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.serialization.StormTupleBytesConverter;
import org.elasticsearch.storm.serialization.StormTupleFieldExtractor;
import org.elasticsearch.storm.serialization.StormValueWriter;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import cn.com.deepdata.esstorm.BulkResponse;
import cn.com.deepdata.esstorm.PartitionWriter;

@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public class EsBolt implements IRichBolt {

	private transient static Log log = LogFactory.getLog(EsBolt.class);

	private Map boltConfig = new LinkedHashMap();

	private transient PartitionWriter writer;
	private transient boolean flushOnTickTuple = true;
	private boolean emitTuples = true;

	private transient List<Tuple> inflightTuples = null;
	private transient int numberOfEntries = 0;
	private transient OutputCollector collector;

	public EsBolt(String target, boolean emitTuples) {
		this.emitTuples = emitTuples;
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public EsBolt(String target, Map configuration, boolean emitTuples) {
		this.emitTuples = emitTuples;
		boltConfig.putAll(configuration);
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		LinkedHashMap copy = new LinkedHashMap(conf);
		copy.putAll(boltConfig);

		StormSettings settings = new StormSettings(copy);
		flushOnTickTuple = settings.getStormTickTupleFlush();

		// trigger manual flush
		settings.setProperty(ES_BATCH_FLUSH_MANUAL, Boolean.TRUE.toString());

		// align Bolt / es-hadoop batch settings
		numberOfEntries = settings.getStormBulkSize();
		settings.setProperty(ES_BATCH_SIZE_ENTRIES,
				String.valueOf(numberOfEntries));

		inflightTuples = new ArrayList<Tuple>(numberOfEntries + 1);

		int totalTasks = context
				.getComponentTasks(context.getThisComponentId()).size();

		InitializationUtils.setValueWriterIfNotSet(settings,
				StormValueWriter.class, log);
		InitializationUtils.setBytesConverterIfNeeded(settings,
				StormTupleBytesConverter.class, log);
		InitializationUtils.setFieldExtractorIfNotSet(settings,
				StormTupleFieldExtractor.class, log);

		writer = PartitionWriter.createWriter(settings,
				context.getThisTaskIndex(), totalTasks, log);
	}

	public void execute(Tuple input) {
		if (flushOnTickTuple && TupleUtils.isTickTuple(input)) {
			flush();
			return;
		}
		inflightTuples.add(input);
		try {
			writer.repository.writeToIndex(input);

			// manual flush in case of ack writes - handle it here.
			if (numberOfEntries > 0 && inflightTuples.size() >= numberOfEntries) {
				flush();
			}
		} catch (RuntimeException ex) {
			throw ex;
		}
	}

	private void flush() {
		BitSet flush = null;
		Map<Integer, Map<String, String>> esIdMapping = Maps.newHashMap();
		Map<Integer, String> unrecoverableError = Maps.newHashMap();

		try {
			BulkResponse response = writer.repository.tryFlush();
			flush = response.getLeftovers();
			esIdMapping = response.getEsIdMapping();
			unrecoverableError = response.getUnrecoverableError();
		} catch (EsHadoopException ex) {
			// fail all recorded tuples
			for (Tuple input : inflightTuples) {
				collector.fail(input);
			}
			inflightTuples.clear();
			throw ex;
		}

		for (int index = 0; index < inflightTuples.size(); index++) {
			Tuple tuple = inflightTuples.get(index);
			// bit set means the entry hasn't been removed and thus wasn't
			// written to ES
			if (flush.get(index) || !esIdMapping.containsKey(index)
					|| unrecoverableError.containsKey(index)) {
				if (unrecoverableError.containsKey(index)) {
					log.error(String.format("index error. doc:%s",
							new Gson().toJson(tuple.getValue(0))));
				}
				collector.fail(tuple);
			} else {
				if (emitTuples) {
					Map<String, String> info = esIdMapping.get(index);
					Map<String, Object> doc = (Map<String, Object>) tuple
							.getValue(0);
					doc.put("snc_month_index", info.get("_index"));
					doc.put("snc_month_id", info.get("_id"));
					collector.emit(tuple, new Values(doc));
				}
				collector.ack(tuple);
			}
		}

		// clear everything in bulk to prevent 'noisy' remove()
		inflightTuples.clear();
	}

	public void cleanup() {
		if (writer != null) {
			try {
				flush();
			} finally {
				writer.close();
				writer = null;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (emitTuples) {
			declarer.declare(new Fields("source"));
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
