package cn.com.deepdata.stormTest;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.storm.cfg.StormSettings;

import cn.com.deepdata.esstorm.BulkResponse;
import cn.com.deepdata.esstorm.PartitionWriter;

public class IndexTest {
	private transient static Log log = LogFactory.getLog(IndexTest.class);;
	static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		LinkedHashMap copy = new LinkedHashMap();
		copy.put(ES_RESOURCE_WRITE, "storm-test/flumetype");
		// copy.put("es.nodes.discovery", "false");
		copy.put("es.nodes.wan.only", "true");
		// copy.put("es.nodes.data.only", "false");
		copy.put("es.nodes", "119.254.86.82");
		copy.put("es.port", "19200");
		copy.put("es.ser.writer.value.class", JdkValueWriter.class.getName());
		StormSettings settings = new StormSettings(copy);
		PartitionWriter writer = PartitionWriter.createWriter(settings, 0, 1,
				log);

		Map<String, Object> doc = Maps.newHashMap();
		doc.put("scc_index", "storm-test");
		doc.put("scc_content", "abc");
		doc.put("tfc_time", format.format(System.currentTimeMillis()));
		writer.repository.writeToIndex(doc);
		// doc.put("scc_content", "bcd");
		// doc.put("tfc_time", format.format(System.currentTimeMillis()));
		// writer.repository.writeToIndex(doc);
		BulkResponse res = writer.repository.tryFlush();
		Map<Integer, Map<String, String>> esIdMapping = res.getEsIdMapping();
		System.out.println(esIdMapping.containsKey(0));

		// Type mapType = new TypeToken<Map<String, Object>>() {
		// }.getType();
		writer.close();
	}
}
