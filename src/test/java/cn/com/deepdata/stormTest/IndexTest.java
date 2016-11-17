package cn.com.deepdata.stormTest;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.elasticsearch.hadoop.serialization.JdkBytesConverter;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.field.AbstractDefaultParamsExtractor;
import org.elasticsearch.hadoop.serialization.field.DefaultIndexExtractor;
import org.elasticsearch.storm.cfg.StormSettings;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import cn.com.deepdata.esstorm.BulkResponse;
import cn.com.deepdata.esstorm.PartitionWriter;
import cn.com.deepdata.streamstorm.entity.DescRiskScore;
import cn.com.deepdata.streamstorm.entity.Entity;

public class IndexTest {
	private transient static Log log = LogFactory.getLog(IndexTest.class);;
	static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		LinkedHashMap copy = new LinkedHashMap();
		copy.put(ES_RESOURCE_WRITE, "storm-test/{snp_type}");
		// copy.put("es.nodes.discovery", "false");
		copy.put("es.nodes.wan.only", "true");
		// copy.put("es.nodes.data.only", "false");
		copy.put("es.nodes", "119.254.86.82");
		copy.put("es.port", "19200");
		copy.put("es.input.json", "true");
		copy.put("es.ser.writer.value.class", JdkValueWriter.class.getName());
		copy.put("es.ser.writer.bytes.class", JdkBytesConverter.class.getName());
		copy.put("es.mapping.default.extractor.class", DefaultIndexExtractor.class.getName());
		StormSettings settings = new StormSettings(copy);
		PartitionWriter writer = PartitionWriter.createWriter(settings, 0, 1, log);

		Map<String, Object> doc = Maps.newHashMap();
		doc.put("scc_index", "storm-test");
		doc.put("scc_content", "2016-11-15 金馆长表情包 金馆长表情包 金馆长表情包 微信号 ghz366 功能介绍 ✅金馆长表情包支持金馆长表情diy在线制作。🔥提供最新逗逼表情大全下载，🔥动态表情、🔥表情包，🔥表情大全，🔥表情仓库🔥 馆长温馨提示 阅读 投诉 阅读\n\n\n");
		doc.put("snp_type", "flumetype");
		DescRiskScore score = new DescRiskScore();
		score.setDna_client_score(2.2);
		score.setDna_score(3.3);
		score.setDna_score_v2(3.5);
		score.setIna_client_id(33);
		score.setIna_id(100);
		score.setSnc_uuid("abcd");
		List<DescRiskScore> riskScore = Lists.newArrayList(score);
		try {
			doc.put("nna_risks", Entity.getMap(riskScore));
		} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException | IntrospectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		doc.put("nnp_abnormal_items", Lists.newArrayList());
		doc.put("tfc_time", format.format(System.currentTimeMillis()));
		System.out.println(new Gson().toJson(doc));
		writer.repository.writeToIndex(new Gson().toJson(doc));
		BulkResponse res = writer.repository.tryFlush();
		Map<Integer, Map<String, String>> esIdMapping = res.getEsIdMapping();
		System.out.println(esIdMapping.get(0));

		// Type mapType = new TypeToken<Map<String, Object>>() {
		// }.getType();
		writer.close();
	}
}
