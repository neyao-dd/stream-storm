package cn.com.deepdata.streamstorm.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings({ "rawtypes", "serial", "unchecked" })
public class CreatESDocBolt extends BaseRichBolt {
	private transient static Log log = LogFactory.getLog(CreatESDocBolt.class);
	private transient OutputCollector _collector;
	private transient SimpleDateFormat format;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public boolean validDate(String str) {
		if (str == null)
			return false;

		try {
			Date date = (Date) format.parse(str);
			return str.equals(format.format(date));
		} catch (Exception e) {
			log.error("Not Valid");
			return false;
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Object> doc = (Map<String, Object>) input.getValue(0);

		List<String> removeKey = doc.keySet().stream()
				.filter(k -> k.indexOf("_") == -1).collect(Collectors.toList());
		if (removeKey.size() > 0) {
			removeKey.stream().forEach(k -> doc.remove(k));
		}

		doc.keySet().stream().filter(k -> k.startsWith("sn"))
				.forEach(k -> doc.put(k, doc.get(k).toString().trim()));

		boolean validTfcTime = false;
		if (doc.containsKey("tfc_time")) {
			if (!validDate((String) doc.get("tfc_time"))) {
				log.error("failed to parse [tfc_time], doc:" + doc.toString());
				doc.put("tfc_time", "2000-01-01 08:00:00");
			}
			validTfcTime = !doc.get("tfc_time").equals("2000-01-01 08:00:00");
		}
		boolean validUrlTime = false;
		if (doc.containsKey("tfc_url_time")) {
			if (!validDate((String) doc.get("tfc_url_time"))) {
				log.error("failed to parse [tfc_url_time], header:"
						+ doc.toString());
				doc.put("tfc_url_time", "2000-01-01 08:00:00");
			}
			validUrlTime = !doc.get("tfc_url_time").equals(
					"2000-01-01 08:00:00");
		}
		doc.put("tfp_save_time", format.format(System.currentTimeMillis()));
		String sortTime;
		if (validTfcTime)
			sortTime = (String) doc.get("tfc_time");
		else if (validUrlTime)
			sortTime = (String) doc.get("tfc_url_time");
		else
			sortTime = (String) doc.get("tfp_save_time");
		doc.put("tfp_sort_time", sortTime);

		if (doc.containsKey("lnc_forward_count")) {
			String value = doc.get("lnc_forward_count").toString().trim();
			if (value.length() == 0)
				value = "0";
			doc.put("lnc_forward_count", value);
		}

		_collector.emit(input, new Values(doc));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("source"));
	}
}
