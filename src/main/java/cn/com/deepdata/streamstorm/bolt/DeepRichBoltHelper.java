package cn.com.deepdata.streamstorm.bolt;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class DeepRichBoltHelper {
	// doc: elasticsearch document(Map<String, Object>)
	// action: action type(String)
	// attach: attach info(Map<String, Object>)
	public static final String[] fields = { "source", "action", "attach" };
	protected OutputCollector _collector;

	public DeepRichBoltHelper(OutputCollector collector) {
		_collector = collector;
	}

	// TODO: 2016/10/26  
	public Map<String, Object> getDoc(Tuple input) {
		if (!input.contains(fields[0])) {
			if (input.contains("doc"))
				return (Map<String, Object>) input.getValueByField("doc");
			return Maps.newHashMap();
		}
		Map<String, Object> doc = (Map<String, Object>) input.getValue(0);
		if (doc.containsKey("source")
				&& Map.class.isInstance(doc.get("source")))
			doc = (Map<String, Object>) doc.get("source");
		return doc;
	}

	// TODO: 2016/10/26  
	public String getDocTitle(Tuple input) {
//		Map<String, Object> doc = getDoc(input);
//		return doc.containsKey("scc_title") && doc.get("scc_title") != null
//				&& doc.get("scc_title").toString().length() > 0 ? doc.get(
//				"scc_title").toString() : doc.containsKey("scm_title")
//				&& doc.get("scm_title") != null
//				&& doc.get("scm_title").toString().length() > 0 ? doc.get(
//				"scm_title").toString() : "";
		return getDoc(input).get("scc_title").toString();
	}

	// TODO: 2016/10/26  
	public String getDocContent(Tuple input) {
//		Map<String, Object> doc = getDoc(input);
//		return doc.containsKey("scc_content") && doc.get("scc_content") != null
//				&& doc.get("scc_content").toString().length() > 0 ? doc.get(
//				"scc_content").toString() : "";
		return getDoc(input).get("scc_content").toString();
	}

	public String getAction(Tuple input) {
		if (!input.contains(fields[1]))
			return "";
		return input.getString(1);
	}

	public Map<String, Object> getAttach(Tuple input) {
		if (!input.contains(fields[2]))
			return Maps.newHashMap();
		return (Map<String, Object>) input.getValue(2);
	}

	public List<Integer> emit(Tuple input, Map<String, Object> doc,
			String action, Map<String, Object> attach, boolean anchor) {
		if (doc == null)
			doc = getDoc(input);
		if (action == null)
			action = getAction(input);
		if (attach == null)
			attach = getAttach(input);
		if (anchor)
			return _collector.emit(input, new Values(doc, action, attach));
		else
			return _collector.emit(new Values(doc, action, attach));
	}

	public List<Integer> emit(Tuple input, boolean anchor) {
		// TODO Auto-generated method stub
		return emit(input, null, null, null, anchor);
	}

	public List<Integer> emitDoc(Tuple input, Map<String, Object> doc,
			boolean anchor) {
		return emit(input, doc, null, null, anchor);
	}

	public List<Integer> emitAttach(Tuple input, Map<String, Object> attach,
			boolean anchor) {
		return emit(input, null, null, attach, anchor);
	}

	public void ack(Tuple input) {
		_collector.ack(input);
	}

	public void fail(Tuple input) {
		_collector.fail(input);
	}
}
