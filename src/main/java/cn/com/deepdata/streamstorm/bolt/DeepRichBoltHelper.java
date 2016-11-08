package cn.com.deepdata.streamstorm.bolt;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;
import static cn.com.deepdata.streamstorm.util.CommonUtil.*;

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

	public String getDocTitle(Tuple input) {
		Map<String, Object> doc = getDoc(input);
		String scc_title = getValue(doc, "scc_title");
		String scm_title = getValue(doc, "scm_title");
		return validString(scc_title) ? scc_title : (validString(scm_title) ? scm_title : "");
		// TODO: 2016/11/8 for test to delete
//		return getDoc(input).get("scc_title").toString();
	}

	private String getValue(Map<String, Object> map, String key) {
		if (map.containsKey(key) && null != map.get(key))
			return map.get(key).toString();
		return null;
	}

	public String getDocContent(Tuple input) {
		Map<String, Object> doc = getDoc(input);
		String scc_content = getValue(doc, "scc_content");
		return validString(scc_content) ? scc_content : "";
		// TODO: 2016/11/8 for test to delete
//		return getDoc(input).get("scc_content").toString();
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
			String action, Map<String, Object> attach, boolean anchor,
			String stream) {
		if (doc == null)
			doc = getDoc(input);
		if (action == null)
			action = getAction(input);
		if (attach == null)
			attach = getAttach(input);
		if (anchor && stream != null) {
			return _collector.emit(stream, input, new Values(doc, action,
					attach));
		} else if (anchor) {
			return _collector.emit(input, new Values(doc, action, attach));
		} else if (stream != null) {
			return _collector.emit(stream, new Values(doc, action, attach));
		} else {
			return _collector.emit(new Values(doc, action, attach));
		}
	}

	public List<Integer> emit(Tuple input, Map<String, Object> doc,
			String action, Map<String, Object> attach, boolean anchor) {
		return emit(input, doc, action, attach, anchor, null);
	}

	public List<Integer> emit(Tuple input, boolean anchor, String stream) {
		// TODO Auto-generated method stub
		return emit(input, null, null, null, anchor, stream);
	}

	public List<Integer> emit(Tuple input, boolean anchor) {
		// TODO Auto-generated method stub
		return emit(input, null, null, null, anchor, null);
	}

	public List<Integer> emitDoc(Tuple input, Map<String, Object> doc,
			boolean anchor) {
		return emit(input, doc, null, null, anchor, null);
	}

	public List<Integer> emitAttach(Tuple input, Map<String, Object> attach,
			boolean anchor) {
		return emit(input, null, null, attach, anchor, null);
	}

	public void ack(Tuple input) {
		_collector.ack(input);
	}

	public void fail(Tuple input) {
		_collector.fail(input);
	}
}
