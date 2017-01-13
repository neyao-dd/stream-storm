package cn.com.deepdata.streamstorm.spout;

import cn.com.deepdata.streamstorm.bolt.DeepRichBoltHelper;
import cn.com.deepdata.streamstorm.util.RESTUtil;
import cn.com.deepdata.streamstorm.util.TypeProvider;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by hdd on 1/13/17.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class ESSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    final String host;
    final String target;
    final String query;
    List<Map<String, Object>> hits = Lists.newArrayList();
    Integer index = 0;

    public ESSpout(String host, String target, String query) {
        this.host = host;
        this.target = target;
        this.query = query;
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        String strRes = RESTUtil.getRequest("http://" + host + "/" + target + "/_search" + query);
        Map<String, Object> result = new Gson().fromJson(strRes, TypeProvider.type_mso);
        result = (Map<String, Object>) result.get("hits");
        hits = (List<Map<String, Object>>) result.get("hits");
        index = 0;
    }

    public void nextTuple() {
        if (hits.size() > index) {
            _collector.emit(new Values(hits.get(index), "", Maps.newHashMap()));
            index++;
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

}
