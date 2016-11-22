package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.streamstorm.util.CommonUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yukh on 2016/11/21
 */
public class CourtSessionBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(CourtSessionBolt.class);
    DeepRichBoltHelper helper;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        helper = new DeepRichBoltHelper(outputCollector);
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> source = helper.getDoc(tuple);
        source.put("nna_risks", analyze(source));
        helper.emitDoc(tuple, source, true);
        helper.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(DeepRichBoltHelper.fields));
    }

    private List<Map<String, Object>> analyze(Map<String, Object> source) {
        List<Map<String, Object>> risks = new ArrayList<>();
        Map<String, Object> risk = new HashMap<>();
        List<String> plaintiffPerson = new ArrayList<>();
        List<String> plaintiffCompany = new ArrayList<>();
        List<String> defendantPerson = new ArrayList<>();
        List<String> defendantCompany = new ArrayList<>();
        String plaintiff = source.get("scc_plaintiff").toString();
        String defendant = source.get("scc_defendant").toString();
        if (plaintiff == null || defendant == null)
            return risks;
        classify(plaintiff, plaintiffCompany, plaintiffPerson);
        classify(defendant, defendantCompany, defendantPerson);
        risk.put("sca_plaintiff_person", plaintiffPerson);
        risk.put("sca_plaintiff_company", plaintiffCompany);
        risk.put("sca_defendant_person", defendantPerson);
        risk.put("sca_defendant_company", defendantCompany);
        risks.add(risk);
        return risks;
    }

    private void classify(String info, List<String> company, List<String> person) {
        if (CommonUtil.validString(info)) {
            String[] entities = info.split(",|，");
            for (String entity : entities) {
                if (entity.length() > 3)
                    company.add(entity);
                else
                    person.add(entity);
            }
        }
    }

}
