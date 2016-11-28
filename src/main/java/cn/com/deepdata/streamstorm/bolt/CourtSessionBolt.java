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
        List<String> threePartPerson = new ArrayList<>();
        List<String> threePartCompany = new ArrayList<>();
        String plaintiff = source.get("scc_plaintiff").toString();
        String defendant = source.get("scc_defendant").toString();
        if (CommonUtil.validString(plaintiff)) {
            if (plaintiff.contains("第三人"))
                plaintiff = findThreePart(plaintiff, threePartCompany, threePartPerson);
            classify(plaintiff, plaintiffCompany, plaintiffPerson);
        }
        if (CommonUtil.validString(defendant)) {
            if (defendant.contains("第三人"))
                defendant = findThreePart(defendant, threePartCompany, threePartPerson);
            classify(defendant, defendantCompany, defendantPerson);
        }
        risk.put("sca_plaintiff_person", plaintiffPerson);
        risk.put("sca_plaintiff_company", plaintiffCompany);
        risk.put("sca_defendant_person", defendantPerson);
        risk.put("sca_defendant_company", defendantCompany);
        risk.put("sca_threePart_person", threePartPerson);
        risk.put("sca_threePart_company", threePartCompany);
        risks.add(risk);
        return risks;
    }

    private String findThreePart(String info, List<String> company, List<String> person) {
        // "高卫华;第三人:东莞市大正贸易有限公司"
        try {
            String[] subInfo;
            if (info.contains("原审第三人"))
                subInfo = info.split(";原审第三人:");
            else
                subInfo = info.split(";第三人:");
            info = subInfo[0];
            classify(subInfo[1], company, person);
        } catch (Exception e) {
            logger.error("findThreePart error, info is: {}", info);
        }
        return info;
    }

    private void classify(String info, List<String> company, List<String> person) {
        if (CommonUtil.validString(info)) {
            info = info.trim();
            if (info.endsWith(";") || info.endsWith("；"))
                info = info.substring(0, info.length() - 1);
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
