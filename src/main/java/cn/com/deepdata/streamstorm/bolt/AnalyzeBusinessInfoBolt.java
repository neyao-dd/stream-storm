package cn.com.deepdata.streamstorm.bolt;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;
import cn.com.deepdata.streamstorm.entity.ChangeRecord;
import cn.com.deepdata.streamstorm.entity.ChangedRisk;
import cn.com.deepdata.streamstorm.entity.Company;
import cn.com.deepdata.streamstorm.util.CommonUtil;
import com.google.gson.Gson;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.*;

/**
 * Created by yukh on 2016/10/31
 */
public class AnalyzeBusinessInfoBolt extends AbstractRedisBolt {
    private  static final Logger logger = LoggerFactory.getLogger(BaseRichBolt.class);
    private static Map<String, Company> companyInfo;
    // 可以存到redis中
    private String s = "ע���ʱ�(��)���,�����±���,Ͷ���ܶ���,�Ǽǻ��ر��,��ҵ���ͱ��,��Ӫ��Χ���,Ͷ����(��Ȩ)���,��Ӫ����(Ӫҵ����)���,ס�����";
    private String s2 = "1,2,3,07,2014年3.8活动,郭武强,-";
    private Set<String> uselessItem = new HashSet<>();

    public AnalyzeBusinessInfoBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        uselessItem.addAll(Arrays.asList(s.split(",")));
        uselessItem.addAll(Arrays.asList(s2.split(",")));
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    // TODO: 2016/11/1
    public void init() {
        Gson gson = new Gson();
        companyInfo = new HashMap<>();
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            // TODO: 2016/11/1 name
            Set<String> riskLevel = jedisCommands.smembers("business_change_risk_level");
            for (String rl : riskLevel) {
                Company company = gson.fromJson(rl, Company.class);
                companyInfo.put(company.name, company);
            }
        } catch (Exception e) {
            logger.error("companyInfo init() error..." + CommonUtil.getExceptionString(e));
        } finally {
            if (jedisCommands != null)
                returnInstance(jedisCommands);
        }
    }

    public ChangedRisk analyze(ChangeRecord record) {
        int level = 0;
        String date = record.tfc_change_date;
        String item = record.scc_change_item;
        String type = "";
        try {
            if (uselessItem.contains(item)) {
                return new ChangedRisk(level, 0, date, type);
            }
            // TODO: 2016/11/1
//            try {
                item = item.trim();
//            } catch (Exception e) {
//                logger.error("trim() error...");
//                return new ChangedRisk(level, 0, date, type);
//            }

            if (companyInfo.containsKey(item)) {
                try {
                    if ((level = companyInfo.get(item).level) == 0) {
                        double before = Double.parseDouble(record.scc_before_content);
                        double after = Double.parseDouble(record.scc_after_content);
                        level = after > before ? 3 : 5;
                    }
                } catch (Exception e) {
                    level = 4;
                }
                type = companyInfo.get(item).type;
            } else {
                String subItem = item.replace("变更", "");
                if (Company.allSubSen.containsKey(subItem)) {
                    String[] lt = Company.allSubSen.get(subItem).split(" ");
                    level = Integer.parseInt(lt[0]);
                    type = lt[1];
                } else {
                    List<String> tokens = AnsjTermAnalyzer.simpleTokens(subItem);
                    boolean newWord = false;
                    for (String token : tokens) {
                        if (!Company.allTokens.contains(token)) {
                            logger.info("the new item is " + item);
                            // TODO: 2016/11/1 return
                            newWord = true;
                            break;
                        }
                    }
                    if (!newWord) {
                        for (String it : companyInfo.keySet()) {
                            int match = 0;
                            for (String token : tokens) {
                                if (companyInfo.get(it).tokens.contains(token)) {
                                    match++;
                                }
                            }
                            if (match > 0 && tokens.size() - match <= 1) {
                                level = companyInfo.get(it).level;
                                type = companyInfo.get(it).type;
                            }
                        }
                    }
                    if (level == 0 && !newWord) {
                        logger.info("level error, item is " + item);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("company analyze error..." + CommonUtil.getExceptionString(e));
            logger.error("error map is" + record.toString());
        }
        ChangedRisk cr;
        try {
            cr = new ChangedRisk(level, record.inp_seq_no, date, type);
        } catch (Exception e) {
            cr = new ChangedRisk(level, 0, date, type);
        }
        return cr;
    }

    public void writeEs() {

    }

    public void writeEsBusinessChange() {

    }

    public void deleteOldDoc() {

    }

    public void calcId() {

    }
}
