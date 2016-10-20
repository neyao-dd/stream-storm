package cn.com.deepdata.streamstorm.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yukh on 2016/10/18
 */
public class RiskInfo {
    public Map<String, Map<String, Integer>> matchs;

    public RiskInfo() {
        matchs = new HashMap<>();
    }

    public String toString() {
        return matchs.toString();
    }
}
