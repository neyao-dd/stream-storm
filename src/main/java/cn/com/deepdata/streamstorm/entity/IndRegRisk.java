package cn.com.deepdata.streamstorm.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yukh on 2016/10/17
 */
public class IndRegRisk implements Entity {
    public double regionRisk;
    public Map<Integer, Double> industryRisk;
    public String regionDebugInfo;
    public String industryDebugInfo;
    public List<Region> regionList;

    public IndRegRisk() {
        regionRisk = 0;
        industryRisk = new HashMap<>();
        regionDebugInfo = "";
        industryDebugInfo = "";
        regionList = new ArrayList<>();
    }

    public void addMax(IndRegRisk other) {
        this.regionRisk = Math.max(this.regionRisk, other.regionRisk);
        for (Integer id : other.industryRisk.keySet()) {
            if (this.industryRisk.containsKey(id)) {
                this.industryRisk.put(id, Math.max(this.industryRisk.get(id), other.industryRisk.get(id)));
            }
            else
                this.industryRisk.put(id, other.industryRisk.get(id));
        }
    }
}