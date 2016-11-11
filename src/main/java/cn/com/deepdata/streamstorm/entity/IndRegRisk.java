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
    public List<Industry> industryRisk;
    public String regionDebugInfo;
    public String industryDebugInfo;
    public List<Region> regionList;

    public IndRegRisk() {
        regionRisk = 0;
        industryRisk = new ArrayList<>();
        regionDebugInfo = "";
        industryDebugInfo = "";
        regionList = new ArrayList<>();
    }

    public void addMaxRegionRisk(IndRegRisk other) {
        this.regionRisk = Math.max(this.regionRisk, other.regionRisk);
    }
}