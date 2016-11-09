package cn.com.deepdata.streamstorm.entity;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by yukh on 2016/10/18
 */
public class RegionRiskInfo implements Serializable {
    public int id;
    public String name;
    public int weight;
    public ArrayList<String> object;
    public ArrayList<String> risk;

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static RegionRiskInfo fromJson(String strJson) {
        Gson gson = new Gson();
        return gson.fromJson(strJson, RegionRiskInfo.class);
    }
}
