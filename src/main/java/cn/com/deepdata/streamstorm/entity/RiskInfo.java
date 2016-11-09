package cn.com.deepdata.streamstorm.entity;

import com.google.gson.Gson;

import java.util.ArrayList;

/**
 * Created by yukh on 2016/10/18
 */
public class RiskInfo implements Entity {
    public String name;
    public int level;
    public String attribute;
    public ArrayList<String> object;
    public ArrayList<String> behavior;
    public ArrayList<String> location;
    public ArrayList<String> risk;
    public ArrayList<String> rerisk;
    public double weight;
    public int p0;
    public int p1;
    public int p2;

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
