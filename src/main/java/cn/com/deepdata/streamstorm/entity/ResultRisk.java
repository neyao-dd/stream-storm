package cn.com.deepdata.streamstorm.entity;

import java.util.ArrayList;

public class ResultRisk implements Entity {
	public int id;
	public int clientId;
	public String uuid;
	public String name;
	public ArrayList<String> object;
	public ArrayList<String> behavior;
	public ArrayList<String> location;
	public ArrayList<String> risk;
	public ArrayList<String> rerisk;
	public double riskScore;
	public double riskScore_v2;
	public double clientScore;
	public double weight;
	public double riskWeight;

	@Override
	public String toString() {
		return "id " + this.id + " clientId " + this.clientId + " name " + this.name + " riskScore " + this.riskScore + " clientScore" + this.clientScore;
	}
}
