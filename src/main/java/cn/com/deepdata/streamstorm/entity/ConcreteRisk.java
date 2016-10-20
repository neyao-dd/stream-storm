package cn.com.deepdata.streamstorm.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConcreteRisk {
	
	private int id;
	private int clientId;
	private String name;
	private String uuid;
	private Map<String, Integer> object;
	private Map<String, Integer> behavior;
	private Map<String, Integer> location;
	private Map<String, Integer> risk;
	private Map<String, Integer> rerisk;
	private Map<String, Integer> wordpos;
	private double clientScore;
	private double score;
	private double score_v2;
	private double weight;
	private double riskWeight;

	public ConcreteRisk() {
		object = new HashMap<>();
		behavior = new HashMap<>();
		location = new HashMap<>();
		risk = new HashMap<>();
		rerisk = new HashMap<>();
		wordpos = new HashMap<>();
		name = "";
	}

	public Map<String, Integer> getBehavior() {
		return behavior;
	}

	public void setBehavior(Map<String, Integer> behavior) {
		this.behavior = behavior;
	}

	public void addBehavior(String key, int value) {
//		if (behavior.containsKey(key)) {
//			behavior.put(key, behavior.get(key) + count);
//		} else {
//			behavior.put(key, count);
//		}
		behavior.put(key, value);
	}

	public Map<String, Integer> getLocation() {
		return location;
	}

	public void setLocation(Map<String, Integer> location) {
		this.location = location;
	}

	public void addLocation(String key, int value) {
//		if (location.containsKey(key)) {
//			location.put(key, location.get(key) + count);
//		} else {
//			location.put(key, count);
//		}
		location.put(key, value);
	}

	public Map<String, Integer> getRisk() {
		return risk;
	}

	public void setRisk(Map<String, Integer> risk) {
		this.risk = risk;
	}

	public void addRisk(String key, int value) {
//		if (risk.containsKey(key)) {
//			risk.put(key, risk.get(key) + count);
//		} else {
//			risk.put(key, count);
//		}
		risk.put(key, value);
	}

	public Map<String, Integer> getObject() {
		return object;
	}

	public void setObject(Map<String, Integer> object) {
		this.object = object;
	}
	
	public void setObject(String word, int count) {
		this.object.clear();
		this.object.put(word, count);
	}

	public void addObject(String key, int value) {
		object.put(key, value);
//		if (object.containsKey(key)) {
//			object.put(key, object.get(key) + 1);
//		} else {
//			object.put(key, 1);
//		}
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	/**
	 * 转换格式
	 */
	public ResultRisk parseRR() {
		ResultRisk rr = new ResultRisk();
		rr.id = this.getId();
		rr.clientId = this.getClientId();
		rr.name = this.getName();
		rr.riskScore = this.getScore();
		rr.riskScore_v2 = this.getScore_v2();
		rr.clientScore = this.getClientScore();
		rr.weight = this.getWeight();
		rr.uuid = this.getUuid();
		rr.riskWeight = this.getRiskWeight();
		rr.object = parseList(this.getObject());
		rr.behavior = parseList(this.getBehavior());
		rr.location = parseList(this.getLocation());
		rr.risk = parseList(this.getRisk());
		rr.rerisk = parseList(this.getRerisk());
		return rr;
	}

	public ArrayList<String> parseList(Map<String, Integer> map) {
		ArrayList<String> list = new ArrayList<>();
		for (String key : map.keySet()) {
			try {
				list.add(key + " " + map.get(key) + " " + getWordpos(key));
			} catch(NullPointerException e) {
				String[] words = key.split("\\*");
				String ps = getWordpos(words[0]) + "," + getWordpos(words[1]);
				list.add(key + " " + map.get(key) + " " + ps);
			}
//			list.add(key + " " + map.get(key));
		}
		return list;
	}

	public int getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public double getClientScore() {
		return clientScore;
	}

	public void setClientScore(double clientScore) {
		this.clientScore = clientScore;
	}
	
	public Map<String, Integer> getRerisk() {
		return rerisk;
	}

	public void setRerisk(Map<String, Integer> rerisk) {
		this.rerisk = rerisk;
	}
	
	public static void main(String[] args) {
		System.out.println(Math.log(3-(120.0/70))/Math.log(3));
		String s = "作为一家第三方B2B电子商务服务商，焦点科技，7月22日公布的半年报显示扣除非经常性损益后净利润";
		System.out.println(s.length());
		System.out.println(Math.log(-32));
	}

	public void addRerisk(String key, int value) {
		rerisk.put(key, value);
	}

	public Map<String, Integer> getWordpos() {
		return wordpos;
	}

	public int getWordpos(String word) {
		return wordpos.get(word);
	}
	
	public void addWordpos(String word, int pos) {
		wordpos.put(word, pos);
	}

	public double getScore_v2() {
		return score_v2;
	}

	public void setScore_v2(double score_v2) {
		this.score_v2 = score_v2;
	}

	public double getRiskWeight() {
		return riskWeight;
	}

	public void setRiskWeight(double riskWeight) {
		this.riskWeight = riskWeight;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
}
