package cn.com.deepdata.streamstorm.entity;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

public class InnerRiskValue {
	public List<ClientScore> clientScore;
	public List<DescRiskScore> riskScore;
	public List<Tag> tag;
	public String clientDebugInfo;
	public String clientDebugInfo2;
	public String riskDebugInfo;
	public Double totalRiskScore;
	public Double maxRiskScore;
	public String adWords;

	public InnerRiskValue() {
		clientScore = new ArrayList<>();
		riskScore = new ArrayList<>();
		tag = new ArrayList<>();
		clientDebugInfo = "";
		clientDebugInfo2 = "";
		riskDebugInfo = "";
		totalRiskScore = 0.;
		maxRiskScore = 0.;
		adWords = "";
	}

	public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, IntrospectionException {
		InnerRiskValue value = new InnerRiskValue();
		ClientScore score = new ClientScore();
		score.setDna_risk_score(33.);
		score.setDna_score(11.);
		value.clientScore = Lists.newArrayList(score);
		System.out.println(new Gson().toJson(Entity.getMap(value)));
	}
}
