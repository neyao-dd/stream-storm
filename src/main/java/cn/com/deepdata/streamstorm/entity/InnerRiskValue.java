package cn.com.deepdata.streamstorm.entity;


import java.util.ArrayList;
import java.util.List;

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
}
