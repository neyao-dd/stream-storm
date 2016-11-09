package cn.com.deepdata.streamstorm.entity;

import java.io.Serializable;

public class ClientScore implements Serializable {
	private int ina_id;
	private double dna_score;
	private double dna_risk_score;
	private double dna_risk_score_v2;
	private String snc_uuid;

	public int getIna_id() {
		return ina_id;
	}

	public void setIna_id(int ina_id) {
		this.ina_id = ina_id;
	}

	public double getDna_score() {
		return dna_score;
	}

	public void setDna_score(double dna_score) {
		this.dna_score = dna_score;
	}

	public double getDna_risk_score() {
		return dna_risk_score;
	}

	public void setDna_risk_score(double dna_risk_score) {
		this.dna_risk_score = dna_risk_score;
	}

	public String getSnc_uuid() {
		return snc_uuid;
	}

	public void setSnc_uuid(String snc_uuid) {
		this.snc_uuid = snc_uuid;
	}

	public double getDna_risk_score_v2() {
		return dna_risk_score_v2;
	}

	public void setDna_risk_score_v2(double dna_risk_score_v2) {
		this.dna_risk_score_v2 = dna_risk_score_v2;
	}
}
