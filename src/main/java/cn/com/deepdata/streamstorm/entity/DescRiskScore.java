package cn.com.deepdata.streamstorm.entity;

import com.google.gson.Gson;

public class DescRiskScore implements Entity {
	private int ina_id;
	private double dna_score;
	private double dna_score_v2;
	private int ina_client_id;
	private double dna_client_score;
	private String snc_uuid;

	public int getIna_id() {
		return ina_id;
	}

	public void setIna_id(int id) {
		this.ina_id = id;
	}

	public double getDna_score() {
		return dna_score;
	}

	public void setDna_score(double score) {
		this.dna_score = score;
	}

	public int getIna_client_id() {
		return ina_client_id;
	}

	public void setIna_client_id(int ina_client_id) {
		this.ina_client_id = ina_client_id;
	}

	public double getDna_client_score() {
		return dna_client_score;
	}

	public void setDna_client_score(double dna_client_score) {
		this.dna_client_score = dna_client_score;
	}

	public double getDna_score_v2() {
		return dna_score_v2;
	}

	public void setDna_score_v2(double dna_score_v2) {
		this.dna_score_v2 = dna_score_v2;
	}

	public String getSnc_uuid() {
		return snc_uuid;
	}

	public void setSnc_uuid(String snc_uuid) {
		this.snc_uuid = snc_uuid;
	}

	public String toString() {
		return new Gson().toJson(this);
	}
}
