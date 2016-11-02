package cn.com.deepdata.streamstorm.controller;

import java.io.Serializable;

public class Action implements Serializable{
	public String name;
	public boolean deDup;
	public String deDupMode;
	public EAnalyzeType analyzeType;
	public String indexName;
	public EIndexType indexType;

	public Action(String name, boolean deDup, String deDupMode,
			EAnalyzeType analyzeType, String indexName, EIndexType indexType) {
		this.name = name;
		this.deDup = deDup;
		this.deDupMode = deDupMode;
		this.analyzeType = analyzeType;
		this.indexName = indexName;
		this.indexType = indexType;
	}

	public Action(String name, boolean deDup, String deDupMode,
			String analyzeType, String indexName, String indexType) {
		this.name = name;
		this.deDup = deDup;
		this.deDupMode = deDupMode;
		this.analyzeType = EAnalyzeType.valueOf(analyzeType);
		this.indexName = indexName;
		this.indexType = EIndexType.valueOf(indexType);
	}
}
