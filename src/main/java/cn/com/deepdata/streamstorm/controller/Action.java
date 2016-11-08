package cn.com.deepdata.streamstorm.controller;

import java.io.Serializable;

public class Action implements Serializable {
	public String name;
	public EDeDupType deDupType;
	public EAnalyzeType analyzeType;
	public String indexName;
	public EIndexType indexType;

	public Action(String name, EDeDupType deDupType, EAnalyzeType analyzeType, String indexName, EIndexType indexType) {
		this.name = name;
		this.deDupType = deDupType;
		this.analyzeType = analyzeType;
		this.indexName = indexName;
		this.indexType = indexType;
	}

	public Action(String name, String deDupType, String analyzeType, String indexName, String indexType) {
		this.name = name;
		this.deDupType = EDeDupType.valueOf(deDupType);
		this.analyzeType = EAnalyzeType.valueOf(analyzeType);
		this.indexName = indexName;
		this.indexType = EIndexType.valueOf(indexType);
	}
}
