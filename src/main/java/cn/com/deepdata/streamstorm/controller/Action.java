package cn.com.deepdata.streamstorm.controller;

public class Action {
	public String name;
	public boolean deDup;
	public String deDupMode;
	public boolean needAnalyze;
	public String indexName;
	public EIndexType indexType;

	public Action(String name, boolean deDup, String deDupMode,
			boolean needAnalyze, String indexName, EIndexType indexType) {
		this.name = name;
		this.deDup = deDup;
		this.deDupMode = deDupMode;
		this.needAnalyze = needAnalyze;
		this.indexName = indexName;
		this.indexType = indexType;
	}

	public Action(String name, boolean deDup, String deDupMode,
			boolean needAnalyze, String indexName, String indexType) {
		this.name = name;
		this.deDup = deDup;
		this.deDupMode = deDupMode;
		this.needAnalyze = needAnalyze;
		this.indexName = indexName;
		this.indexType = EIndexType.valueOf(indexType);
	}
}
