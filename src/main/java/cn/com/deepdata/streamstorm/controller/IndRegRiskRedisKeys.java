package cn.com.deepdata.streamstorm.controller;

public class IndRegRiskRedisKeys extends RedisKeys {
	static public final String indRegRiskVersionsListKey = "IRRI_VL";
	static public final String indRegRiskInfoVersionKey = "IRRI_V";
	static public final String regRiskItemPrefixKey = "RRI_%v%_I_";
	static public final String indRiskItemPrefixKey = "IRI_%v%_I_";
	static public final String indRegRiskTokenItemPrefixKey = "IRRI_%v%_TI_";
	static public final String indRegRiskTokenSetKey = "IRRI_%v%_TS";

	public IndRegRiskRedisKeys() {
		versionKey = indRegRiskInfoVersionKey;
		tokensKey = indRegRiskTokenSetKey;
	}
}
