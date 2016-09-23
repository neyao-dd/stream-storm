package cn.com.deepdata.streamstorm.controller;

public class NetRiskRedisKeys extends RedisKeys {

	public static final String riskInfoVersionKey = "RI_V";
	public static final String riskItemPrefixKey = "RI_%v%_I_";
	public static final String riskTokenItemPrefixKey = "RI_%v%_TI_";
	public static final String riskTokenSetKey = "RI_%v%_TS";
	public static final String riskInfoSubVersionKey = "RI_%v%_SV";
	public static final String riskAddedTokenSetKey = "RI_%v%_ATS";
	public static final String riskDeletedTokenSetKey = "RI_%v%_DTS";

	public NetRiskRedisKeys() {
		versionKey = riskInfoVersionKey;
		tokensKey = riskTokenSetKey;
		subVerKey = riskInfoSubVersionKey;
		addedTermsKey = riskAddedTokenSetKey;
		deletedTermsKey = riskDeletedTokenSetKey;
	}
}
