package cn.com.deepdata.streamstorm.controller;

import cn.com.deepdata.streamstorm.entity.RiskFields;

public class NetRiskRedisKeys extends RedisKeys {

	private NetRiskRedisKeys() {
		versionKey = RiskFields.riskInfoVersionKey;
		tokensKey = RiskFields.riskTokenSetKey;
		subVerKey = RiskFields.riskInfoSubVersionKey;
		addedTermsKey = RiskFields.riskAddedTokenSetKey;
		deletedTermsKey = RiskFields.riskDeletedTokenSetKey;
	}

	private static class InnerInstance {
		private static NetRiskRedisKeys instance = new NetRiskRedisKeys();
	}

	public static NetRiskRedisKeys getInstance() {
		return InnerInstance.instance;
	}
}
