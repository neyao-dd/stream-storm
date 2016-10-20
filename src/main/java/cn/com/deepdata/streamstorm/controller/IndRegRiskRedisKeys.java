package cn.com.deepdata.streamstorm.controller;

import cn.com.deepdata.streamstorm.entity.RiskFields;

public class IndRegRiskRedisKeys extends RedisKeys {
	private IndRegRiskRedisKeys() {
		versionKey = RiskFields.indRegRiskInfoVersionKey;
		tokensKey = RiskFields.indRegRiskTokenSetKey;
	}

	private static class InnerInstance {
		private static IndRegRiskRedisKeys instance = new IndRegRiskRedisKeys();
	}

	public static IndRegRiskRedisKeys getInstance() {
		return InnerInstance.instance;
	}
}
