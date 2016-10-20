package cn.com.deepdata.streamstorm.controller;

import cn.com.deepdata.streamstorm.entity.RiskFields;

public class AdRedisKeys extends RedisKeys {

	private AdRedisKeys() {
		versionKey = RiskFields.adVersionKey;
		tokensKey = RiskFields.adTokenSetKey;
	}

	private static class InnerInstance {
		private static AdRedisKeys instance = new AdRedisKeys();
	}

	public static AdRedisKeys getInstance() {
		return InnerInstance.instance;
	}
}
