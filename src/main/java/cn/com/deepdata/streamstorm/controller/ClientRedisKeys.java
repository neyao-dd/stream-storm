package cn.com.deepdata.streamstorm.controller;

import cn.com.deepdata.streamstorm.entity.RiskFields;

public class ClientRedisKeys extends RedisKeys {

	private ClientRedisKeys() {
		versionKey = RiskFields.clientInfoVersionKey;
		tokensKey = RiskFields.clientTokenSetKey;
		subVerKey = RiskFields.clientInfoSubVersionKey;
		addedTermsKey = RiskFields.clientAddedTokenSetKey;
		deletedTermsKey = RiskFields.clientDeletedTokenSetKey;
	}

	private static class InnerInstance {
		private static ClientRedisKeys instance = new ClientRedisKeys();
	}

	public static ClientRedisKeys getInstance() {
		return InnerInstance.instance;
	}

}
