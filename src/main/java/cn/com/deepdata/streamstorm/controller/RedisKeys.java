package cn.com.deepdata.streamstorm.controller;

public abstract class RedisKeys {
	public enum Type {
		kClient, kNetRisk, kIndRegRisk, kAd
	}

	public String versionKey = null;
	public String tokensKey = null;
	public String subVerKey = null;
	public String addedTermsKey = null;
	public String deletedTermsKey = null;

	public static RedisKeys CreateRedisKey(Type type) {
		switch (type) {
		case kClient:
			return ClientRedisKeys.getInstance();
		case kNetRisk:
			return NetRiskRedisKeys.getInstance();
		case kIndRegRisk:
			return IndRegRiskRedisKeys.getInstance();
		case kAd:
			return AdRedisKeys.getInstance();
		default:
			return null;
		}
	}

}
