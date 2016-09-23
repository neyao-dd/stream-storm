package cn.com.deepdata.streamstorm.controller;

public class RedisKeys {
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
			return new ClientRedisKeys();
		case kNetRisk:
			return new NetRiskRedisKeys();
		case kIndRegRisk:
			return new IndRegRiskRedisKeys();
		case kAd:
			return new AdRedisKeys();
		default:
			return null;
		}
	}
}
