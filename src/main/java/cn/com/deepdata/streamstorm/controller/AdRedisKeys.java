package cn.com.deepdata.streamstorm.controller;

public class AdRedisKeys extends RedisKeys {

	public static final String adVersionKey = "AD_V";
	public static final String adTokenSetKey = "AD_%v%_TS";

	public AdRedisKeys() {
		versionKey = adVersionKey;
		tokensKey = adTokenSetKey;
	}
}
