package cn.com.deepdata.streamstorm.controller;

public class ClientRedisKeys extends RedisKeys {

	public static final String clientInfoVersionKey = "CI_V";
	public static final String clientTokenItemPrefixKey = "CI_%v%_TI_";
	public static final String clientTokenSetKey = "CI_%v%_NPTS";
	public static final String clientInfoSubVersionKey = "CI_%v%_SV";
	public static final String clientAddedTokenSetKey = "CI_%v%_ATS";
	public static final String clientDeletedTokenSetKey = "CI_%v%_DTS";

	public static final String brandScoreKey = "CI_BrandScore";
	public static final String productScoreKey = "CI_ProductsScore";
	public static final String peopleScoreKey = "CI_PeopleScore";
	public static final String otherScoreKey = "CI_OtherScore";
	public static final String brandScore2Key = "CI_BrandScore2";
	public static final String productScore2Key = "CI_ProductsScore2";
	public static final String peopleScore2Key = "CI_PeopleScore2";
	public static final String otherScore2Key = "CI_OtherScore2";

	public ClientRedisKeys() {
		versionKey = clientInfoVersionKey;
		tokensKey = clientTokenSetKey;
		subVerKey = clientInfoSubVersionKey;
		addedTermsKey = clientAddedTokenSetKey;
		deletedTermsKey = clientDeletedTokenSetKey;
	}
}
