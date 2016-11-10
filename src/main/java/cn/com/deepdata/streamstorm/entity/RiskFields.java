package cn.com.deepdata.streamstorm.entity;

import java.io.Serializable;

/**
 * Created by yukh on 2016/10/18.
 */
public class RiskFields implements Serializable {
    public static final String indRegRiskVersionsListKey = "IRRI_VL";
    public static final String indRegRiskInfoVersionKey = "IRRI_V";
    public static final String regRiskItemPrefixKey = "RRI_%v%_I_";
    public static final String indRiskItemPrefixKey = "IRI_%v%_I_";
    public static final String indRegRiskTokenItemPrefixKey = "IRRI_%v%_TI_";
    public static final String indRegRiskTokenSetKey = "IRRI_%v%_TS";
    public static final String regionAliasVersionKey = "RA_V";
    public static final String regionAliasTokenSetKey = "RA_%v%_TS";

    public static final String brandScoreKey = "CI_BrandScore";
    public static final String productScoreKey = "CI_ProductsScore";
    public static final String peopleScoreKey = "CI_PeopleScore";
    public static final String otherScoreKey = "CI_OtherScore";
    public static final String brandScore2Key = "CI_BrandScore2";
    public static final String productScore2Key = "CI_ProductsScore2";
    public static final String peopleScore2Key = "CI_PeopleScore2";
    public static final String otherScore2Key = "CI_OtherScore2";

    public static final String clientInfoVersionKey = "CI_V";
    public static final String clientTokenItemPrefixKey = "CI_%v%_TI_";
    public static final String clientTokenSetKey = "CI_%v%_NPTS";
    public static final String clientInfoSubVersionKey = "CI_%v%_SV";
    public static final String clientAddedTokenSetKey = "CI_%v%_ATS";
    public static final String clientDeletedTokenSetKey = "CI_%v%_DTS";

    public static final String riskInfoVersionKey = "RI_V";
    public static final String riskItemPrefixKey = "RI_%v%_I_";
    public static final String riskTokenItemPrefixKey = "RI_%v%_TI_";
    public static final String riskTokenSetKey = "RI_%v%_TS";
    public static final String riskInfoSubVersionKey = "RI_%v%_SV";
    public static final String riskAddedTokenSetKey = "RI_%v%_ATS";
    public static final String riskDeletedTokenSetKey = "RI_%v%_DTS";

    public static final String adVersionKey = "AD_V";
    public static final String adTokenSetKey = "AD_%v%_TS";

    public static final String REGION_LAST_UPDATE_TIME = "sc_region_last_update_time";
    public static final String REGION_TERMS_SET = "sc_region_terms_set";
    public static final String REGION_TERM_INFOS_PREFIX = "sc_region_term_infos_";
    public static final String REGION_ITEM_INFO_PREFIX = "sc_region_item_info_";

    public static final String CLIENT_LAST_UPDATE_TIME = "sc_client_last_update_time";
    public static final String CLIENT_TERMS_SET = "sc_client_terms_set";
    public static final String CLIENT_TERM_INFOS_PREFIX = "sc_client_term_infos_";
    public static final String CLIENT_ITEM_INFO_PREFIX = "sc_client_item_info_";

    public static final String RISK_LAST_UPDATE_TIME = "sc_risk_last_update_time";
    public static final String RISK_TERMS_SET = "sc_risk_terms_set";
    public static final String RISK_TERM_INFOS_PREFIX = "sc_risk_term_infos_";
    public static final String RISK_ITEM_INFO_PREFIX = "sc_risk_item_info_";

    public static final String REGION_RISK_LAST_UPDATE_TIME = "sc_regionrisk_last_update_time";
    public static final String REGION_RISK_TERMS_SET = "sc_regionrisk_terms_set";
    public static final String REGION_RISK_TERM_INFOS_PREFIX = "sc_regionrisk_term_infos_";
    public static final String REGION_RISK_ITEM_INFO_PREFIX = "sc_regionrisk_item_info_";

    public static final String INDUSTRY_RISK_LAST_UPDATE_TIME = "sc_industryrisk_last_update_time";
    public static final String INDUSTRY_RISK_TERMS_SET = "sc_industryrisk_terms_set";
    public static final String INDUSTRY_RISK_TERM_INFOS_PREFIX = "sc_industryrisk_term_infos_";
    public static final String INDUSTRY_RISK_ITEM_INFO_PREFIX = "sc_industryrisk_item_info_";

    public static final String INDUSTRY_GEN_RISK_LAST_UPDATE_TIME = "sc_industrygenrisk_last_update_time";
    public static final String INDUSTRY_GEN_RISK_TERMS_SET = "sc_industrygenrisk_terms_set";
    public static final String INDUSTRY_GEN_RISK_TERM_INFOS_PREFIX = "sc_industrygenrisk_term_infos_";
    public static final String INDUSTRY_GEN_RISK_ITEM_INFO_PREFIX = "sc_industrygenrisk_item_info_";
}
