package cn.com.deepdata.streamstorm.controller;

import java.util.HashMap;
import java.util.Map;

public class ActionController {
	static public Map<String, Action> actions = new HashMap<>();

	static {
		actions.put("addUsers", new Action("addUsers", EDeDupType.None, EAnalyzeType.None, "-user", EIndexType.ByMonth));
		actions.put("addContents", new Action("addContents", EDeDupType.ByUrl, EAnalyzeType.RiskInfo, "-content", EIndexType.ByDay));
		actions.put("addLog", new Action("addLog", EDeDupType.None, EAnalyzeType.None, "-log", EIndexType.ByMonth));
		actions.put("addCreditInfo", new Action("addCreditInfo", EDeDupType.ByUrl, EAnalyzeType.None, "credit", EIndexType.None));
		actions.put("addLawInfo", new Action("addLawInfo", EDeDupType.ByUrl, EAnalyzeType.None, "lawinfo", EIndexType.None));
		actions.put("addUnusualCompany", new Action("addUnusualCompany", EDeDupType.ByUrl, EAnalyzeType.None, "unusual-company", EIndexType.None));
		actions.put("addCheat", new Action("addCheat", EDeDupType.ByUrl, EAnalyzeType.None, "cheat", EIndexType.None));
		actions.put("addJudgement", new Action("addJudgement", EDeDupType.ByUrl, EAnalyzeType.None, "judgement", EIndexType.None));
		// 记录flume中解析错误并被丢弃的event
		actions.put("addLogError", new Action("addLogError", EDeDupType.None, EAnalyzeType.None, "-log-error", EIndexType.ByDay));
	}
}
