package cn.com.deepdata.streamstorm.controller;

import java.util.HashMap;
import java.util.Map;

enum EIndexType {
	None, ByDay, ByMonth
}

public class ActionController {
	static public Map<String, Action> actions = new HashMap<>();

	static {
		actions.put("addUsers", new Action("addUsers", false, "none",
				EAnalyzeType.None, "-user", EIndexType.ByMonth));
		actions.put("addContents", new Action("addContents", true, "url",
				EAnalyzeType.RiskInfo, "-content", EIndexType.ByDay));
		actions.put("addLog", new Action("addLog", false, "none",
				EAnalyzeType.None, "-log", EIndexType.ByMonth));
		actions.put("addCreditInfo", new Action("addCreditInfo", true, "url",
				EAnalyzeType.None, "credit", EIndexType.None));
		actions.put("addLawInfo", new Action("addLawInfo", true, "url",
				EAnalyzeType.None, "lawinfo", EIndexType.None));
		actions.put("addUnusualCompany", new Action("addUnusualCompany", true,
				"url", EAnalyzeType.None, "unusual-company", EIndexType.None));
		actions.put("addCheat", new Action("addCheat", true, "url",
				EAnalyzeType.None, "cheat", EIndexType.None));
		actions.put("addJudgement", new Action("addJudgement", true, "url",
				EAnalyzeType.None, "judgement", EIndexType.None));
		// 记录flume中解析错误并被丢弃的event
		actions.put("addLogError", new Action("addLogError", false, "none",
				EAnalyzeType.None, "-log-error", EIndexType.ByDay));
	}
}
