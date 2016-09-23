package cn.com.deepdata.streamstorm.controller;

import java.util.HashMap;
import java.util.Map;

enum EIndexType {
	None, ByDay, ByMonth
}

public class ActionController {
	static public Map<String, Action> actions = new HashMap<>();

	static {
		actions.put("addUsers", new Action("addUsers", false, "none", false,
				"-user", EIndexType.ByMonth));
		actions.put("addContents", new Action("addContents", true, "url", true,
				"-content", EIndexType.ByDay));
		actions.put("addLog", new Action("addLog", false, "none", false,
				"-log", EIndexType.ByMonth));
		actions.put("addCreditInfo", new Action("addCreditInfo", true, "url",
				false, "credit", EIndexType.None));
		actions.put("addLawInfo", new Action("addLawInfo", true, "url", false,
				"lawinfo", EIndexType.None));
		actions.put("addUnusualCompany", new Action("addUnusualCompany", true,
				"url", false, "unusual-company", EIndexType.None));
		actions.put("addCheat", new Action("addCheat", true, "url", false,
				"cheat", EIndexType.None));
		actions.put("addJudgement", new Action("addJudgement", true, "url",
				false, "judgement", EIndexType.None));
		// 记录flume中解析错误并被丢弃的event
		actions.put("addLogError", new Action("addLogError", false, "none",
				false, "-log-error", EIndexType.ByDay));
	}
}
