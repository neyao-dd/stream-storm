package cn.com.deepdata.streamstorm.controller;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScanResult;
import cn.com.deepdata.commonutil.UsrLibraryController;

public class UsrDefineWordsController {
	private transient static Log log = LogFactory
			.getLog(UsrDefineWordsController.class);
	private final RedisKeys redisKeys;
	private final String nature;
	private Map<String, String> usrWords = null;
	private Map<String, String> addedWords = null;
	private Map<String, String> deletedWords = null;
	private String version = null;
	private String subVersion = null;
	private static final Logger LOG = LoggerFactory
			.getLogger(UsrDefineWordsController.class);

	public UsrDefineWordsController(RedisKeys redisKeys, String nature) {
		this.redisKeys = redisKeys;
		this.nature = nature;
	}

	public Map<String, String> loadFromRedis(JedisCommands jedisCommands,
			String key) {
		Map<String, String> ret = new HashMap<>(0);
		String nowVer = jedisCommands.get(redisKeys.versionKey);
		String cursor = "0";
		String tokenSetKey = key.replace("%v%", nowVer);
		// System.out.println("token num:" + jedis.scard(riskTokenSetKey));
		while (true) {
			ScanResult<String> bResult = jedisCommands.sscan(tokenSetKey,
					cursor);
			if (bResult.getResult().size() == 0
					|| bResult.getStringCursor() == null
					|| bResult.getStringCursor().length() == 0)
				break;
			cursor = bResult.getStringCursor();
			for (String word : bResult.getResult()) {
				ret.put(word, nature);
			}
			if (cursor.equals("0"))
				break;
		}
		return ret;
	}

	public void loadRedisWordsWithNoVersion(JedisCommands jedisCommands,
			String key) {
		usrWords = loadFromRedis(jedisCommands, key);
		AddWords(usrWords);
	}

	public int usrWordsSize() {
		return usrWords.size();
	}

	public void AddWords(Map<String, String> words) {
		log.info("AddWords:" + words.size());
		UsrLibraryController.ChangeNature(words,
				UsrLibraryController.EChangeOprationType.kAddOpration);
	}

	private void RemoveWords(Map<String, String> words) {
		UsrLibraryController.ChangeNature(words,
				UsrLibraryController.EChangeOprationType.kDeleteOpration);
	}

	public int load(JedisCommands jedisCommands) {
		String nowVer = jedisCommands.get(redisKeys.versionKey);
		if (usrWords == null || version == null || !version.equals(nowVer)) {
			if (usrWords != null && usrWords.size() > 0)
				RemoveWords(usrWords);
			if (addedWords != null && addedWords.size() > 0)
				RemoveWords(addedWords);
			usrWords = loadFromRedis(jedisCommands, redisKeys.tokensKey);
			AddWords(usrWords);
			addedWords = null;
			deletedWords = null;
			version = nowVer;
			LOG.info("Load " + nature + " words. count:" + usrWords.size()
					+ " version:" + version);
		} else if (redisKeys.subVerKey != null
				&& jedisCommands.exists(redisKeys.subVerKey.replace("%v%",
						version))) {
			String nowSubVer = jedisCommands.get(redisKeys.subVerKey.replace(
					"%v%", version));
			if (subVersion == null || !subVersion.equals(nowSubVer)) {
				if (addedWords != null && addedWords.size() > 0)
					RemoveWords(addedWords);
				if (deletedWords != null && deletedWords.size() > 0)
					AddWords(deletedWords);
				addedWords = loadFromRedis(jedisCommands,
						redisKeys.addedTermsKey);
				deletedWords = loadFromRedis(jedisCommands,
						redisKeys.deletedTermsKey);
				if (addedWords != null && addedWords.size() > 0)
					AddWords(addedWords);
				if (deletedWords != null && deletedWords.size() > 0)
					RemoveWords(deletedWords);
				subVersion = nowSubVer;
				LOG.info("Update " + nature + " words. addedTerms:"
						+ addedWords.size() + "deletedTerms:"
						+ deletedWords.size() + " sub version:" + subVersion);
			}
		}
		return 0;
	}

	public String version() {
		return version;
	}

	public String subVersion() {
		return subVersion;
	}
}
