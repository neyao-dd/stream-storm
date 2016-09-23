package cn.com.deepdata.streamstorm.controller;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class BloomFilterController {

	private static final Logger log = LoggerFactory
			.getLogger(BloomFilterController.class);
	static public final String urlSetKeyPrefix = "RS_URLS_";
	static public final String urlCountKeyPrefix = "RS_URLC_";
	static public final String bloomHashBitmapKeyPrefix = "RS_BHB_";
	static public final String bloomHashDirtyKeyPrefix = "RS_BHD_";
	static public final String dayListKey = "RS_DL";
	static public final int compareDaysCount = 32;
	static final int[] hashseeds = { 31, 131, 1313, 13131, 131313, 1313131,
			13131313, 131313131, 1313131313 };
	static final int bitSize = 1 << 30;

	public static String[] GetDaySuffix(int days) {
		String[] res = new String[days];
		long time = System.currentTimeMillis();
		long milSecInDay = 24 * 3600 * 1000;
		for (int i = 0; i < days; i++) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(time);
			int year = cal.get(Calendar.YEAR) - 2000;
			int month = cal.get(Calendar.MONTH) + 1;
			int day = cal.get(Calendar.DAY_OF_MONTH);
			time -= milSecInDay;
			String strDay = String.format("%02d%02d%02d", year, month, day);
			res[i] = strDay;
		}
		return res;
	}

	public void InsertUrls(Jedis jedis, ArrayList<String> urls) {
		ArrayList<Integer> setbits = new ArrayList<Integer>(0);
		for (String url : urls) {
			for (int i = 0; i < hashseeds.length; i++) {
				int seed = hashseeds[i];
				int hash = BKDRHash(url, seed);
				setbits.add(hash % bitSize);
			}
		}

		String[] strDaySuffix = GetDaySuffix(1);
		Transaction tx = jedis.multi();
		for (Integer setbit : setbits) {
			tx.setbit(bloomHashBitmapKeyPrefix + strDaySuffix[0], setbit, true);
		}
		tx.exec();
		jedis.setbit(bloomHashDirtyKeyPrefix + strDaySuffix[0], 0, true);
		jedis.incrBy(urlCountKeyPrefix + strDaySuffix[0], urls.size());

		String lastDay = jedis.lindex(dayListKey, -1);
		if (lastDay == null || !lastDay.equals(strDaySuffix[0]))
			jedis.rpush(dayListKey, strDaySuffix[0]);
		while (jedis.llen(dayListKey) > compareDaysCount) {
			String rmDay = jedis.lpop(dayListKey);
			jedis.del(urlSetKeyPrefix + rmDay);
			jedis.del(urlCountKeyPrefix + rmDay);
			jedis.del(bloomHashBitmapKeyPrefix + rmDay);
			jedis.del(bloomHashDirtyKeyPrefix + rmDay);
			log.info("Remove " + rmDay + "'s URLs");
		}
	}

	public boolean existURL(Jedis jedis, String url) {
		ArrayList<Integer> bits = new ArrayList<Integer>(0);
		for (int i = 0; i < hashseeds.length; i++) {
			int seed = hashseeds[i];
			int hash = BKDRHash(url, seed);
			bits.add(hash % bitSize);
		}

		boolean dup = false;
		List<String> compDaysSet = jedis.lrange(dayListKey, 0, -1);
		String findDay = null;
		for (String strDaySuffix : compDaysSet) {
			String strKey = bloomHashBitmapKeyPrefix + strDaySuffix;
			Transaction tx = jedis.multi();
			for (Integer bit : bits) {
				// System.out.println(strKey + " " + bit);
				tx.getbit(strKey, bit);
			}
			List<Object> res = tx.exec();
			// System.out.println(res.toString());
			boolean dup2 = true;
			for (Object value : res) {
				if (!(Boolean) value) {
					dup2 = false;
					break;
				}
			}
			dup = dup2;
			if (dup) {
				findDay = strDaySuffix;
				log.debug("find url in " + findDay);
				break;
			}
		}
		return dup;
	}

	public boolean checkUrlExistAndInsert(Jedis jedis, String url) {
		ArrayList<Integer> bits = new ArrayList<Integer>(0);
		for (int i = 0; i < hashseeds.length; i++) {
			int seed = hashseeds[i];
			int hash = BKDRHash(url, seed);
			bits.add(hash % bitSize);
		}

		boolean dup = false;
		try {
			String strToday = GetDaySuffix(1)[0];
			jedis.watch(dayListKey);
			List<String> compDaysSet = jedis.lrange(dayListKey, 0, -1);
			int totalDays = compDaysSet.size();
			String lastDay = (totalDays == 0) ? "" : compDaysSet
					.get(compDaysSet.size() - 1);
			if (strToday.compareToIgnoreCase(lastDay) > 0) {
				Transaction tx1 = jedis.multi();
				tx1.rpush(dayListKey, strToday);
				tx1.exec();
				compDaysSet.add(strToday);
				lastDay = strToday;
				totalDays++;
			}
			jedis.unwatch();
			Transaction tx = jedis.multi();
			for (String strDaySuffix : compDaysSet) {
				String strKey = bloomHashBitmapKeyPrefix + strDaySuffix;
				for (Integer bit : bits) {
					// System.out.println(strKey + " " + bit);
					tx.getbit(strKey, bit);
				}
			}
			List<Object> res = tx.exec();
			// System.out.println(res.toString());
			for (int i = 0; i < compDaysSet.size(); i++) {
				// String strDaySuffix = compDaysSet.get(i);
				boolean dup2 = true;
				for (int j = 0; j < bits.size(); j++) {
					Object value = res.get(i * bits.size() + j);
					if (!(Boolean) value) {
						dup2 = false;
					}
				}
				dup = dup2;
				if (dup) {
					break;
				}
			}
			if (!dup) {
				tx = jedis.multi();
				for (Integer setbit : bits) {
					tx.setbit(bloomHashBitmapKeyPrefix + strToday, setbit, true);
				}
				tx.setbit(bloomHashDirtyKeyPrefix + strToday, 0, true);
				tx.exec();
			}

			while (true) {
				jedis.watch(dayListKey);
				compDaysSet = jedis.lrange(dayListKey, 0, -1);
				if (compDaysSet.size() > compareDaysCount) {
					tx = jedis.multi();
					tx.lpop(dayListKey);
					String rmDay = compDaysSet.get(compDaysSet.size() - 1);
					tx.del(urlSetKeyPrefix + rmDay);
					tx.del(urlCountKeyPrefix + rmDay);
					tx.del(bloomHashBitmapKeyPrefix + rmDay);
					tx.del(bloomHashDirtyKeyPrefix + rmDay);
					tx.exec();
					compDaysSet.remove(compDaysSet.size() - 1);

					Set<String> remKeys = jedis.keys(bloomHashBitmapKeyPrefix
							+ "*");
					for (String remKey : remKeys)
						if (!compDaysSet.contains(remKey.substring(remKey
								.length() - 6)))
							jedis.del(remKey);
					remKeys = jedis.keys(bloomHashDirtyKeyPrefix + "*");
					for (String remKey : remKeys)
						if (!compDaysSet.contains(remKey.substring(remKey
								.length() - 6)))
							jedis.del(remKey);
				} else {
					jedis.unwatch();
					break;
				}
			}
		} catch (Exception e) {
			log.error(e.toString());
		}
		return dup;
	}

	public static int BKDRHash(String str, int seed) {
		// BKDR Hash Function
		int hash = 0;
		for (int i = 0; i < str.length(); i++) {
			hash = hash * seed + str.charAt(i);
		}
		return hash & 0x7FFFFFFF;
	}
}
