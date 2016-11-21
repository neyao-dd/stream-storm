package cn.com.deepdata.streamstorm.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.storm.tuple.Tuple;

/**
 * Created by yukh on 2016/10/25
 */
public class CommonUtil {
	public static String getExceptionString(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		e.printStackTrace(pw);
		return sw.getBuffer().toString();
	}

	public static String getRuntimeJarOuterPath() {
		String path = CommonUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (path.endsWith(".jar"))
			path = path.substring(0, path.lastIndexOf("/"));
		return path.substring(0, path.lastIndexOf("/") + 1);
	}

	public static boolean validString(String s) {
		return !(s == null || s.length() == 0);
	}

	public static String getSortTime(Map<String, Object> doc) {
		String sortTime = "unknown";
		if (doc.containsKey("tfp_sort_time")) {
			sortTime = ((String) doc.get("tfp_sort_time")).substring(0, 10);
		} else if (doc.containsKey("tfc_time")) {
			sortTime = ((String) doc.get("tfc_time")).substring(0, 10);
		}
		if (!sortTime.equals("unknown")) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(sdf.parse(sortTime));
				Calendar maxCalendar = Calendar.getInstance();
				maxCalendar.add(Calendar.DATE, 2);
				if (calendar.get(Calendar.YEAR) < 2010)
					sortTime = "unknown";
				else if (calendar.compareTo(maxCalendar) > 0)
					sortTime = "unknown";
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				sortTime = "unknown";
			}
		}
		return sortTime;
	}

	public static String getSaveTime(Map<String, Object> doc) {
		String saveTime = "unknown";
		if (doc.containsKey("tfp_save_time")) {
			saveTime = ((String) doc.get("tfp_save_time")).substring(0, 10);
		}
		if (saveTime.equals("unknown")) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			saveTime = sdf.format(System.currentTimeMillis());
		}
		return saveTime;
	}
}
