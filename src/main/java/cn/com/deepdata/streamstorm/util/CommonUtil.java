package cn.com.deepdata.streamstorm.util;

import java.io.PrintWriter;
import java.io.StringWriter;

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
}
