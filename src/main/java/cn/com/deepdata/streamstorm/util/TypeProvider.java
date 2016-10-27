package cn.com.deepdata.streamstorm.util;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yukh on 2016/10/25
 */
public class TypeProvider {
    public static final Type type_ss = new TypeToken<Set<String>>() {
    }.getType();
    public static final Type type_mss = new TypeToken<Map<String, String>>() {
    }.getType();
    public static final Type type_mso = new TypeToken<Map<String, Object>>() {
    }.getType();
    public static final Type type_ls = new TypeToken<List<String>>() {
    }.getType();
}
