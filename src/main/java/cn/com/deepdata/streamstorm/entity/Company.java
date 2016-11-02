package cn.com.deepdata.streamstorm.entity;

import cn.com.deepdata.commonutil.AnsjTermAnalyzer;

import java.util.*;

/**
 * Created by yukh on 2016/11/1
 */
public class Company {
    public int level;
    public String name;
    public String type;
    public Set<String> tokens;
    public static Set<String> allTokens = new HashSet<>();
    public static Map<String, String> allSubSen = new HashMap<>();

    public Company(String name, String type, int level) {
        this.level = level;
        this.name = name;
        this.type = type;
        String punctuation = "、|（|）|\\(|\\)|,|/|，";
        String subName = name.replace("变更", "");
        String[] sentence = subName.split(punctuation);
        for (String subSen : sentence)
            allSubSen.put(subSen, level + " " + type);
        List<String> subTokens = AnsjTermAnalyzer.simpleTokens(subName);
        tokens = new HashSet<>(subTokens);
        allTokens.addAll(tokens);
    }
}
