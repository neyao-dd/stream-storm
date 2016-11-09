package cn.com.deepdata.streamstorm.entity;

import java.util.Map;

/**
 * Created by yukh on 2016/11/1
 */
public class ChangeRecord implements Entity {
    public String scc_change_item;
    public String scc_before_content;
    public String scc_after_content;
    public int inp_seq_no;
    public String tfc_change_date;

    public ChangeRecord(String item, String before, String after, int no, String date) {
        scc_change_item = item;
        scc_before_content = before;
        scc_after_content = after;
        inp_seq_no = no;
        tfc_change_date = date;
    }

    public ChangeRecord(Map<String, Object> map) {
        scc_change_item = (String)map.get("scc_change_item");
        scc_before_content = (String)map.get("scc_before_content");
        scc_after_content = (String)map.get("scc_after_content");
        try {
            inp_seq_no = (int)(double)map.get("inp_seq_no");
        } catch (Exception e) {
            inp_seq_no = 0;
        }
        tfc_change_date = (String)map.get("tfc_change_date");
    }
}
