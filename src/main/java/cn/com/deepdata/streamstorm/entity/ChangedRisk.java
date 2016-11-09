package cn.com.deepdata.streamstorm.entity;

import java.io.Serializable;

/**
 * Created by yukh on 2016/11/1
 */
public class ChangedRisk implements Serializable {
	int inp_level;
	int inp_seq_no;
	String sca_risk_type;
	String tfc_change_date;

	public ChangedRisk(int level, int no, String date) {
		inp_level = level;
		inp_seq_no = no;
		tfc_change_date = date;
	}

	public ChangedRisk(int level, int no, String date, String type) {
		inp_level = level;
		inp_seq_no = no;
		tfc_change_date = date;
		sca_risk_type = type;
	}
}
