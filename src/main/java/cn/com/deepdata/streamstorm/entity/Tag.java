package cn.com.deepdata.streamstorm.entity;

import com.google.gson.Gson;

/**
 * Created by yukh on 2016/10/20
 */
public class Tag implements Entity {
	public String sna_tag;

	public Tag(String tag) {
		sna_tag = tag;
	}

	public String toString() {
		return new Gson().toJson(this);
	}
}
