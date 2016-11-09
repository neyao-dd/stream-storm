package cn.com.deepdata.streamstorm.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ClientInfoItem implements Serializable {
	private int id;
	private Map<String, ArrayList<String>> clientInfo;
	
	public ClientInfoItem() {
		id = 0;
		clientInfo = new HashMap<>();
	}
	
//	@Override
//	public int hashCode() {
//		return this.clientInfo.hashCode();
//	}
	
//	@Override
//	public boolean equals(Object o) {
//		ClientInfoItem cii = (ClientInfoItem)o;
//		return this.id == cii.getId() && this.client.equals(cii.getClient()) && this.type.equals(cii.getType());
//	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Map<String, ArrayList<String>> getClientInfo() {
		return clientInfo;
	}

	public void setClientInfo(Map<String, ArrayList<String>> clientInfo) {
		this.clientInfo = clientInfo;
	}
}
