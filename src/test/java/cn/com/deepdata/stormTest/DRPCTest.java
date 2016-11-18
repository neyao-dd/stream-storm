package cn.com.deepdata.stormTest;

import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

public class DRPCTest {
	public static void main(String[] args) {
		Config conf = new Config();
		Map defaultConfig = Utils.readDefaultConfig();
		conf.putAll(defaultConfig);
		try {
			DRPCClient client = new DRPCClient(conf, "master", 3772);
			List<String> docInfo = Lists.newArrayList("storm-test1", "flumetype", "AVhIjZp-_GvZumdm71ps");
			String result = client.execute("risk-recalc", new Gson().toJson(docInfo));
			System.out.println(result);
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
