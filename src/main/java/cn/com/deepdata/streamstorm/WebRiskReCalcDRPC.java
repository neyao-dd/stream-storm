package cn.com.deepdata.streamstorm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import cn.com.deepdata.streamstorm.bolt.AnalyzeIndRegRiskBolt;
import cn.com.deepdata.streamstorm.bolt.AnalyzeWebRiskBolt;
import cn.com.deepdata.streamstorm.bolt.CutWordsBolt;
import cn.com.deepdata.streamstorm.bolt.ESDataProcessBolt;
import cn.com.deepdata.streamstorm.bolt.ESLookUpBolt;
import cn.com.deepdata.streamstorm.bolt.ParserBolt;
import cn.com.deepdata.streamstorm.bolt.ReCalcDRPCResutlBolt;

public class WebRiskReCalcDRPC {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("risk-recalc");
		builder.addBolt(new ESLookUpBolt(), 1);
		JedisPoolConfig jedisCfg = new JedisPoolConfig("slave1", 11122, 2000, "deepdata", 2);
		builder.addBolt(new CutWordsBolt(jedisCfg), 5).shuffleGrouping();
		builder.addBolt(new AnalyzeWebRiskBolt(jedisCfg), 5).shuffleGrouping();
		builder.addBolt(new AnalyzeIndRegRiskBolt(jedisCfg), 5).shuffleGrouping();
		builder.addBolt(new ReCalcDRPCResutlBolt(), 1).shuffleGrouping();
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(8);
		try {
			StormSubmitter.submitTopology("recalc-drpc", conf, builder.createRemoteTopology());
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
