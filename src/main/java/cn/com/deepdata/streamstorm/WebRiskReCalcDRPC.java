package cn.com.deepdata.streamstorm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import cn.com.deepdata.streamstorm.bolt.ESLookUpBolt;
import cn.com.deepdata.streamstorm.bolt.ReCalcDRPCResutlBolt;

public class WebRiskReCalcDRPC {
	static final String OPTION_CONFIG = "conf";

	private static Option option(int argCount, String shortName, String longName, String description) {
		return option(argCount, shortName, longName, longName, description);
	}

	private static Option option(int argCount, String shortName, String longName, String argName, String description) {
		Option option = OptionBuilder.hasArgs(argCount).withArgName(argName).withLongOpt(longName).withDescription(description).create(shortName);
		return option;
	}

	private static void usage(Options options) {
		System.out.println("usage:");
		System.out.println(String.format("  storm jar <my_topology_uber_jar.jar> %s --%s <file>", WebRiskReCalcDRPC.class.getName(), OPTION_CONFIG));
	}

	private static void submitDRPCTopology(Properties prop) {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("risk-recalc");
		builder.addBolt(new ESLookUpBolt(prop.getProperty("es.nodes", "slave1:9200")), 1);
		String redisHost = prop.getProperty("analyzer.redis.host", "slave1").trim();
		String redisPort = prop.getProperty("analyzer.redis.port", "11122").trim();
		String redisPassword = prop.getProperty("analyzer.redis.password", "deepdata").trim();
		String redisDatabase = prop.getProperty("analyzer.redis.database", "2").trim();
		JedisPoolConfig jedisCfg = new JedisPoolConfig(redisHost, Integer.parseInt(redisPort), 2000, redisPassword, Integer.parseInt(redisDatabase));
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Options options = new Options();
		options.addOption(option(1, "c", OPTION_CONFIG, "file", "Config property file. Use the specified file as a source of properties"));
		try {
			CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse(options, args);
			if (!cmd.hasOption(OPTION_CONFIG)) {
				usage(options);
				System.exit(1);
			}
			String confFile = cmd.getOptionValue(OPTION_CONFIG);
			System.out.println("confFile:" + confFile);
			InputStream is = new FileInputStream(confFile);;
			if (is == null) {
				System.out.println("config file (" + confFile + ") does not exist");
				System.exit(2);
			}
			Properties prop = new Properties();
			prop.load(is);
			submitDRPCTopology(prop);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
