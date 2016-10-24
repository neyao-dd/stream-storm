package cn.com.deepdata.streamstorm.util;

import cn.com.deepdata.streamstorm.controller.UsrDefineWordsController;
import cn.com.deepdata.streamstorm.entity.Region;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by yukh on 2016/10/9
 */
public class StormUtil {

    private transient static Logger logger = LoggerFactory.getLogger(StormUtil.class);

    public static final Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    public static final Type type_ss = new TypeToken<Set<String>>() {
    }.getType();
    public static final Type type_hss = new TypeToken<Map<String, String>>() {
    }.getType();
    public static final Type type_hos = new TypeToken<Map<String, Object>>() {
    }.getType();
    public static final Type type_ls = new TypeToken<List<String>>() {
    }.getType();
    private static Client client = Client.create();
//    private static String radarServerHost;
//    private static String radarHost;
//    private static String urlDupRedisHost;
//    private static String urlDupRedisPort;
//    // 排重方式
//    private static String deduplication;
//    private static boolean existUuid;
//    private static String uuidHost;
//    private static int uuidPageSize;
//    // 计算方式
//    private static String calcType;
//    private static String regionHost;
//    private static String redisHost;
//    private static int redisPort;
//    private static boolean newRegion;
//    public static Gson gson = new Gson();

//    static {
//        try {
//            PropertiesConfiguration config = new PropertiesConfiguration(getRuntimeJarOuterPath() + "conf/flume.properties");
//            radarServerHost = config.getString("radarServerHost");
//            radarHost = config.getString("radarHost");
//            urlDupRedisHost = config.getString("urlDupRedisHost");
//            urlDupRedisPort = config.getString("urlDupRedisPort");
//            deduplication = config.getString("deduplication");
//            existUuid = config.getBoolean("existUuid");
//            uuidHost = config.getString("uuidHost");
//            uuidPageSize = config.getInt("uuidPageSize");
//            //TODO enum
//            calcType = config.getString("calcType");
//            newRegion = config.getBoolean("isNewRegion");
//            regionHost = config.getString("regionHost");
//            redisHost = config.getString("redisHost");
//            redisPort = config.getInt("redisPort");
//        } catch (ConfigurationException e) {
//            e.printStackTrace();
//        }
//    }

//    public static String getRuntimeJarOuterPath() {
//        String path = StormUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//        if (path.endsWith(".jar"))
//            path = path.substring(0, path.lastIndexOf("/"));
//        return path.substring(0, path.lastIndexOf("/") + 1);
//    }

    public static String getExceptionString(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        e.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    /**
     * rest get request
     *
     * @param host
     */
    public static String getRequest(String host) {
        WebResource webResource = client.resource(host);
        ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }
        return response.getEntity(String.class);
    }
}
