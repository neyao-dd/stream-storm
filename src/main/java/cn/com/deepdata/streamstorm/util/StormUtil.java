package cn.com.deepdata.streamstorm.util;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yukh on 2016/10/9
 */
public class StormUtil {

    private transient static Logger log = LoggerFactory.getLogger(StormUtil.class);

    public static final Type type = new TypeToken<Map<String, Object>>(){}.getType();
    public static final Type type_hss = new TypeToken<HashMap<String, String>>() {}.getType();
    public static final Type type_ls = new TypeToken<List<String>>() {}.getType();
    private static Client client = Client.create();
    private static String radarServerHost;
    private static String radarHost;
    private static String urlDupRedisHost;
    private static String urlDupRedisPort;
    // 排重方式
    private static String deduplication;
    private static boolean existUuid;
    private static String uuidHost;
    private static int uuidPageSize;
    // 计算方式
    private static String calcType;
    private static String regionHost;
    private static String redisHost;
    private static int redisPort;
    private static boolean newRegion;
    public static Gson gson = new Gson();

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(getRuntimeJarOuterPath() + "conf/flume.properties");
            radarServerHost = config.getString("radarServerHost");
            radarHost = config.getString("radarHost");
            urlDupRedisHost = config.getString("urlDupRedisHost");
            urlDupRedisPort = config.getString("urlDupRedisPort");
            deduplication = config.getString("deduplication");
            existUuid = config.getBoolean("existUuid");
            uuidHost = config.getString("uuidHost");
            uuidPageSize = config.getInt("uuidPageSize");
            //TODO enum
            calcType = config.getString("calcType");
            newRegion = config.getBoolean("isNewRegion");
            regionHost = config.getString("regionHost");
            redisHost = config.getString("redisHost");
            redisPort = config.getInt("redisPort");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static String getRuntimeJarOuterPath() {
        String path = StormUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path.endsWith(".jar"))
            path = path.substring(0, path.lastIndexOf("/"));
        return path.substring(0, path.lastIndexOf("/") + 1);
    }

    public static String getExceptionString(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        e.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public static void addMap(Map<String, List<Integer>> m, String k, int v) {
        if (m.containsKey(k)) {
            List<Integer> li = m.get(k);
            li.add(v);
        } else {
            List<Integer> li = new ArrayList<>();
            li.add(v);
            m.put(k, li);
        }
    }

    public static void parseMap(Map<String, Integer> map, Map<String, Integer> old) {
        old.forEach((k, v) -> {
            if (map.containsKey(k))
                map.put(k, v + map.get(k));
            else
                map.put(k, v);
        });
    }

    public static void parseMap(Map<String, List<Integer>> map, Map<String, List<Integer>> old, int position) {
        for (String word : old.keySet()) {
            if (map.containsKey(word)) {
                List<Integer> pos = map.get(word);
                old.get(word).stream().forEach(p -> pos.add(p + position));
            }
            else {
                List<Integer> pos = new ArrayList<>();
                old.get(word).stream().forEach(p -> pos.add(p + position));
                map.put(word, pos);
            }
        }
    }

    /**
     * rest get request
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

    public static String getTitle(String head) {
        String title = null;
        Type headersType = new TypeToken<HashMap<String, Object>>() {
        }.getType();
        Map<String, String> header = (new Gson()).fromJson(head, headersType);
        if (header.containsKey("scc_title"))
            title = header.get("scc_title");
        else if (header.containsKey("scm_title"))
            title = header.get("scm_title");
        return title;
    }

    public static int getID(String name) {
        if (idByName.containsKey(name))
            return idByName.get(name);
        return 0;
    }

    public static String getUUID(String name) {
        int id = getID(name);
        if (0 == id)
            return "";
        return getUUID(id);
    }

    public static String getUUID(int id) {
        if (uuidById.containsKey(id))
            return uuidById.get(id);
        return "";
    }

    public static boolean isNewRegion() {
        return newRegion;
    }

    public static boolean existUuid() {
        return existUuid;
    }

    public static int getRedisPort() {
        return redisPort;
    }

    public static String getRadarServerHost() {
        return radarServerHost;
    }

    public static String getRadarHost() {
        return radarHost;
    }

    public static String getUrlDupRedisHost() {
        return urlDupRedisHost;
    }

    public static String getUrlDupRedisPort() {
        return urlDupRedisPort;
    }

    public static String getDeduplication() {
        return deduplication;
    }

    public static String getUuidHost() {
        return uuidHost;
    }

    public static int getUuidPageSize() {
        return uuidPageSize;
    }

    public static String getCalcType() {
        return calcType;
    }

    public static String getRegionHost() {
        return regionHost;
    }

    public static String getRedisHost() {
        return redisHost;
    }
}
