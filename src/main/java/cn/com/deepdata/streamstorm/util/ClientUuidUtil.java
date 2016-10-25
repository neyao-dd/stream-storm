package cn.com.deepdata.streamstorm.util;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.com.deepdata.streamstorm.util.RESTUtil.getExceptionString;

/**
 * Created by Administrator on 2016/10/24
 */
public class ClientUuidUtil {
    private transient Logger logger = LoggerFactory.getLogger(ClientUuidUtil.class);

    private String host;
    private Map<String, Integer> idByName = new HashMap<>();
    private Map<Integer,String> uuidById = new HashMap<>();

    public ClientUuidUtil(String host){
        this.host = host;
        long s = System.currentTimeMillis();
        syncUuid();
        logger.info("syncUuid() use time:" + (System.currentTimeMillis() - s));
    }

    public int getId(String name) {
        if (idByName.containsKey(name))
            return idByName.get(name);
        return 0;
    }

    public String getUuid(String name) {
        int id = getId(name);
        if (0 == id)
            return "";
        return getUuid(id);
    }

    public String getUuid(int id) {
        if (uuidById.containsKey(id))
            return uuidById.get(id);
        return "";
    }

    public void syncUuid() {
        try {
            Gson gson = new Gson();
            int page = 0;
            int currentPage;
            do {
                String result = RESTUtil.getRequest(host);
                Map<String, Object> resultMap = gson.fromJson(result, RESTUtil.type_hos);
                currentPage = (int) (double) resultMap.get("current_page_total");
                List<Map<String, Object>> items = (List<Map<String, Object>>) resultMap.get("page_items");
                for (Map<String, Object> map : items) {
                    String uuid = map.get("uuid").toString();
                    int id = (int) (double) map.get("id");
                    String name = map.get("name").toString();
                    uuidById.put(id, uuid);
                    idByName.put(name, id);
                }
                page++;
            } while (currentPage > 0);
        } catch (Exception e) {
            logger.error("syncUuid error..." + getExceptionString(e));
        }
    }
}
