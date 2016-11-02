package cn.com.deepdata.streamstorm.util;

import cn.com.deepdata.streamstorm.entity.Region;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.com.deepdata.streamstorm.util.RESTUtil.*;

/**
 * Created by yukh on 2016/10/24
 */
public class RegionUtil {
    private transient Logger logger = LoggerFactory.getLogger(Region.class);

    private Map<String, Map<Integer, Integer>> regionAlias = new HashMap<>();
    private Map<Integer, Region> regionDetail = new HashMap<>();
    private String host;

    public RegionUtil(String host) {
        this.host = host;
        long s = System.currentTimeMillis();
        syncNewRegion();
        logger.info("loadNewRegion use time:" + (System.currentTimeMillis() - s));
        logger.info("regionAlias size:" + regionAlias.size());
        logger.info("regionDetail size():" + regionDetail.size());
    }

    public Map<String, Map<Integer, Integer>> getRegionAlias() {
        return regionAlias;
    }

    public Map<Integer, Region> getRegionDetail() {
        return regionDetail;
    }

    public String getRegionName(int id) {
        if (regionDetail.containsKey(id))
            return regionDetail.get(id).sca_region;
        logger.error("缺少名字的地区id：" + id);
        return "";
    }

    public int getParentId(int id) {
        if (regionDetail.containsKey(id))
            return regionDetail.get(id).ina_pid;
        logger.error("缺少id的地区id：" + id);
        return 0;
    }

    public void syncNewRegion() {
        try {
            Gson gson = new Gson();
            String response = getRequest(host);
            Map<String, Object> region = gson.fromJson(response, TypeProvider.type_mso);
            List<Map<String, Object>> areaList;
            String[] areaType = {"AREA", "CITY", "PROVINCE"};
            for (String at : areaType) {
                areaList = (List<Map<String, Object>>) region.get(at);
                for (Map<String, Object> areaInfo : areaList) {
                    List<Map<String, Object>> alias = (ArrayList<Map<String, Object>>) areaInfo.get("alias");
                    if (validList(alias))
                        continue;
                    int id = Integer.parseInt(areaInfo.get("id").toString());
                    String name = areaInfo.get("name").toString();
                    int pid;
                    try {
                        pid = Integer.parseInt(areaInfo.get("parent_id").toString());
                    } catch (NumberFormatException e) {
                        pid = 0;
                    }
                    addRegionAlias(name, id, pid);
                    for (Map<String, Object> map : alias) {
                        String ali = map.get("alias").toString();
                        addRegionAlias(ali, id, pid);
                    }
                    regionDetail.put(id, new Region(name, areaInfo.get("uuid").toString(), id, pid));
                }
            }
        } catch (Exception e) {
            logger.error("syncRegion error..." + CommonUtil.getExceptionString(e));
        }
    }

    // TODO: 2016/10/24 multimap
    public void addRegionAlias(String name, int id, int pid) {
        Map<Integer, Integer> idMapping;
        if (regionAlias.containsKey(name)) {
            idMapping = regionAlias.get(name);
            idMapping.put(id, pid);
        } else {
            idMapping = new HashMap<>();
            idMapping.put(id, pid);
            regionAlias.put(name, idMapping);
        }
    }

    public boolean validList(List list) {
        return list == null || list.isEmpty();
    }
}
