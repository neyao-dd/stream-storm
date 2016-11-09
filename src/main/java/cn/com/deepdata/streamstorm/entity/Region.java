package cn.com.deepdata.streamstorm.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by yukh on 2016/10/17
 */
public class Region implements Serializable {
    public String sca_region;
    public double dna_score;
    public String snc_uuid;
    public int ina_id;
    public boolean bna_analyze;
    public transient int ina_pid;

    public Region(String sca_region, String snc_uuid, int ina_id, int ina_pid) {
        this.sca_region = sca_region;
        this.dna_score = 0.;
        this.snc_uuid = snc_uuid;
        this.ina_id = ina_id;
        this.bna_analyze = false;
        this.ina_pid = ina_pid;
    }

    public Region(String sca_region, String snc_uuid, int ina_id, boolean bna_analyze, int ina_pid) {
        this.sca_region = sca_region;
        this.dna_score = 0.;
        this.snc_uuid = snc_uuid;
        this.ina_id = ina_id;
        this.bna_analyze = bna_analyze;
        this.ina_pid = ina_pid;
    }

    public Region(Map<String, Object> map) {
        this.sca_region = map.get("name").toString();
        this.dna_score = 0.;
        this.snc_uuid = map.get("uuid").toString();
        this.ina_id = Integer.parseInt(map.get("id").toString());
        this.bna_analyze = false;
        try {
            this.ina_pid = Integer.parseInt(map.get("parent_id").toString());
        } catch (Exception e) {
            this.ina_pid = 0;
        }
    }

    public Region clone() {
        return new Region(this.sca_region, this.snc_uuid, this.ina_id, this.ina_pid);
    }

    @Override
    public String toString() {
        return "{\"sca_region\":" + sca_region + ",\"snc_uuid\":" + snc_uuid + ",\"dna_score\":" + dna_score + "}";
    }
}
