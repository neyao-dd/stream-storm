package cn.com.deepdata.streamstorm.entity;

/**
 * Created by yukh on 2016/11/11
 */
public class Industry implements Entity{
    private int ina_id;
    private double dna_score;

    public Industry(int id, double score) {
        ina_id = id;
        dna_score = score;
    }

    public int getIna_id() {
        return ina_id;
    }

    public void setIna_id(int ina_id) {
        this.ina_id = ina_id;
    }

    public double getDna_score() {
        return dna_score;
    }

    public void setDna_score(double dna_score) {
        this.dna_score = dna_score;
    }
}
