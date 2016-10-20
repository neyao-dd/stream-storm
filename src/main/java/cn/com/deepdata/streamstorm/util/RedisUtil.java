package cn.com.deepdata.streamstorm.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by yukh on 2016/10/18.
 */
public class RedisUtil {
    private JedisPool pool = null;

    public void initialize(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1000);
        config.setMaxWaitMillis(30000L);
        pool = new JedisPool(config, host, port);
    }

    public void close() {
        pool.destroy();
    }

    public Jedis getJedisResource(int redisDBNum) {
        Jedis jedis = pool.getResource();
        jedis.select(redisDBNum);
        return jedis;
    }

}
