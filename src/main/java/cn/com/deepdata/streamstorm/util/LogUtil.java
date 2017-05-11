package cn.com.deepdata.streamstorm.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by shixin on 2017/4/19.
 */
public class LogUtil {

    private  static Logger logger = LoggerFactory.getLogger(LogUtil.class);

    public static String logString(Object obj){
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (IOException e) {
            logger.error("error while writeValueAsString", e);
        }
        return "";
    }
}
