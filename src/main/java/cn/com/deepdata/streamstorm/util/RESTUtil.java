package cn.com.deepdata.streamstorm.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by yukh on 2016/10/9
 */
public class RESTUtil {

    private transient static Logger logger = LoggerFactory.getLogger(RESTUtil.class);
    private static Client client = Client.create();

    public static String getRequest(String host) {
        ClientResponse response = null;
        try {
            WebResource webResource = client.resource(host);
            response = webResource.accept("application/json").get(ClientResponse.class);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }
            return response.getEntity(String.class);
        } finally {
            if (null != response)
                response.close();
        }
    }

    public static String postRequest(String host, String path, String json) {
        ClientResponse response = null;
        try {
            WebResource webResource = client.resource(host);
            response = webResource.path(path).accept("application/json")
                    .type("application/json").post(ClientResponse.class, json);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            return response.getEntity(String.class);
        } finally {
            if (null != response)
               response.close();
        }
    }

}
