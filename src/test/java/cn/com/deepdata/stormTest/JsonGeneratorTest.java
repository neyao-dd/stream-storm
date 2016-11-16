package cn.com.deepdata.stormTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Maps;

public class JsonGeneratorTest {
	private JsonGenerator jsonGenerator = null;
	private ObjectMapper objectMapper = null;
	private ByteArrayOutputStream baos = null;

	public void init() {
		try {
			baos = new ByteArrayOutputStream();
			objectMapper = new ObjectMapper();
			jsonGenerator = objectMapper.getJsonFactory().createJsonGenerator(baos, JsonEncoding.UTF8);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void destory() {
		try {
			if (jsonGenerator != null) {
				jsonGenerator.flush();
			}
			if (!jsonGenerator.isClosed()) {
				jsonGenerator.close();
			}
			jsonGenerator = null;
			objectMapper = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void Test() {
		Map<String, Object> value = Maps.newHashMap();
		value.put("content", "哈哈😄");
		try {
			jsonGenerator.writeObject(value);
			baos.flush();
			System.out.println(baos.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			baos.reset();
			jsonGenerator.writeObject(value);
			baos.flush();
			String strV = baos.toString();
			System.out.println(strV);
			System.out.println(objectMapper.writeValueAsString(value));
			Map<String, Object> v = objectMapper.readValue(strV, Map.class);
			System.out.println(v);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		JsonGeneratorTest test = new JsonGeneratorTest();
		test.init();
		test.Test();
		test.destory();
	}
}
