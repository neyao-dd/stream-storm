package cn.com.deepdata.stormTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import cn.com.deepdata.streamstorm.entity.ClientScore;
import cn.com.deepdata.streamstorm.entity.InnerRiskValue;

import com.google.common.collect.Lists;

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
		InnerRiskValue value = new InnerRiskValue();
		ClientScore score = new ClientScore();
		score.setDna_risk_score(33.);
		score.setDna_score(11.);
		value.clientScore = Lists.newArrayList(score);
		try {
			jsonGenerator.writeObject(value);
			baos.flush();
			System.out.println(baos.toString());
			baos.reset();
			jsonGenerator.writeObject(value);
			baos.flush();
			System.out.println(baos.toString());
			baos.reset();
			jsonGenerator.writeObject(value);
			baos.flush();
			System.out.println(baos.toString());
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
