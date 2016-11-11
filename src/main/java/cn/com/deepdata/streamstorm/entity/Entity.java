package cn.com.deepdata.streamstorm.entity;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

public interface Entity {

	public static Object getMap(Object value) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, IntrospectionException {
		Object ret = value;
		if (value instanceof Entity) {
			ret = parseFields(value);
		} else if (value instanceof Iterable) {
			List<Object> list = Lists.newArrayList();
			for (Object subObj : (Iterable<?>) value) {
				list.add(getMap(subObj));
			}
			ret = list;
		} else {
			ret = value;
		}
		return ret;
	}

	static Map<String, Object> parseFields(Object o) throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
		Map<String, Object> result = new HashMap<String, Object>();
		Field[] declaredFields = o.getClass().getFields();
		for (Field field : declaredFields) {
			Object value = field.get(o);
			result.put(field.getName(), getMap(value));
		}
		BeanInfo info = Introspector.getBeanInfo(o.getClass());
		for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
			if (pd.getName().equals("class"))
				continue;
			Method reader = pd.getReadMethod();
			if (reader != null) {
				Object value = reader.invoke(o);
				result.put(pd.getName(), getMap(value));
			}
		}
		return result;
	}
}
