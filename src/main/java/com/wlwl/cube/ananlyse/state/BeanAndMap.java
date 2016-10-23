/**  
* @Title: BeanToMap.java
* @Package com.wlwl.cube.ananlyse.state
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月11日 下午5:00:59
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.state;


import java.lang.reflect.InvocationTargetException;

import java.util.Map;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;


public class BeanAndMap {

	public static Object map2Object(Map<String, Object> map, Class<?> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		if (map == null) {
			return null;
		}
		Object obj = clazz.newInstance();

		BeanUtils.populate(obj, map);

		return obj;
	}
	public static Object maps2Object(Map<String, String> map, Class<?> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		if (map == null) {
			return null;
		}
		Object obj = clazz.newInstance();

		BeanUtils.populate(obj, map);

		return obj;
	}
	public static Object stringMap2Object(Map<String, String[]> map, Class<?> clazz)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		if (map == null) {
			return null;
		}
		Object obj = clazz.newInstance();

		BeanUtils.populate(obj, map);

		return obj;
	}

	public static Map<?, ?> object2Map(Object obj) throws InstantiationException, IllegalAccessException {
		if (obj == null) {
			return null;
		}
		return new BeanMap(obj);
	}
}