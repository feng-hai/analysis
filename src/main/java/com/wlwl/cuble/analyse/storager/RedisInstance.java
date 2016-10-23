/**  
* @Title: RedisInstance.java
* @Package com.wlwl.cuble.analyse.storager
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 上午9:31:09
* @version V1.0.0  
*/
package com.wlwl.cuble.analyse.storager;


import java.util.Map;

import com.wlwl.cube.ananlyse.state.BeanAndMap;
import com.wlwl.cube.ananlyse.state.JsonUtils;


import com.wlwl.cube.redis.RedisUtils;

/**
 * @ClassName: RedisInstance
 * @Description: TODO从Redis中获取和存储数据
 * @author fenghai
 * @date 2016年9月24日 上午9:31:09
 *
 */
public class RedisInstance<T> extends RedisUtils implements IStorager<T> {


	private Class<T> entity;
	public RedisInstance(Class<T> entity){
		
		this.entity=entity;
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wlwl.cuble.analyse.storager.Istorager#getStorager(java.lang.String)
	 */
	public T getStorager(String key) {
		try {
			String value = get(key);
			  
			return (T) JsonUtils.deserialize(value,entity);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wlwl.cuble.analyse.storager.Istorager#setStorager(java.lang.Object)
	 */
	public void setStorager(String  key, T obj) {
		
		try {
		    set(key,JsonUtils.serialize(obj));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see com.wlwl.cuble.analyse.storager.IStorager#ketExists(java.lang.String)
	 */
	public Boolean keyExists(String key) {
		// TODO Auto-generated method stub
		return exists(key);
	}
	
	public void deleteByKey(String key)
	{
		del(key);
	}
	
	

}
