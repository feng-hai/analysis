/**  
* @Title: Istorager.java
* @Package com.wlwl.cuble.analyse.storager
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 上午9:23:13
* @version V1.0.0  
*/ 
package com.wlwl.cuble.analyse.storager;

/**
* @ClassName: Istorager
* @Description: TODO操作存储介质的接口
* @author fenghai
* @date 2016年9月24日 上午9:23:13
*
*/
/**
* @ClassName: Istorager
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月29日 上午10:01:59
*
* @param <T>
*/ 
public interface IStorager<T> {
	
	/**
	* @Title: getStorager
	* @Description: TODO从存储介质中获取数据
	* @param @param key
	* @param @return    
	* @return T    返回类型
	* @throws
	*/ 
	T getStorager(String key);
	/**
	* @Title: setStorager
	* @Description: TODO保存数据到存储介质
	* @param @param obj    数据对象
	* @return void    返回类型
	* @throws
	*/ 
	void setStorager(String key,T obj);
	
	/**
	* @Title: exists
	* @Description: TODO判断key是否存在
	* @param @param key
	* @param @return    设定文件
	* @return Boolean    返回类型
	* @throws
	*/ 
	Boolean keyExists(String key);
	
	
	void deleteByKey(String key);

}
