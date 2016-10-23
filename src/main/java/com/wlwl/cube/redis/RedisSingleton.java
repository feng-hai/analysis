/**  
* @Title: RedisSingleton.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月19日 上午10:52:56
* @version V1.0.0  
*/ 
package com.wlwl.cube.redis;

/**
* @ClassName: RedisSingleton
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月19日 上午10:52:56
*
*/
public class RedisSingleton {
	
	private static RedisUtils redis=null;
	private RedisSingleton()
	{
		
	}
	
	public static RedisUtils instance()
	{
		if(redis==null)
		{
		 redis=	new RedisUtils();
		}
		return redis;
	}
}
