/**  
* @Title: storagerSingleton.java
* @Package com.wlwl.cuble.analyse.storager
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月19日 上午10:57:24
* @version V1.0.0  
*/ 
package com.wlwl.cuble.analyse.storager;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
* @ClassName: storagerSingleton
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月19日 上午10:57:24
*
*/
public class StoragerSingleton {
	
	private static RedisInstance<VehicleStatisticBean> redis=null;
	private StoragerSingleton()
	{
		
	}
	public static RedisInstance<VehicleStatisticBean> getInstance()
	{
		if(redis==null)
		{
			redis=new RedisInstance<VehicleStatisticBean>(VehicleStatisticBean.class);
		}
		return redis;	
	}
	
}
