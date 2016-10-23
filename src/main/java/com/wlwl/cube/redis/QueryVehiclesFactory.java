/**  
* @Title: QueryVehiclesFactory.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月25日 上午7:12:16
* @version V1.0.0  
*/ 
package com.wlwl.cube.redis;

import java.util.Map;

import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

/**
* @ClassName: QueryVehiclesFactory
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月25日 上午7:12:16
*
*/
public class QueryVehiclesFactory implements StateFactory {

	
	/**
	* @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	*/ 
	private static final long serialVersionUID = 2454858674673986171L;


	/* (non-Javadoc)
	 * @see org.apache.storm.trident.state.StateFactory#makeState(java.util.Map, org.apache.storm.task.IMetricsContext, int, int)
	 */
	@SuppressWarnings("rawtypes")
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		// TODO Auto-generated method stub
		return  new RedisState_U();
	}

}
