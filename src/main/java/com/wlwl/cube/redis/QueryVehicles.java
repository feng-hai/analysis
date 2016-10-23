/**  
* @Title: QueryVehicles.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月25日 上午6:44:04
* @version V1.0.0  
*/
package com.wlwl.cube.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
 * @ClassName: QueryVehicles
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月25日 上午6:44:04
 *
 */
public class QueryVehicles extends BaseQueryFunction<RedisState_U, VehicleStatisticBean> {

	private static final long serialVersionUID = 1663675689853476303L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.state.QueryFunction#batchRetrieve(org.apache.
	 * storm.trident.state.State, java.util.List)
	 */
	public List<VehicleStatisticBean> batchRetrieve(RedisState_U state, List<TridentTuple> inputs) {
		List<String> vehicleIDs = new ArrayList<String>();
		for (TridentTuple input : inputs) {
			vehicleIDs.add(input.getString(0));
		}
		return state.bulkGetVehicles(vehicleIDs);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.state.QueryFunction#execute(org.apache.storm.
	 * trident.tuple.TridentTuple, java.lang.Object,
	 * org.apache.storm.trident.operation.TridentCollector)
	 */
	public void execute(TridentTuple tuple, VehicleStatisticBean result, TridentCollector collector) {
		collector.emit(new Values(result));

	}

}
