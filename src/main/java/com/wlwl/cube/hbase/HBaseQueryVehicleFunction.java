/**  
* @Title: HBaseQueryVehicleFunction.java
* @Package com.wlwl.cube.hbase
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月30日 上午11:16:34
* @version V1.0.0  
*/ 
package com.wlwl.cube.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
* @ClassName: HBaseQueryVehicleFunction
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月30日 上午11:16:34
*
*/
public class HBaseQueryVehicleFunction extends  BaseQueryFunction<HBaseState, VehicleStatisticBean>{


	private static final long serialVersionUID = 5520648691384620816L;

	/* (non-Javadoc)
	 * @see org.apache.storm.trident.state.QueryFunction#batchRetrieve(org.apache.storm.trident.state.State, java.util.List)
	 */
	public List<VehicleStatisticBean> batchRetrieve(HBaseState state, List<TridentTuple> inputs) {
		// TODO Auto-generated method stub
		  List<String> vehicleIDs = new ArrayList<String>();
	        for(TridentTuple input: inputs) {
	        	vehicleIDs.add(input.getStringByField("deviceId"));
	        }
	        return state.bulkGetVehicles(vehicleIDs);
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.trident.state.QueryFunction#execute(org.apache.storm.trident.tuple.TridentTuple, java.lang.Object, org.apache.storm.trident.operation.TridentCollector)
	 */
	public void execute(TridentTuple tuple, VehicleStatisticBean result, TridentCollector collector) {
		collector.emit(new Values(result));
		
	}



}
