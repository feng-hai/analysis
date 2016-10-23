/**  
* @Title: RedisState.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月25日 上午6:36:56
* @version V1.0.0  
*/
package com.wlwl.cube.redis;

import java.util.List;

import org.apache.storm.trident.state.State;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
 * @ClassName: RedisState
 * @Description: TODO更新redis的具体类
 * @author fenghai
 * @date 2016年9月25日 上午6:36:56
 *
 */
public class RedisState_U implements State {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.storm.trident.state.State#beginCommit(java.lang.Long)
	 */
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.storm.trident.state.State#commit(java.lang.Long)
	 */
	public void commit(Long txid) {
		// TODO Auto-generated method stub

	}

	public void setVehicleBulk(List<String> vehicleIDS, List<VehicleStatisticBean> vehicles) {
		
	}

	public List<VehicleStatisticBean> bulkGetVehicles(List<String> vehicleIDs) {
		
		return null;
	}

}
