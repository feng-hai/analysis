/**  
* @Title: HBaseVehicleUpdate.java
* @Package com.wlwl.cube.hbase
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月30日 上午11:27:24
* @version V1.0.0  
*/
package com.wlwl.cube.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cuble.analyse.storager.IStorager;
import com.wlwl.cuble.analyse.storager.RedisInstance;

/**
 * @ClassName: HBaseVehicleUpdate
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @param <VehicleStatisticBean>
 * @date 2016年9月30日 上午11:27:24
 *
 */
public class HBaseVehicleUpdate extends BaseStateUpdater<HBaseState> {

	private Map<String, VehicleStatisticBean> lastVehicles = null;
	private long lastTime;
	private static final Logger log=LoggerFactory.getLogger(HBaseVehicleUpdate.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		lastVehicles = new ConcurrentHashMap <String, VehicleStatisticBean>();
		lastTime = System.currentTimeMillis();
	}

	private static final long serialVersionUID = -3960567570106445647L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.state.StateUpdater#updateState(org.apache.storm.
	 * trident.state.State, java.util.List,
	 * org.apache.storm.trident.operation.TridentCollector)
	 */
	
	public void updateState(HBaseState state, List<TridentTuple> tuples, TridentCollector collector) {
		long currentTime = System.currentTimeMillis();
		
		//Log.info("开始批量处理");
		// List<VehicleStatisticBean> vehicles = new
		try{
		for (TridentTuple t : tuples) {
			
			VehicleStatisticBean vehicle = (VehicleStatisticBean) t.getValueByField("vehicleInfo");
			
			String key =vehicle.getVehicle_unid();
			if (lastVehicles.containsKey(key)) {
				VehicleStatisticBean vehicle_temp=	lastVehicles.get(key);
				vehicle.setWorkTimeDateTime_temp(vehicle.getWorkTimeDateTime_temp()+vehicle_temp.getWorkTimeDateTime_temp());
				//vehicle.setWorkTimeDateTime_end(workTimeDateTime_end);
				lastVehicles.replace(key, vehicle);
			} else {
				lastVehicles.put(key, vehicle);
			}
			//log.info("分析时间："+vehicle.getVehicle_unid()+":"+StateUntils.formate( vehicle.getStatisticDateTime()));
			// vehicles.add(vehicle);
		}
		}catch(Exception e)
		{
			Log.error("错误",e);
		}

		if (currentTime >= lastTime + 1000 * 60 *5) {
			lastTime=currentTime;
			//Log.info("开始分析提交："+lastVehicles.size());
			List<VehicleStatisticBean> vehiclesM = new ArrayList<VehicleStatisticBean>();
			vehiclesM.addAll(lastVehicles.values());
			lastVehicles.clear();
			state.setVehicleBulk(vehiclesM);	
		}

	}

}
