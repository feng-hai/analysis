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

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

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

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		lastVehicles = new HashMap<String, VehicleStatisticBean>();
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
	@SuppressWarnings("rawtypes")
	public void updateState(HBaseState state, List<TridentTuple> tuples, TridentCollector collector) {
		long currentTime = System.currentTimeMillis();
		// List<VehicleStatisticBean> vehicles = new
		// ArrayList<VehicleStatisticBean>();
		for (TridentTuple t : tuples) {
			VehicleStatisticBean vehicle = (VehicleStatisticBean) t.getValueByField("vehicleInfo");
			if (lastVehicles.containsKey(vehicle.getVehicle_unid())) {

				lastVehicles.replace(vehicle.getVehicle_unid()+StateUntils.formateDay(vehicle.getWorkTimeDateTime_end_t()), vehicle);
			} else {

				lastVehicles.put(vehicle.getVehicle_unid()+StateUntils.formateDay(vehicle.getWorkTimeDateTime_end_t()), vehicle);

			}
			// vehicles.add(vehicle);
		}

		if (currentTime >= lastTime + 1000 * 60 *5) {
			lastTime=currentTime;

			List<VehicleStatisticBean> vehiclesM = new ArrayList<VehicleStatisticBean>();
			Iterator iter = lastVehicles.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				Object val = entry.getValue();
				vehiclesM.add((VehicleStatisticBean) val);
			}
			lastVehicles.clear();
			state.setVehicleBulk(vehiclesM);	
		}

	}

}
