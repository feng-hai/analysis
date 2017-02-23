/**  
* @Title: RedisUpdate.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月20日 下午10:36:08
* @version V1.0.0  
*/
package com.wlwl.cube.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;

/**
 * @ClassName: RedisUpdate
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月20日 下午10:36:08
 *
 */
public class VehicleAlarmUpdate extends BaseStateUpdater<VehicleAlarmState> {

	BlockingQueue<List<VehicleAlarmBean>> lastQueue;
	private long lastTime;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		lastTime = System.currentTimeMillis();
		lastQueue = new LinkedBlockingQueue<List<VehicleAlarmBean>>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.state.StateUpdater#updateState(org.apache.storm.
	 * trident.state.State, java.util.List,
	 * org.apache.storm.trident.operation.TridentCollector)
	 */
	@Override
	public void updateState(VehicleAlarmState state, List<TridentTuple> tuples, TridentCollector collector) {
		// TODO Auto-generated method stub
		long currentTime = System.currentTimeMillis();
		// List<VehicleStatisticBean> vehicles = new
		// ArrayList<VehicleStatisticBean>();
		for (TridentTuple t : tuples) {
			@SuppressWarnings("unchecked")
			List<VehicleAlarmBean> alarmList = (List<VehicleAlarmBean>) t.getValueByField("vehicleInfo");
			if (alarmList.size() > 0) {
				try {
					lastQueue.put(alarmList);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if (currentTime >= lastTime + 1000 * 60 * 5 || lastQueue.size() > 0) {
			lastTime = currentTime;
			while (lastQueue.size() > 0) {
				List<VehicleAlarmBean> alarmList;
				try {
					alarmList = lastQueue.take();
					state.setVehicleBulk(alarmList);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}

}
