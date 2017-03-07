package com.wlwl.cube.ananlyse.state.alarm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

public class LocationUpdater extends BaseStateUpdater<LocationDB> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//	private long lastTime;
//	List<List<VehicleAlarmBean>> omokList;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		//lastTime = System.currentTimeMillis();
		
	}

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {
		//long currentTime = System.currentTimeMillis();
		List<List<VehicleAlarmBean>> omokList= new ArrayList<List<VehicleAlarmBean>>();
		for (TridentTuple input : tuples) {
			@SuppressWarnings("unchecked")
			List<VehicleAlarmBean> omok = (List<VehicleAlarmBean>) input.getValueByField("vehicleInfo");
			
			omokList.add(omok);
		}
		//if (currentTime - lastTime > 1000 * 60 * 5||omokList.size()>100) {
		//	lastTime=currentTime;
			state.setLocationsBulk(omokList);
		//	omokList.clear();
		//}
	}
}
