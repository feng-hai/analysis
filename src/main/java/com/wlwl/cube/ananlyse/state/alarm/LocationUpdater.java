package com.wlwl.cube.ananlyse.state.alarm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

public class LocationUpdater extends BaseStateUpdater<LocationDB> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//private long lastTime;
	
	private static final Logger log=LoggerFactory.getLogger(LocationUpdater.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		//lastTime = System.currentTimeMillis();
		//omokList=  Collections.synchronizedList(new ArrayList<List<VehicleAlarmBean>>());
	}

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {
		//long currentTime = System.currentTimeMillis();
		List<List<VehicleAlarmBean>> omokList=new ArrayList<>();
		for (TridentTuple input : tuples) {
			@SuppressWarnings("unchecked")
			List<VehicleAlarmBean> omok = (List<VehicleAlarmBean>) input.getValueByField("vehicleInfo");
			if(omok.size()>0)
			{
			 omokList.add(omok);
			}
		}
		state.setLocationsBulk(omokList);
//		if (currentTime - lastTime > 1000 * 60*2 ||omokList.size()>100) {
//			log.info("提交"+omokList.size());
//			lastTime=currentTime;
//			List<List<VehicleAlarmBean>> omokList2= new ArrayList<List<VehicleAlarmBean>>();
//			omokList2.addAll(omokList);
//			omokList.clear();
			
		//}
	}
}
