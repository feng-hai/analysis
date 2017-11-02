package com.wlwl.cube.ananlyse.state.charge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;



public class LocationUpdater extends BaseStateUpdater<LocationDB> {  

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
  Map<String,ObjectModelOfKafka> omokList;
	private static final Logger log=LoggerFactory.getLogger(LocationUpdater.class);
	private long lastTime;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		lastTime = System.currentTimeMillis();
		omokList=  new ConcurrentHashMap<>();
	}

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {  
		long currentTime = System.currentTimeMillis();
		//List<ObjectModelOfKafka> omokList = new ArrayList<ObjectModelOfKafka>();
		for (TridentTuple input : tuples) {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) input.getValueByField("vehicle");
			if(omokList.containsKey(omok.getVehicle_UNID().getValue()))
			{
				omokList.replace(omok.getVehicle_UNID().getValue(), omok);
			}else
			{
				omokList.put(omok.getVehicle_UNID().getValue(), omok);
			}
		}
		if (currentTime - lastTime > 1000 * 60*2 ||omokList.size()>100) {
			log.info("提交充电"+omokList.size());
			lastTime=currentTime;
			List<ObjectModelOfKafka> omokList2= new ArrayList<ObjectModelOfKafka>();
			omokList2.addAll(omokList.values());
			omokList.clear();
			state.setLocationsBulk(omokList2);
		}
		//state.setLocationsBulk(omokList);
    }  
}
