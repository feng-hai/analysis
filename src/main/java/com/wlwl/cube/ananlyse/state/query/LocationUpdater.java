package com.wlwl.cube.ananlyse.state.query;

import java.util.ArrayList;
import java.util.Date;
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
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.ananlyse.state.StateUntils;

public class LocationUpdater extends BaseStateUpdater<LocationDB> {  

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
private Long lastTime;
private Map <String, ObjectModelOfKafka> omokMap;
private static final Logger log=LoggerFactory.getLogger(LocationUpdater.class);

@Override
public void prepare(Map conf, TridentOperationContext context) {
	omokMap = new ConcurrentHashMap <String, ObjectModelOfKafka>();
	lastTime = System.currentTimeMillis();
}

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {  
		
		
		long currentTime = System.currentTimeMillis();
		
		for (TridentTuple input : tuples) {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) input.getValueByField("vehicle");
			//log.info("车辆数据"+omok.getVehicle_UNID()+":"+StateUntils.formate(omok.getTIMESTAMP()));
			if(!omokMap.containsKey(omok.getVehicle_UNID().getValue()))
			{
				omokMap.put(omok.getVehicle_UNID().getValue(), omok);
			}else
			{
				omokMap.replace(omok.getVehicle_UNID().getValue(), omok);
			}
			
		}
		if(currentTime-lastTime>1000*60*1)
		{
			lastTime=currentTime;
			List<ObjectModelOfKafka> omokList = new ArrayList<ObjectModelOfKafka>();	
			omokList.addAll(omokMap.values());
			omokMap.clear();
			state.setLocationsBulk(omokList);
		}
		
    }  
}
