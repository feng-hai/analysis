package com.wlwl.cube.ananlyse.state.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;


public class LocationUpdater extends BaseStateUpdater<LocationDB> {  

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//private Long lastTime;

//private static final Logger log=LoggerFactory.getLogger(LocationUpdater.class);

@Override
public void prepare(Map conf, TridentOperationContext context) {
	
	//lastTime = System.currentTimeMillis();
}

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {  
		
		List<ObjectModelOfKafka> omokList = new ArrayList<ObjectModelOfKafka>();	
		for (TridentTuple input : tuples) {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) input.getValueByField("vehicle");
			omokList.add(omok);
			
		}
		state.setLocationsBulk(omokList);
		
    }  
}
