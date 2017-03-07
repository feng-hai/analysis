package com.wlwl.cube.ananlyse.state.charge;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;

public class LocationUpdater extends BaseStateUpdater<LocationDB> {  

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void updateState(LocationDB state, List<TridentTuple> tuples, TridentCollector collector) {  

		List<ObjectModelOfKafka> omokList = new ArrayList<ObjectModelOfKafka>();
		for (TridentTuple input : tuples) {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) input.getValueByField("vehicle");
			omokList.add(omok);
		}
		state.setLocationsBulk(omokList);
    }  
}
