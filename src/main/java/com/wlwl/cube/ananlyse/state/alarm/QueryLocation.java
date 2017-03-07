package com.wlwl.cube.ananlyse.state.alarm;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;

public class QueryLocation extends BaseQueryFunction<LocationDB, List<VehicleAlarmBean>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public List<List<VehicleAlarmBean>>  batchRetrieve(LocationDB state, List<TridentTuple> inputs) {
		List<ObjectModelOfKafka> omokList = new ArrayList<ObjectModelOfKafka>();
		for (TridentTuple input : inputs) {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) input.getValueByField("vehicle");
			omokList.add(omok);
		}
		return state.bulkGetLocations(omokList);
	}
	public void execute(TridentTuple tuple, List<VehicleAlarmBean> alarms, TridentCollector collector) {
		collector.emit(new Values(alarms));
	}
}