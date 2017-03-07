package com.wlwl.cube.ananlyse.state.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class QueryLocation extends BaseQueryFunction<LocationDB, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public List<String> batchRetrieve(LocationDB state, List<TridentTuple> inputs) {
		List<Long> userIds = new ArrayList<Long>();
		for (TridentTuple input : inputs) {
			userIds.add(input.getLong(0));
		}
		return state.bulkGetLocations(userIds);
	}

	public void execute(TridentTuple tuple, String location, TridentCollector collector) {
		collector.emit(new Values(location));
	}
}