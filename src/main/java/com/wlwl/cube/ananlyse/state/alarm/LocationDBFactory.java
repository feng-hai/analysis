package com.wlwl.cube.ananlyse.state.alarm;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import com.wlwl.cube.analyse.bean.VehicleStatusBean;


public class LocationDBFactory implements StateFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

private Map<String, List<VehicleStatusBean>>  statusData;
	
	public LocationDBFactory()
	{
		
	}
//	public LocationDBFactory(Map<String, List<VehicleStatusBean>> map)
//	{
//		statusData=map;
//		
//	}
	@SuppressWarnings("rawtypes")
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		// TODO Auto-generated method stub
		return new LocationDB();

	}
}
