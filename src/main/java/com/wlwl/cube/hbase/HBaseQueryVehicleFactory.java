/**  
* @Title: HBaseQueryVehicleFactory.java
* @Package com.wlwl.cube.hbase
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月30日 上午11:19:55
* @version V1.0.0  
*/ 
package com.wlwl.cube.hbase;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;



/**
* @ClassName: HBaseQueryVehicleFactory
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月30日 上午11:19:55
*
*/
public class HBaseQueryVehicleFactory implements StateFactory{


	private static final long serialVersionUID = 4742251168237786716L;

	/* (non-Javadoc)
	 * @see org.apache.storm.trident.state.StateFactory#makeState(java.util.Map, org.apache.storm.task.IMetricsContext, int, int)
	 */

	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		
				return new HBaseState();
	}

}
