/**  
* @Title: PrientlnInfo.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月26日 下午8:12:39
* @version V1.0.0  
*/ 
package com.wlwl.cube.ananlyse.functions;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
* @ClassName: PrientlnInfo
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月26日 下午8:12:39
*
*/
public class PrientlnInfoFunction extends BaseFunction {


	private static final long serialVersionUID = -5315043430241922535L;

	/* (non-Javadoc)
	 * @see org.apache.storm.trident.operation.Function#execute(org.apache.storm.trident.tuple.TridentTuple, org.apache.storm.trident.operation.TridentCollector)
	 */
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		
		VehicleStatisticBean vehicleInfo=(VehicleStatisticBean)tuple.getValueByField("vehicle");
		System.out.println(vehicleInfo.toString());
	}

}
