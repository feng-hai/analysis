/**  
* @Title: MileCount.java
* @Package com.wlwl.cube.ananlyse.aggregator
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月26日 上午11:31:35
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.aggregator;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
 * @ClassName: MileCount
 * @Description: TODO里程统计
 * @author fenghai
 * @date 2016年9月26日 上午11:31:35
 *
 */
public class MileAggregator implements CombinerAggregator<VehicleStatisticBean> {

	private static final long serialVersionUID = 345525291893216637L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.operation.CombinerAggregator#init(org.apache.
	 * storm.trident.tuple.TridentTuple)
	 */
	public VehicleStatisticBean init(TridentTuple tuple) {
		VehicleStatisticBean example = new VehicleStatisticBean();
		// 获取里程值
		ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");
		System.out.println(omok);
		Pair totalMile = omok.getAllMile();
		if (totalMile != null) {
			String mile = totalMile.getValue();
			// 定义一个中间变量，
			// 1、判断是否是第一次，如果是第一次，为开始里程赋值
			// example.setWorkMile_start(Double.parseDouble(mile));
			// String deviceId=tuple.getStringByField("deviceId");
			// 根据deviceId更新redis数据
			// 设置当前的车辆的总里程
			System.out.println("init" + example);
			example.setWorkTotalMile(Double.parseDouble(mile));

			example.setStatisticDateTime(omok.getTIMESTAMP());
		}
		System.out.println("#######################################################");
		System.out.println(example);
		System.out.println("#######################################################");
		return example;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.operation.CombinerAggregator#combine(java.lang.
	 * Object, java.lang.Object)
	 */
	public VehicleStatisticBean combine(VehicleStatisticBean vehicleF, VehicleStatisticBean vehicleE) {
		if (vehicleF != null && vehicleE != null) {
			if (vehicleF.getWorkMile_start().equals(0.0)) {
				vehicleF.setWorkMile_start(vehicleF.getWorkTotalMile());
				vehicleF.setWorkTimeDateTime_end(vehicleF.getStatisticDateTime());
			}
			vehicleF.setWorkMile_end(vehicleE.getWorkTotalMile());
			vehicleF.setWorkTimeDateTime_end(vehicleF.getStatisticDateTime());
			vehicleF.setWorkMileCount(
					vehicleF.getWorkMileCount() + (vehicleE.getWorkTotalMile() - vehicleF.getWorkTotalMile()));
		}
		return vehicleF;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.storm.trident.operation.CombinerAggregator#zero()
	 */
	public VehicleStatisticBean zero() {
		// TODO Auto-generated method stub
		return null;
	}

}

//
// public class Count implements CombinerAggregator<Long> {
//
// @Override
// public Long init(TridentTuple tuple) {
// return 1L;
// }
//
// @Override
// public Long combine(Long val1, Long val2) {
// return val1 + val2;
// }
//
// @Override
// public Long zero() {
// return 0L;
// }
//
// }
