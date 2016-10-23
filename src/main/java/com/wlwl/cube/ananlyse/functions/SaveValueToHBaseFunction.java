/**  
* @Title: SaveValueToHBase.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月29日 下午1:25:23
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.ananlyse.state.TimeBaseRowStrategy;
import com.wlwl.cube.hbase.HBaseUtils;

/**
 * @ClassName: SaveValueToHBase
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月29日 下午1:25:23
 *
 */
public class SaveValueToHBaseFunction extends BaseFunction {

	private static final String tableName = Conf.TABLENAME;
	private static final String family = Conf.FAMILY;
	private static final long serialVersionUID = 2694950601657725381L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.operation.Function#execute(org.apache.storm.
	 * trident.tuple.TridentTuple,
	 * org.apache.storm.trident.operation.TridentCollector)
	 */
	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		try{
		// TODO Auto-generated method stub
		if (!HBaseUtils.exists(tableName)) {
			HBaseUtils.createTable(tableName, family);
		}
		VehicleStatisticBean vehicle = (VehicleStatisticBean) tuple.getValueByField("countInfo");
		if (vehicle != null) {
			collector.emit(new Values(vehicle));

			insertDataForMile(vehicle);
			insertDataForEnergy(vehicle);
			insertDataForFule(vehicle);
			
			
//			java.util.Map<String, String> values = HBaseUtils.byGet(tableName,
//					TimeBaseRowStrategy.getRowKeyForHase(vehicle), family);
//			System.out.println("#########################hbase>>>>>>>>>>>>>"
//					+ TimeBaseRowStrategy.getRowKeyForHase(vehicle));
//			System.out.println(values);
		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
		  	e.printStackTrace();
		}

	}

	/**
	 * @Title: insertDataForMile @Description: TODO 里程更新 @param @param vehicle
	 * 设定文件 @return void 返回类型 @throws
	 */
	private void insertDataForMile(VehicleStatisticBean vehicle) {
		if (vehicle.getWorkMileCount() != null) {
			HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
					"mileCount", vehicle.getWorkMileCount().toString());
		}
		HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
				"mileTatol", vehicle.getWorkMile_end().toString());
		HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
				"workStart", StateUntils.formate(vehicle.getWorkTimeDateTime_start()));
		if(vehicle.getWorkTimeDateTime_end()!=null)
		{
		HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
				"workEnd",
				StateUntils.formate(vehicle.getWorkTimeDateTime_end()));
		}

	}

	/**
	 * @Title: insertDataForEnergy @Description: TODO 能耗更新 @param @param vehicle
	 * 设定文件 @return void 返回类型 @throws
	 */
	private void insertDataForEnergy(VehicleStatisticBean vehicle) {
		if (vehicle.getWorkEnergyCount() != null) {
			HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
					"energyCount", vehicle.getWorkEnergyCount().toString());
		}
		HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
				"energyTatol", vehicle.getWorkEnergy_end().toString());

	}
	

	/**
	 * @Title: insertDataForEnergy @Description: TODO 能耗更新 @param @param vehicle
	 * 设定文件 @return void 返回类型 @throws
	 */
	private void insertDataForFule(VehicleStatisticBean vehicle) {
		if (vehicle.getWorkFuleCount() != null) {
			HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
					"FuleCount", vehicle.getWorkFuleCount().toString());
		}
		HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicle), family,
				"FuleTatol", vehicle.getWorkFule_end().toString());

	}

}
