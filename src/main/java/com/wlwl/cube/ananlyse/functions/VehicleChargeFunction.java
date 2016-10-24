/**  
* @Title: VehicleChargeFunction.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月12日 上午10:40:52
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.ChargeBean;
import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.ananlyse.state.TimeBaseRowStrategy;
import com.wlwl.cube.hbase.HBaseUtils;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;

/**
 * @ClassName: VehicleChargeFunction
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月12日 上午10:40:52
 *
 */
public class VehicleChargeFunction extends BaseFunction {

	/**
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	 */
	private static final long serialVersionUID = 6433822494043666537L;
	private static final String tableName = "DataAnalysis";
	private static final String tableName_Charge = "DATAANALYSIS_CHARGE";
	private static final String family_charge = "record";
	private static final String family = "count";
	private static final String CHARGERQUANTITY = "ChargeQuantity";
	private static final String CHARGECOUNT = "ChargeCount";
	/**
	 * @Fields PERFIX : TODO(用一句话描述这个变量表示什么)
	 */
	private static final String PERFIX = "DATAANALYSIS:";

	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;

	// private Map<String, VehicleStatisticBean> lastCharges = null;
	// private long lastTime;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
		// lastCharges = new HashMap<String, VehicleStatisticBean>();
		// lastTime = System.currentTimeMillis();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.storm.trident.operation.Function#execute(org.apache.storm.
	 * trident.tuple.TridentTuple,
	 * org.apache.storm.trident.operation.TridentCollector)
	 */
	public void execute(TridentTuple tuple, TridentCollector collector) {

		try {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");

			if (omok != null) {
				checkCharge(omok);
				updateChargeConfig(omok.getVehicle_UNID().getValue());
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	/**
	 * @Title: checkCharge
	 * @Description: 判断充电是否满足条件 如果满足返回true ，如果不满足返回false
	 * @param @param
	 *            vehicle
	 * @param @return
	 *            设定文件 @return Boolean 返回类型 @throws
	 */
	private Boolean isCharge(ObjectModelOfKafka vehicle) {
		// String status = vehicle.getChargeStatus();

		String vehicleCharge = Conf.VEHICLE_CONDITION_CHARGE + vehicle.getVehicle_UNID().getValue();
		// 從redis中獲取數據

		Map<String, String> map = util.hgetall(vehicleCharge);

		if (map.size() == 0) {

			// redis 中沒有數據，從數據庫中讀取並複製
			map = setRedis(vehicle.getVehicle_UNID().getValue());

			if (map.size() > 0) {
				util.hmset(vehicleCharge, map);
			}
		}
		Iterator<String> it = map.keySet().iterator();

		while (it.hasNext()) {

			String key;

			String valueStr;

			key = it.next().toString();

			valueStr = map.get(key);

			VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr, VehicleStatusBean.class);

			Pair pair = vehicle.getPairByCode(statusBean.getCODE());

			if (pair != null) {

				String value = pair.getValue();

				if (value != null && value != "") {

					Boolean isTrue = statusBean.checkStatus(value);

					return isTrue;

				}
			}
		}

		return false;

	}

	/**
	 * @Title: checkCharge
	 * @Description: 保存充电信息
	 * @param
	 * @param vehicle
	 *            设定文件 @return void 返回类型 @throws
	 */
	private void checkCharge(ObjectModelOfKafka vehicle) {

		String id = PERFIX + vehicle.getVehicle_UNID().getValue() + StateUntils.formateDay(vehicle.getTIMESTAMP());

		VehicleStatisticBean vehicleObj = new VehicleStatisticBean();

		vehicleObj.setVehicle_unid(vehicle.getVehicle_UNID().getValue());

		vehicleObj.setWorkTimeDateTime_end(vehicle.getTIMESTAMP());

		if (isCharge(vehicle)) {// 判断是否是充电状态

			if (vehicle.getInCharge() != null) {
				// 获取充电量
				Double chargeNum = Double.parseDouble(vehicle.getInCharge().getValue());
				ChargeBean cbean;
				if (chargeNum > 0) {

					String str = util.hget(id, "charges");

					if (str != null) {

						cbean = JsonUtils.deserialize(str, ChargeBean.class);

						cbean.setEndDate(vehicle.getTIMESTAMP());

						cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));

						cbean.setEndCharger(chargeNum);

					} else {

						cbean = new ChargeBean();

						cbean.setStartCharge(chargeNum);

						cbean.setEndCharger(chargeNum);

						cbean.setStartDate(vehicle.getTIMESTAMP());

						cbean.setEndDate(vehicle.getTIMESTAMP());

						cbean.setStartSOC(Double.parseDouble(vehicle.getSOC().getValue()));

						cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));
					}
					util.hset(id, "charges", JsonUtils.serialize(cbean));
				}
			}

		} else {

			String str = util.hget(id, "charges");

			if (str != null) {

				ChargeBean cbean = JsonUtils.deserialize(str, ChargeBean.class);

				if (!HBaseUtils.exists(tableName_Charge)) {
					HBaseUtils.createTable(tableName_Charge, family_charge);
				}
				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "startDate", StateUntils.formate(cbean.getStartDate()));

				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "endDate", StateUntils.formate(cbean.getEndDate()));

				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "startCharge", cbean.getStartCharge().toString());

				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "endCharge", cbean.getEndCharger().toString());

				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "Charge", Double.toString(cbean.getEndCharger() - cbean.getStartCharge()));

				HBaseUtils.insert(tableName_Charge,
						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "startSOC", cbean.getStartSOC().toString());
				HBaseUtils.insert(tableName_Charge,

						TimeBaseRowStrategy.getRowKeyForHase(vehicleObj) + cbean.getStartDate().getTime(),
						family_charge, "endSOC", cbean.getEndSOC().toString());

				String chargeQuantity = null;
				String chargeNumber = null;
				try {
					chargeQuantity = HBaseUtils.byGet(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj),
							family, CHARGERQUANTITY);
					chargeNumber = HBaseUtils.byGet(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
							CHARGECOUNT);
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (chargeNumber != null) {

					int num = (Integer.parseInt(chargeNumber) + 1);

					Double qNub = Double.parseDouble(chargeQuantity) + cbean.getEndCharger() - cbean.getStartCharge();

					HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family, CHARGECOUNT,
							Integer.toString(num));

					HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
							CHARGERQUANTITY, Double.toString(qNub));

				} else {

					HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family, CHARGECOUNT,
							Integer.toString(1));

					HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
							CHARGERQUANTITY, Double.toString(cbean.getEndCharger() - cbean.getStartCharge()));

				}
				util.hdel(id, "charges");
			}

		}
	}

	/**
	 * @Title: updateChargeConfig @Description: 更新充电配置 @param @param device
	 *         车辆唯一标识 @return void 返回类型 @throws
	 */
	private void updateChargeConfig(String device) {

		String timekey = Conf.STORM_TIMER + device;

		String timer = util.hget(timekey, Conf.ACTIVE_CHARGE_TIMER);

		if (timer != null) {

			Date date = StateUntils.strToDate(timer);

			if (date != null) {

				long m = new Date().getTime() - date.getTime();

				if (m > 1000 * 60 * 3) {

					util.hset(timekey, Conf.ACTIVE_CHARGE_TIMER, StateUntils.formate(new Date()));

					// 更新数据
					String vehicleStatus = Conf.VEHICLE_CONDITION_CHARGE + device;

					Map<String, String> map = setRedis(device);

					if (map.size() > 0) {

						util.del(vehicleStatus);

						util.hmset(vehicleStatus, map);
					}

				}
			} else {

				util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER, StateUntils.formate(new Date()));

			}

		} else {

			util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER, StateUntils.formate(new Date()));

		}

	}

	/**
	 * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private Map<String, String> setRedis(String vehicleUnid) {

		String id = Conf.PERFIX + vehicleUnid;

		String field = "fiber_unid";

		String sql = "SELECT code,option,value,VALUE_LAST ,status  FROM cube.PDA_VEHICLE_DETAIL where fiber_unid=? and type=1 and status=3";

		List<Object> params = new ArrayList<Object>();

		String fiber_unid = util.hget(id, field);

		params.add(fiber_unid);

		List<VehicleStatusBean> list = null;
		try {

			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Map<String, String> map = new HashMap<String, String>();

		for (VehicleStatusBean vsbean : list) {

			map.put(vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));

		}

		return map;

	}

}
