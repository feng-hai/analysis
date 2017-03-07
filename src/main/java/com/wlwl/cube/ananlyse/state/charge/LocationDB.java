package com.wlwl.cube.ananlyse.state.charge;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlwl.cube.analyse.bean.ChargeBean;
import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.functions.VehicleStatusFunction;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.ananlyse.state.TimeBaseRowStrategy;
import com.wlwl.cube.hbase.HBaseUtils;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;

public class LocationDB implements State {

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
	private long lastTimeCharge;

	public LocationDB() {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
		// lastCharges = new HashMap<String, VehicleStatisticBean>();
		lastTimeCharge = System.currentTimeMillis();
	}

	public void beginCommit(Long txid) {

	}

	public void commit(Long txid) {

	}

	public void setLocationsBulk(List<ObjectModelOfKafka> vehicleIds) {

		for (ObjectModelOfKafka omok : vehicleIds) {
			try {
				checkCharge(omok);
				//updateChargeConfig();
			} catch (Exception ex) {
				System.out.println(ex.getMessage());
			}

		}

	}

	public List<String> bulkGetLocations(List<Long> userIds) {
		return null;
	}

	/**
	 * @Title: checkCharge
	 * @Description: 保存充电信息
	 * @param
	 * @param vehicle
	 *            设定文件 @return void 返回类型 @throws
	 */
	private void checkCharge(ObjectModelOfKafka vehicle) {

		VehicleStatisticBean vehicleObj = new VehicleStatisticBean();
		vehicleObj.setVehicle_unid(vehicle.getVehicle_UNID().getValue());
		// 保存到hbase中时间值
		// vehicleObj.setWorkTimeDateTime_end(vehicle.getTIMESTAMP());

		// redis 中保存充电状态的key
		String id = PERFIX + vehicle.getVehicle_UNID().getValue() + "charge";

		if (isCharge(vehicle)) {// 判断是否是充电状态

			Double chargeNum = 0.0;

			util.set(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id, Long.toString(System.currentTimeMillis()));
			// lastTime = System.currentTimeMillis();
			chargeNum = Double.parseDouble(vehicle.getInCharge().getValue());

			// 获取充电量
			ChargeBean cbean;

			// 获取上一次保存的数据
			String str = util.hget(id, "charges");

			if (str != null) {
				// System.out.println(id + "继续充电" + StateUntils.formate(new
				// Date()) + str);
				cbean = JsonUtils.deserialize(str, ChargeBean.class);

				cbean.setEndDate(vehicle.getTIMESTAMP());

				if (Double.parseDouble(vehicle.getSOC().getValue()) > 0) {

					cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));

					if (cbean.getStartCharge() <= 0) {
						cbean.setStartCharge(cbean.getEndCharger());
					}
				}

				cbean.setEndCharger(chargeNum);

			} else {

				// System.out.println(id + "开始充电" + StateUntils.formate(new
				// Date()) + str);

				cbean = new ChargeBean();

				cbean.setStartCharge(chargeNum);

				cbean.setEndCharger(chargeNum);

				cbean.setStartDate(vehicle.getTIMESTAMP());

				cbean.setEndDate(vehicle.getTIMESTAMP());

				cbean.setStartSOC(Double.parseDouble(vehicle.getSOC().getValue()));

				cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));

				vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值

				if (!HBaseUtils.exists(tableName_Charge)) {
					HBaseUtils.createTable(tableName_Charge, family_charge);
				}

				HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
						"startDate", StateUntils.formate(cbean.getStartDate()));

				HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
						"startCharge", cbean.getStartCharge().toString());

				HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
						"startSOC", cbean.getStartSOC().toString());

			}
			util.hset(id, "charges", JsonUtils.serialize(cbean));

		} else {

			String time = util.get(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);
			if (time == null || time.equals("")) {
				return;
			}
			Long lastTime = Long.parseLong(time);
			Long currentTime = System.currentTimeMillis();

			// System.out.println((currentTime - lastTime)/60000);
			if (currentTime - lastTime > 1000 * 60 * 5) {

				String str = util.hget(id, "charges");
				// System.out.println(id + "结束充电-结束" + StateUntils.formate(new
				// Date()) + str);

				if (str != null) {

					ChargeBean cbean = JsonUtils.deserialize(str, ChargeBean.class);

					vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值

					if (!HBaseUtils.exists(tableName_Charge)) {
						HBaseUtils.createTable(tableName_Charge, family_charge);
					}
					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "startDate", StateUntils.formate(cbean.getStartDate()));

					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "endDate", StateUntils.formate(cbean.getEndDate()));

					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "startCharge", cbean.getStartCharge().toString());

					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "endCharge", cbean.getEndCharger().toString());

					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "Charge", Double.toString(cbean.getEndCharger() - cbean.getStartCharge()));

					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "startSOC", cbean.getStartSOC().toString());
					HBaseUtils.insert(tableName_Charge, TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
							family_charge, "endSOC", cbean.getEndSOC().toString());

					String chargeQuantity = null;
					String chargeNumber = null;
					try {

						chargeQuantity = HBaseUtils.byGet(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj),
								family, CHARGERQUANTITY);
						chargeNumber = HBaseUtils.byGet(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj),
								family, CHARGECOUNT);
					} catch (Exception e) {
						// e.printStackTrace();
					}

					if (chargeNumber != null) {

						int num = (Integer.parseInt(chargeNumber) + 1);

						Double qNub = Double.parseDouble(chargeQuantity) + cbean.getEndCharger()
								- cbean.getStartCharge();

						HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
								CHARGECOUNT, Integer.toString(num));

						HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
								CHARGERQUANTITY, Double.toString(qNub));

					} else {

						HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
								CHARGECOUNT, Integer.toString(1));

						HBaseUtils.insert(tableName, TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
								CHARGERQUANTITY, Double.toString(cbean.getEndCharger() - cbean.getStartCharge()));

					}
					util.hdel(id, "charges");
					util.del(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);
				}
			}

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

		String vehicleCharge = Conf.VEHICLE_CONDITION_CHARGE + "config";// vehicle.getVehicle_UNID().getValue();
		// 從redis中獲取數據

		Map<String, String> map = util.hgetall(vehicleCharge);

		if (map.size() == 0) {

			// redis 中沒有數據，從數據庫中讀取並複製
			map = setRedis();

			if (map.size() > 0) {
				util.hmset(vehicleCharge, map);
			}
		}
		// System.out.println(map);

		String id = Conf.PERFIX + vehicle.getVehicle_UNID().getValue();

		String fiber_unid = util.hget(id, "fiber_unid");

		String valueStr = map.get(fiber_unid + "3");

		// System.out.println(valueStr);

		if (valueStr != null && valueStr != "") {

			VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr, VehicleStatusBean.class);

			Pair pair = vehicle.getPairByCode(statusBean.getCODE());

			if (pair != null) {

				String value = pair.getValue();

				if (value != null && value != "") {

					Boolean isTrue = statusBean.checkStatus(value);
					if (isTrue) {

						return isTrue;
					}

				}
			}
		}
		return false;

	}

	/**
	 * @Title: updateChargeConfig @Description: 更新充电配置 @param @param device
	 *         车辆唯一标识 @return void 返回类型 @throws
	 */
	private void updateChargeConfig() {

		long currentTime = System.currentTimeMillis();
		if (currentTime - lastTimeCharge > 1000 * 60 * 30) {
			lastTimeCharge = currentTime;
			String vehicleAlarm = Conf.VEHICLE_CONDITION_CHARGE + "config";

			Map<String, String> map = setRedis();

			if (map.size() > 0) {

				util.del(vehicleAlarm);

				util.hmset(vehicleAlarm, map);
			}

		}

	}

	/**
	 * @return
	 * @Title: setRedis
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param 设定文件
	 * @return void 返回类型 @throws
	 */
	private Map<String, String> setRedis() {

		String sql = "SELECT code,option,value,VALUE_LAST ,status,FIBER_UNID  FROM cube.PDA_VEHICLE_DETAIL where type=1 and status=3 and flag_del=0 order by INX desc";

		List<Object> params = new ArrayList<Object>();

		List<VehicleStatusBean> list = null;
		try {

			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Map<String, String> map = new HashMap<String, String>();
		for (VehicleStatusBean vsbean : list) {
			map.put(vsbean.getFIBER_UNID() + vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));
		}
		return map;
	}

}
