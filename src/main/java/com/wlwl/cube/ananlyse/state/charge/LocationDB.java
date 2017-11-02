package com.wlwl.cube.ananlyse.state.charge;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.storm.trident.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
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
import com.wlwl.cube.hbase.HBaseState;
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
	private Map<String, List<VehicleStatusBean>> statusData = null;
	private static final Logger LOG = LoggerFactory.getLogger(LocationDB.class);

	private static Map<String, String> fiberMap = new ConcurrentHashMap<>();
	private static Map<String, String> chargeTimer = new ConcurrentHashMap<>();
	private static Map<String, String> charge = new ConcurrentHashMap<>();

	private static Map<String, VehicleStatusBean> fiberCode = new ConcurrentHashMap<>();

	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	// private Map<String, VehicleStatisticBean> lastCharges = null;
	private long lastTime;

	private long removeTime;

	public LocationDB(Map<String, List<VehicleStatusBean>> statusData) {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
		// lastCharges = new HashMap<String, VehicleStatisticBean>();
		// lastTimeCharge = System.currentTimeMillis();
		this.statusData = statusData;
		lastTime = System.currentTimeMillis();
		removeTime = System.currentTimeMillis();
	}

	public void beginCommit(Long txid) {

	}

	public void commit(Long txid) {

	}

	public void setLocationsBulk(List<ObjectModelOfKafka> vehicleIds) {
		long currentTime = System.currentTimeMillis();

		if (currentTime - removeTime > 1000 * 60 * 60 * 24) {
			this.removeTime = currentTime;
			chargeTimer.clear();
			charge.clear();
		}

		for (ObjectModelOfKafka omok : vehicleIds) {
			try {
				checkCharge(omok);
				if (currentTime - lastTime > 1000 * 60 * 5) {
					this.lastTime = currentTime;
					loadData();
				}
			} catch (Exception ex) {
				LOG.error("setLocationsBulk", ex);
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
	 * @throws ParseException
	 */
	private void checkCharge(ObjectModelOfKafka vehicle) {
		try {

			if (vehicle.getVehicle_UNID() == null) {
				return;
			}
			VehicleStatisticBean vehicleObj = new VehicleStatisticBean();
			vehicleObj.setVehicle_unid(vehicle.getVehicle_UNID().getValue());
			// 保存到hbase中时间值
			// vehicleObj.setWorkTimeDateTime_end(vehicle.getTIMESTAMP());

			Date datetime = vehicle.getTIMESTAMP();
			// System.out.println(vehicle.getDATIME_RX());
			try {
				datetime = DEFAULT_DATE_SIMPLEDATEFORMAT.parse(vehicle.getDATIME_RX());
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				// e1.printStackTrace();
				System.out.println(vehicle.getDATIME_RX());
				Log.error("格式转换");
			}
			// redis 中保存充电状态的key
			String id = PERFIX + vehicle.getVehicle_UNID().getValue() + "charges";
			if (isCharge(vehicle)) {// 判断是否是充电状态
				// 是充电状态
				Double chargeNum = 0.0;
				// 保存最后一条数据，充电时间
				chargeTimer.put(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id, Long.toString(datetime.getTime()));
				// util.set(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id,
				// Long.toString(datetime.getTime()));
				// lastTime = System.currentTimeMillis();
				// 获取最后一条充电量
				chargeNum = Double.parseDouble(vehicle.getInCharge().getValue());

				// 获取充电量
				ChargeBean cbean;

				// 获取上一次保存的数据
				String str = charge.get(id);// util.hget(id, "charges");
				Log.info("充电字符串：" + id + ":" + str);
				// 该车正在充电
				if (str != null) {
					Log.info("继续充电" + id);
					// 获取上一条数据信息
					cbean = JsonUtils.deserialize(str, ChargeBean.class);
					// 更新最好一条充电时间
					cbean.setEndDate(datetime);
					if (Double.parseDouble(vehicle.getSOC().getValue()) > 0) {
						cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));
					}
					// 如果是开始充电量小于等于0，把结束电量赋值给开始充电量
					if (cbean.getStartCharge() <= 0) {
						cbean.setStartCharge(cbean.getEndCharger());
					}
					cbean.setEndCharger(chargeNum);

				} else {

					// System.out.println(id + "开始充电" + StateUntils.formate(new
					// Date()) + str);
					Log.info("开始充电：" + id);
					cbean = new ChargeBean();

					cbean.setStartCharge(chargeNum);

					cbean.setEndCharger(chargeNum);

					cbean.setStartDate(datetime);

					cbean.setEndDate(datetime);

					cbean.setStartSOC(Double.parseDouble(vehicle.getSOC().getValue()));

					cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));

					vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值

					if (!HBaseUtils.exists(tableName_Charge)) {
						HBaseUtils.createTable(tableName_Charge, family_charge);
					}
					String rowKey = TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj);
					HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startDate",
							StateUntils.formate(cbean.getStartDate()));

					HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startCharge",
							cbean.getStartCharge().toString());

					HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startSOC",
							cbean.getStartSOC().toString());

				}
				charge.put(id, JsonUtils.serialize(cbean));
				// util.hset(id, "charges", JsonUtils.serialize(cbean));

			} else {
				// 获取最后一条充电数据时间
				String time = chargeTimer.get(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);// util.get(Conf.STORM_TIMER
																								// +
																								// Conf.ACTIVE_CHARGE_TIMER
																								// +
																								// id);
				if (time == null || time.equals("")) {
					return;
				}
				Long lastTime = Long.parseLong(time);
				Long currentTime = datetime.getTime();
				// System.out.println((currentTime - lastTime)/60000);
				if (currentTime - lastTime > 1000 * 60 * 30) {

					String str = charge.get(id);// util.hget(id, "charges");
					// System.out.println(id + "结束充电-结束" +
					// StateUntils.formate(new
					// Date()) + str);

					if (str != null) {

						ChargeBean cbean = JsonUtils.deserialize(str, ChargeBean.class);

						vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值
						String rowKey = TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj);
						if (!HBaseUtils.exists(tableName_Charge)) {
							HBaseUtils.createTable(tableName_Charge, family_charge);
						}
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startDate",
								StateUntils.formate(cbean.getStartDate()));
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "endDate",
								StateUntils.formate(cbean.getEndDate()));
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startCharge",
								cbean.getStartCharge().toString());
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "endCharge",
								cbean.getEndCharger().toString());
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "Charge",
								Double.toString(cbean.getEndCharger() - cbean.getStartCharge()));
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "startSOC",
								cbean.getStartSOC().toString());
						HBaseUtils.insert(tableName_Charge, rowKey, family_charge, "endSOC",
								cbean.getEndSOC().toString());

						String chargeQuantity = null;
						String chargeNumber = null;
						try {
							chargeQuantity = HBaseUtils.byGet(tableName, rowKey, family, CHARGERQUANTITY);
							chargeNumber = HBaseUtils.byGet(tableName, rowKey, family, CHARGECOUNT);
						} catch (Exception e) {
							// e.printStackTrace();
							Log.error("", e);
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
						chargeTimer.remove(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);
						charge.remove(id);
						// util.hdel(id, "charges");
						// util.del(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER
						// + id);
					}
				}
			}
		} catch (Exception ex) {
			Log.error("错误", ex);
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
	private Boolean isCharge(ObjectModelOfKafka omok) {

		String id = Conf.PERFIX + omok.getVehicle_UNID().getValue();
		if (!fiberMap.containsKey(id)) {
			String fiber_unid = util.hget(id, "fiber_unid");
			if (fiber_unid != null && !fiber_unid.isEmpty()) {
				fiberMap.put(id, fiber_unid);
			}
		}
		String fiber_unid = fiberMap.get(id);// 获取数据字典信息
		if (fiber_unid == null || fiber_unid.isEmpty()) {
			return false;
		}
		if (this.fiberCode.containsKey(fiber_unid)) {
			VehicleStatusBean statusBean = this.fiberCode.get(fiber_unid);
			Pair pair = omok.getPairByCode(statusBean.getCODE());
			if (pair != null) {
				String value = pair.getValue();
				if (value != null && value != "") {
					Boolean isTrue = statusBean.checkStatus(value);
					if (isTrue) {
						return isTrue;
					}
				}
			}
			return false;
		}

		if (this.statusData.containsKey(fiber_unid)) {// 判断数据字典是否设置了充电信息
			// 根据数据字典获取自定义设置
			List<VehicleStatusBean> statusList = this.statusData.get(fiber_unid);

			for (VehicleStatusBean statusBean : statusList) {
				if (statusBean.getStatus() == 3) {
					this.fiberCode.put(fiber_unid, statusBean);
					Pair pair = omok.getPairByCode(statusBean.getCODE());

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

			}
		}
		return false;

	}

	// /**
	// * @Title: checkCharge
	// * @Description: 保存充电信息
	// * @param
	// * @param vehicle
	// * 设定文件 @return void 返回类型 @throws
	// * @throws ParseException
	// */
	// private void checkCharge(ObjectModelOfKafka vehicle) {
	//
	// if(vehicle.getVehicle_UNID()==null)
	// {
	// return;
	// }
	// VehicleStatisticBean vehicleObj = new VehicleStatisticBean();
	// vehicleObj.setVehicle_unid(vehicle.getVehicle_UNID().getValue());
	// // 保存到hbase中时间值
	// // vehicleObj.setWorkTimeDateTime_end(vehicle.getTIMESTAMP());
	//
	// Date datetime=vehicle.getTIMESTAMP();
	// //System.out.println(vehicle.getDATIME_RX());
	// try {
	// datetime = DEFAULT_DATE_SIMPLEDATEFORMAT.parse(vehicle.getDATIME_RX());
	// } catch (ParseException ex) {
	// // TODO Auto-generated catch block
	// //e1.printStackTrace();
	// LOG.error("setLocationsBulk",ex);
	// }
	//
	// // redis 中保存充电状态的key
	// String id = PERFIX + vehicle.getVehicle_UNID().getValue() + "charges";
	//
	// if (isCharge(vehicle)) {// 判断是否是充电状态
	//
	// System.out.println("充电状态");
	//
	// Double chargeNum = 0.0;
	//
	// util.set(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id,
	// Long.toString(datetime.getTime()));
	// // lastTime = System.currentTimeMillis();
	// chargeNum = Double.parseDouble(vehicle.getInCharge().getValue());
	//
	// // 获取充电量
	// ChargeBean cbean;
	//
	// // 获取上一次保存的数据
	// util.del(id);
	// String str = util.hget(id, "charges");
	//
	// if (str != null) {
	// System.out.println(id + "继续充电" + StateUntils.formate(new Date()) + str);
	// cbean = JsonUtils.deserialize(str, ChargeBean.class);
	//
	// cbean.setEndDate(datetime);
	//
	// if (Double.parseDouble(vehicle.getSOC().getValue()) > 0) {
	//
	// cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));
	//
	// if (cbean.getStartCharge() <= 0) {
	// cbean.setStartCharge(cbean.getEndCharger());
	// }
	// }
	//
	// cbean.setEndCharger(chargeNum);
	//
	// } else {
	//
	// System.out.println(id + "开始充电" + StateUntils.formate(new Date()) +
	// str+vehicle.getDeviceId());
	//
	// cbean = new ChargeBean();
	//
	// cbean.setStartCharge(chargeNum);
	//
	// cbean.setEndCharger(chargeNum);
	//
	// cbean.setStartDate(datetime);
	//
	// cbean.setEndDate(datetime);
	//
	// cbean.setStartSOC(Double.parseDouble(vehicle.getSOC().getValue()));
	//
	// cbean.setEndSOC(Double.parseDouble(vehicle.getSOC().getValue()));
	//
	// vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值
	//
	// if (!HBaseUtils.exists(tableName_Charge)) {
	// HBaseUtils.createTable(tableName_Charge, family_charge);
	// }
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
	// "startDate", StateUntils.formate(cbean.getStartDate()));
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
	// "startCharge", cbean.getStartCharge().toString());
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj), family_charge,
	// "startSOC", cbean.getStartSOC().toString());
	// System.out.println(id + "插入充电数据" + StateUntils.formate(new Date()) +
	// str+vehicle.getDeviceId());
	//
	// }
	// util.hset(id, "charges", JsonUtils.serialize(cbean));
	//
	// } else {
	//
	// String time = util.get(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);
	// if (time == null || time.equals("")) {
	// return;
	// }
	// Long lastTime = Long.parseLong(time);
	// Long currentTime = datetime.getTime();
	// // System.out.println((currentTime - lastTime)/60000);
	// if (currentTime - lastTime > 1000 * 60 * 30) {
	//
	// String str = util.hget(id, "charges");
	// System.out.println(id + "结束充电-结束" + StateUntils.formate(new Date()) +
	// str+vehicle.getDeviceId());
	//
	// if (str != null) {
	//
	// ChargeBean cbean = JsonUtils.deserialize(str, ChargeBean.class);
	//
	// vehicleObj.setWorkTimeDateTime_end(cbean.getStartDate());// 设置key值
	//
	// if (!HBaseUtils.exists(tableName_Charge)) {
	// HBaseUtils.createTable(tableName_Charge, family_charge);
	// }
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "startDate", StateUntils.formate(cbean.getStartDate()));
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "endDate", StateUntils.formate(cbean.getEndDate()));
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "startCharge", cbean.getStartCharge().toString());
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "endCharge", cbean.getEndCharger().toString());
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "Charge", Double.toString(cbean.getEndCharger() -
	// cbean.getStartCharge()));
	//
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "startSOC", cbean.getStartSOC().toString());
	// HBaseUtils.insert(tableName_Charge,
	// TimeBaseRowStrategy.getRowKeyFor2Hase(vehicleObj),
	// family_charge, "endSOC", cbean.getEndSOC().toString());
	//
	// String chargeQuantity = null;
	// String chargeNumber = null;
	// try {
	//
	// chargeQuantity = HBaseUtils.byGet(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj),
	// family, CHARGERQUANTITY);
	// chargeNumber = HBaseUtils.byGet(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj),
	// family, CHARGECOUNT);
	// } catch (Exception e) {
	// // e.printStackTrace();
	// LOG.error("",e);
	// }
	//
	// if (chargeNumber != null) {
	//
	// int num = (Integer.parseInt(chargeNumber) + 1);
	//
	// Double qNub = Double.parseDouble(chargeQuantity) + cbean.getEndCharger()
	// - cbean.getStartCharge();
	//
	// HBaseUtils.insert(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
	// CHARGECOUNT, Integer.toString(num));
	//
	// HBaseUtils.insert(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
	// CHARGERQUANTITY, Double.toString(qNub));
	//
	// } else {
	//
	// HBaseUtils.insert(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
	// CHARGECOUNT, Integer.toString(1));
	//
	// HBaseUtils.insert(tableName,
	// TimeBaseRowStrategy.getRowKeyForHase(vehicleObj), family,
	// CHARGERQUANTITY, Double.toString(cbean.getEndCharger() -
	// cbean.getStartCharge()));
	//
	// }
	// util.hdel(id, "charges");
	// util.del(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id);
	// }
	// }
	//
	// }
	// }
	//
	// /**
	// * @Title: checkCharge
	// * @Description: 判断充电是否满足条件 如果满足返回true ，如果不满足返回false
	// * @param @param
	// * vehicle
	// * @param @return
	// * 设定文件 @return Boolean 返回类型 @throws
	// */
	// private Boolean isCharge(ObjectModelOfKafka omok) {
	//
	// String id = Conf.PERFIX + omok.getVehicle_UNID().getValue();
	// String fiber_unid = util.hget(id, "fiber_unid");
	// if(fiber_unid==null||fiber_unid.isEmpty())
	// {
	// return false;
	// }
	// if (this.statusData.containsKey(fiber_unid)) {
	// List<VehicleStatusBean> statusList = this.statusData.get(fiber_unid);
	//
	// for (VehicleStatusBean statusBean : statusList) {
	// if (statusBean.getStatus() == 3) {
	// Pair pair = omok.getPairByCode(statusBean.getCODE());
	//
	// if (pair != null) {
	//
	// String value = pair.getValue();
	//
	// if (value != null && value != "") {
	//
	// Boolean isTrue = statusBean.checkStatus(value);
	// if (isTrue) {
	//
	// return isTrue;
	// }
	//
	// }
	// }
	// }
	//
	// }
	// }
	// return false;
	//
	// }

	// /**
	// *
	// * 加载数据库中数据，安装数据字典存储
	// */
	private void loadData() {
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME,fiber_unid  FROM  cube.PDA_CUSTOM_SETUP where type=1 and flag_del=0 and status=3 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		List<VehicleStatusBean> list = null;
		try {
			JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			LOG.error("setLocationsBulk", ex);
		}
		// Map<String, List<VehicleStatusBean>> map = new ConcurrentHashMap<>();
		// this.statusData.clear();
		if (list == null) {
			return;
		}
		Map<String, List<VehicleStatusBean>> beaMap = new ConcurrentHashMap<>();
		for (VehicleStatusBean vsbean : list) {
			if (!beaMap.containsKey(vsbean.getFIBER_UNID())) {
				List<VehicleStatusBean> temp = new ArrayList<VehicleStatusBean>();
				temp.add(vsbean);
				beaMap.put(vsbean.getFIBER_UNID(), temp);
			} else {
				List<VehicleStatusBean> temp = beaMap.get(vsbean.getFIBER_UNID());
				temp.add(vsbean);
				beaMap.replace(vsbean.getFIBER_UNID(), temp);
			}
		}
		this.statusData = beaMap;

	}

}
