package com.wlwl.cube.ananlyse.state.query;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
	// private Map<String, VehicleStatisticBean> lastCharges = null;
	private long lastTime;

	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private static final Logger LOG = LoggerFactory.getLogger(VehicleStatusFunction.class);
	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Map<String, List<VehicleStatusBean>> statusData = null;

	public LocationDB() {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
		lastTime = System.currentTimeMillis();
	}

	public LocationDB(Map<String, List<VehicleStatusBean>> map) {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
		this.statusData = map;
		lastTime = System.currentTimeMillis();
	}

	public void beginCommit(Long txid) {

	}

	public void commit(Long txid) {

	}

	public void setLocationsBulk(List<ObjectModelOfKafka> vehicleIds) {
		long currentTime = System.currentTimeMillis();
		for (ObjectModelOfKafka omok : vehicleIds) {
			if (omok == null) {
				continue;
			}
			// 更新车辆在线状态
			try {

				updateVehicleSatusNew(omok);
				// updateVehicleStatus(omok, omok.getVehicle_UNID().getValue());
				// 定时更新在线状态
				checkCharge(omok);
				updateNoOnline();
				if (currentTime - lastTime > 1000 * 60 *5) {
					this.lastTime = currentTime;
					loadData();
				}

			} catch (Exception ex) {
				System.out.println(ex.getMessage());
			}
		}

		
	}

	public List<String> bulkGetLocations(List<Long> userIds) {
		return null;
	}

	// /**
	// * @Title: updateCondition @Description: TODO固定的时间更新一下判断条件 @param
	// * 设定文件 @return void 返回类型 @throws
	// */
	// private void updateCondition(String device) {
	//
	// String timekey = Conf.STORM_TIMER + "status" + device;
	// String timer = util.hget(timekey, Conf.ACTIVE_CONDITION_TIMER +
	// "status");
	// if (timer != null) {
	// Date date = StateUntils.strToDate(timer);
	// if (date != null) {
	// long m = new Date().getTime() - date.getTime();
	// if (m > 1000 * 60 * 30) {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "status",
	// StateUntils.formate(new Date()));
	// // 更新数据
	// String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "status" + device;
	// Map<String, String> map = setRedis(device);
	// if (map.size() > 0) {
	// util.hmset(vehicleStatus, map);
	// }
	// }
	// } else {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "status",
	// StateUntils.formate(new Date()));
	// }
	//
	// } else {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "status",
	// StateUntils.formate(new Date()));
	// }
	//
	// }

	/**
	 * 检查5分钟没有上线的车辆，并把车辆设置为离线状态
	 */
	private void updateNoOnline() {

		String timekey = Conf.STORM_TIMER + "status" + "ONLINETIER";
		String timer = util.hget(timekey, Conf.ACTIVE_ONLINE_TIMER + "status");
		if (timer != null) {
			Date date = StateUntils.strToDate(timer);
			if (date != null) {
				long m = new Date().getTime() - date.getTime();
				if (m > 1000 * 60 * 5) {

					util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status", StateUntils.formate(new Date()));
					// 更新数据
					checkOnLine();

				}
			} else {
				// checkOnLine();
				util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status", StateUntils.formate(new Date()));
			}

		} else {
			// checkOnLine();
			util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status", StateUntils.formate(new Date()));
		}

	}

	/**
	 * @Title: checkOnLine @Description:
	 *         TODO长时间不更新的数据，状态更新为离线，此方法定时运行，五分钟一次 @param 设定文件 @return void
	 *         返回类型 @throws
	 */
	public void checkOnLine() {

		Set<String> set = util.keys(Conf.PERFIX + "*");
		for (String str : set) {
			// String status = util.hget(str, Conf.ACTIVE_STATUS);
			// if (status != null && status != "0") {
			if(str==null)
			{
				continue;
			}
			String time = util.hget(str, Conf.DATIME_RX);
			String status = util.hget(str, Conf.ACTIVE_STATUS);
			if (status == null || !status.equals("0")) {
				if (time != null) {// 如果时间存在
					if (new Date().getTime() - StateUntils.strToDate(time).getTime() > 1000 * 60 * 1) {
						util.hset(str, Conf.ACTIVE_STATUS, "0");
						String unid = util.hget(str, "unid");
						alertEnd(unid);
					}
				} else {// 如果时间不存在
					util.hset(str, Conf.ACTIVE_STATUS, "0");
					String unid = util.hget(str, "unid");
					alertEnd(unid);
				}
			}
			// }
		}
		set = null;
	}

	private static final String aiid_key = "ALARM_AIID:";

	private void alertEnd(String vehicleUnid) {
		Map<String, String> result = util.hgetall(aiid_key + vehicleUnid);
		if(result==null)
		{
			return;
		}
		for (String key : result.keySet()) {
			if(key==null)
			{
				continue;
			}
			if(!result.containsKey(key)||!result.containsKey(key + "beginTime"))
			{
				continue;
			}
			String aiid = result.get(key);
			String dateStr = result.get(key + "beginTime");
			if (aiid != null &&!aiid.isEmpty()&& dateStr != null&&!dateStr.isEmpty()) {
				
				StringBuilder update = new StringBuilder();
				update.append("CALL `sensor`.`offAlarmting`('");
				update.append(aiid);
				update.append("', '");
				update.append(StateUntils.formate(new Date()));
				update.append("', '");
				update.append(dateStr);
				update.append("')");
				try {
					jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		util.del(aiid_key + vehicleUnid);
	}

	private void updateVehicleSatusNew(ObjectModelOfKafka omok) {
		if (omok.getVehicle_UNID() == null) {
			return;
		}
		String vehicleUnid = omok.getVehicle_UNID().getValue();
		String timekey = Conf.STORM_TIMER + "status" + "ONLINETIERDEFAULT";
		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String fiber_unid = util.hget(id, field);
		if (fiber_unid == null) {
			System.out.println("车辆数据字典为空："+id);
			return;
		}
		Boolean isMatch = false;
		if (this.statusData.containsKey(fiber_unid)) {
			List<VehicleStatusBean> statusList = this.statusData.get(fiber_unid);
			for (VehicleStatusBean statusBean : statusList) {
				String code = statusBean.getCODE();
				Integer status = statusBean.getStatus();
				Pair pair = omok.getPairByCode(code);
				if (pair == null) {
					continue;
				}
				String value = pair.getValue();
				if (value == null) {
					continue;
				}
				Boolean isTrue = statusBean.checkStatus(value);
				if (isTrue&&status!=0) {
					isMatch = true;
					util.hset(id, Conf.ACTIVE_STATUS, String.valueOf(status));
					// util.hset(id, Conf.DATIME_RX,
					// omok.getDATIME_RX());
					util.hset(timekey, "default", StateUntils.formate(new Date()));
					break;

				}

			}
		}
		if (!isMatch)// 设置上线默认值
		{
			String timer = util.hget(timekey, "default");
			if (timer != null) {
				Date date = StateUntils.strToDate(timer);
				if (date != null) {
					long m = new Date().getTime() - date.getTime();
					if (m > 1000 * 60) {
						util.hset(id, Conf.ACTIVE_STATUS, "2");
						//util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
					}
				}
			} else {
				util.hset(timekey, "default", StateUntils.formate(new Date()));
			}
			String currentStatus = util.hget(id, Conf.ACTIVE_STATUS);
			if (currentStatus == null || (currentStatus != null && currentStatus.equals("0"))) {
				util.hset(id, Conf.ACTIVE_STATUS, "2");
			}
		}
	}

	// /**
	// * @param omok
	// * 读取kafka的实体类对象
	// * @param device
	// * 车辆唯一标识 vehicle_unid
	// */
	// private void updateVehicleStatus(ObjectModelOfKafka omok, String device)
	// {
	//
	// String timekey = Conf.STORM_TIMER + "status" + "ONLINETIERDEFAULT";
	// String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "status" + device;
	//
	// String id = Conf.PERFIX + device;
	//
	// // 需要删除
	// // util.del(Conf.VEHICLE_CONDITION_STATUS+"*");
	// // util.del(vehicleStatus);
	// // 從redis中獲取數據
	//
	// Map<String, String> map = util.hgetall(vehicleStatus);
	// // 判斷redis中是否有數據
	// if (map == null || map.size() == 0) {
	//
	// return;
	// // redis 中沒有數據，從數據庫中讀取並複製
	// // map = setRedis(device);
	// // if (map.size() > 0) {
	// // util.hmset(vehicleStatus, map);
	// // }
	// }
	//
	// Boolean isMatch = false;
	//
	// try {
	// Iterator<String> it = map.keySet().iterator();
	// while (it.hasNext()) {
	// String key;
	// String valueStr;
	// key = it.next().toString();
	// valueStr = map.get(key);
	// VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr,
	// VehicleStatusBean.class);
	//
	// if (statusBean != null) {
	// // 根据key获取数据值
	// Pair pair = omok.getPairByCode(statusBean.getCODE());
	// if (pair != null) {
	// String value = pair.getValue();
	// if (value != null) {
	// Boolean isTrue = statusBean.checkStatus(value);
	// if (isTrue) {
	// isMatch = true;
	// util.hset(id, Conf.ACTIVE_STATUS, key);
	// util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
	// util.hset(timekey, "default", StateUntils.formate(new Date()));
	// break;
	// }
	// }
	// }
	// }
	// }
	// } catch (Exception ex) {
	// ex.printStackTrace();
	// util.del(vehicleStatus);
	// }
	// if (!isMatch)// 设置上线默认值
	// {
	// String timer = util.hget(timekey, "default");
	// if (timer != null) {
	// Date date = StateUntils.strToDate(timer);
	// if (date != null) {
	// long m = new Date().getTime() - date.getTime();
	// if (m > 1000 * 60) {
	// util.hset(id, Conf.ACTIVE_STATUS, "2");
	// util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
	// }
	// }
	//
	// } else {
	// util.hset(timekey, "default", StateUntils.formate(new Date()));
	// }
	// String currentStatus = util.hget(id, Conf.ACTIVE_STATUS);
	// if (currentStatus == null || (currentStatus != null &&
	// currentStatus.equals("0"))) {
	// util.hset(id, Conf.ACTIVE_STATUS, "2");
	// }
	// isMatch = false;
	// }
	// }

	// /**
	// * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	// * 设定文件 @return void 返回类型 @throws
	// */
	// private Map<String, String> setRedis(String vehicleUnid) {
	//
	// String id = Conf.PERFIX + vehicleUnid;
	// String field = "fiber_unid";
	// String sql = "SELECT code,option,value,VALUE_LAST ,status FROM
	// cube.PDA_CUSTOM_SETUP where fiber_unid=? and type=1 and flag_del=0 order
	// by INX desc";
	// List<Object> params = new ArrayList<Object>();
	//
	// String fiber_unid = util.hget(id, field);
	// // LOG.error("数据字典id" + fiber_unid);
	// params.add(fiber_unid);
	// List<VehicleStatusBean> list = new ArrayList<VehicleStatusBean>();
	// if (fiber_unid != null) {
	// try {
	// jdbcUtils = SingletonJDBC.getJDBC();
	// list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params,
	// VehicleStatusBean.class);
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// Map<String, String> map = new ConcurrentHashMap<String, String>();
	//
	// for (VehicleStatusBean vsbean : list) {
	// map.put(vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));
	// }
	// return map;
	//
	// }

	/**
	 * @Title: checkCharge
	 * @Description: 保存充电信息
	 * @param
	 * @param vehicle
	 *            设定文件 @return void 返回类型 @throws
	 * @throws ParseException 
	 */
	private void checkCharge(ObjectModelOfKafka vehicle) {

		if(vehicle.getVehicle_UNID()==null)
		{
			return;
		}
		VehicleStatisticBean vehicleObj = new VehicleStatisticBean();
		vehicleObj.setVehicle_unid(vehicle.getVehicle_UNID().getValue());
		// 保存到hbase中时间值
		// vehicleObj.setWorkTimeDateTime_end(vehicle.getTIMESTAMP());
		
		Date datetime=vehicle.getTIMESTAMP();
		//System.out.println(vehicle.getDATIME_RX());
		try {
			datetime = DEFAULT_DATE_SIMPLEDATEFORMAT.parse(vehicle.getDATIME_RX());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
			System.out.println(vehicle.getDATIME_RX());
		}

		// redis 中保存充电状态的key
		String id = PERFIX + vehicle.getVehicle_UNID().getValue() + "charge";

		if (isCharge(vehicle)) {// 判断是否是充电状态

			Double chargeNum = 0.0;

			util.set(Conf.STORM_TIMER + Conf.ACTIVE_CHARGE_TIMER + id, Long.toString(datetime.getTime()));
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

				cbean.setEndDate(datetime);

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

				cbean.setStartDate(datetime);

				cbean.setEndDate(datetime);

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
			Long currentTime = datetime.getTime();
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
	private Boolean isCharge(ObjectModelOfKafka omok) {

		String id = Conf.PERFIX + omok.getVehicle_UNID().getValue();
		String fiber_unid = util.hget(id, "fiber_unid");
		if(fiber_unid==null||fiber_unid.isEmpty())
		{
			return false;
		}
		if (this.statusData.containsKey(fiber_unid)) {
			List<VehicleStatusBean> statusList = this.statusData.get(fiber_unid);
			
			for (VehicleStatusBean statusBean : statusList) {
				if (statusBean.getStatus() == 3) {
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
	// *
	// * 加载数据库中数据，安装数据字典存储
	// */
	private void loadData() {
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME,fiber_unid  FROM  cube.PDA_CUSTOM_SETUP where type=1 and flag_del=0 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		List<VehicleStatusBean> list = null;
		try {
			JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Map<String, List<VehicleStatusBean>> map = new ConcurrentHashMap<>();
		this.statusData.clear();
		if(list==null)
		{
			return ;
		}
		for (VehicleStatusBean vsbean : list) {
			if (!this.statusData.containsKey(vsbean.getFIBER_UNID())) {
				List<VehicleStatusBean> temp = new ArrayList<VehicleStatusBean>();
				temp.add(vsbean);
				this.statusData.put(vsbean.getFIBER_UNID(), temp);
			} else {
				List<VehicleStatusBean> temp = this.statusData.get(vsbean.getFIBER_UNID());
				temp.add(vsbean);
				this.statusData.replace(vsbean.getFIBER_UNID(), temp);
			}
		}
		

	}

}
