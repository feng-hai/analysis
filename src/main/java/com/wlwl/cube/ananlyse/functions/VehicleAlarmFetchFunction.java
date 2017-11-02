/**  
* @Title: VehicleAlarmFunction.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(��һ�仰�������ļ���ʲô)
* @author fenghai  
* @date 2016��9��16�� ����10:10:10
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.UNID;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;
import com.wlwl.cube.analyse.bean.VehicleAlarmStatus;
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
 * @ClassName: VehicleAlarmFetchFunction
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月18日 下午9:42:59
 *
 */
public class VehicleAlarmFetchFunction extends BaseFunction {

	private static final long serialVersionUID = 8414621340097218898L;
	private  Map<String, List<VehicleStatusBean>> statusData;

	private long lastTime;
	private static final Logger log = LoggerFactory.getLogger(VehicleAlarmFetchFunction.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	public VehicleAlarmFetchFunction(Map<String, List<VehicleStatusBean>> status) {
		this.statusData = status;
		lastTime = System.currentTimeMillis();
	}

	@Override
	public void cleanup() {

		// jdbcUtils.releaseConn();
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
		long currentTime = System.currentTimeMillis();
		List<VehicleAlarmBean> alarmList = new ArrayList<VehicleAlarmBean>();
		try {
			ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");
			try {
				VehicleAlarmStatus vehicleAlarm = new VehicleAlarmStatus(omok, this.statusData);
				alarmList = vehicleAlarm.getAlarmBean();
			} catch (Exception ex) {
				log.error("数据转化错误", ex);
			}
			collector.emit(new Values(alarmList));
		} catch (Exception e) {
			log.error("错误", e);
		}
		if (currentTime - lastTime > 1000 * 60 * 60 * 12) {
			this.lastTime = currentTime;
			loadData();
		}

	}

	private void loadData() {
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME,fiber_unid  FROM  cube.PDA_CUSTOM_SETUP where type=2 and flag_del=0 and status=1 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		List<VehicleStatusBean> list = null;
		try {
			JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Log.error("错误", e);
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

	// /**
	// * @Title: saveMap @Description: 保存在线车辆缓存 @param @param vehicle 设定文件
	// @return
	// * void 返回类型 @throws
	// */
	// private void saveMap(VehicleStatisticBean vehicle) {
	// String id = vehicle.getVehicle_unid() +
	// StateUntils.formateDay(vehicle.getStatisticDateTime());
	// if (!vehiclesMap.containsKey(id)) {
	// vehiclesMap.put(id, vehicle);
	// }
	// }
	//
	// /**
	// * @Title: cleanMap @Description: 清空缓存 @param 设定文件 @return void 返回类型
	// @throws
	// */
	// private void cleanMap() {
	// vehiclesMap.clear();
	// }
	//
	// private void updateAlarm() {
	// Iterator<String> it = vehiclesMap.keySet().iterator();
	// while (it.hasNext()) {
	// String key;
	//
	// key = it.next().toString();
	//
	// VehicleStatisticBean vehicle = vehiclesMap.get(key);
	//
	// insertDataForAlarm(vehicle);
	// }
	// }
	//
	// /**
	// * @Title: insertDataForMile @Description: TODO 里程更新 @param @param vehicle
	// * 设定文件 @return void 返回类型 @throws
	// */
	// private void insertDataForAlarm(VehicleStatisticBean vehicle) {
	//
	// // String sql = "SELECT count(0) alarmCount FROM
	// // sensor.ANA_VEHICLE_EVENT where entity_unid=? and DATIME_BEGIN between
	// // STR_TO_DATE(?,\"%Y-%m-%d %H:%i:%s\") and STR_TO_DATE(?,\"%Y-%m-%d
	// // %H:%i:%s\")";
	// // List<Object> params = new ArrayList<Object>();
	// // params.add(vehicle.getVehicle_unid());
	// //
	// // params.add(new
	// // SimpleDateFormat("yyyy-MM-dd").format(vehicle.getStatisticDateTime())
	// // + " 00:00:00");
	// // params.add(new
	// // SimpleDateFormat("yyyy-MM-dd").format(vehicle.getStatisticDateTime())
	// // + " 23:59:59");
	// //
	// // Map<String, Object> list = null;
	// // try {
	// // list = jdbcUtils.findSimpleResult(sql, params);
	// //
	// // HBaseUtils.insert(Conf.TABLENAME,
	// // TimeBaseRowStrategy.getRowKeyForHase(vehicle), Conf.FAMILY,
	// // "alarmCount", list.get("alarmCount").toString());
	// //
	// // } catch (SQLException e) {
	// // e.printStackTrace();
	// // jdbcUtils.releaseConn();
	// // } finally{
	// // //
	// // }
	//
	// }
	//
	/**
	 * @Title: updateCondition @Description: TODO固定的时间更新一下判断条件 @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	// private void updateCondition(String device) {
	// String timekey = Conf.STORM_TIMER + "alarm" + device;
	// String timer = util.hget(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm");
	// if (timer != null) {
	// Date date = StateUntils.strToDate(timer);
	// if (date != null) {
	// long m = new Date().getTime() - date.getTime();
	// if (m > 1000 * 60 * 5) {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm",
	// StateUntils.formate(new Date()));
	// // 更新数据
	// String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "alarm" + device;
	// Map<String, String> map = setRedis(device);
	// if (map.size() > 0) {
	// util.del(vehicleStatus);
	// util.hmset(vehicleStatus, map);
	// }
	// }
	// } else {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm",
	// StateUntils.formate(new Date()));
	// }
	//
	// } else {
	// util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm",
	// StateUntils.formate(new Date()));
	// }
	//
	// }

	//
	// /**
	// * @param omok
	// * 读取kafka的实体类对象
	// * @param device
	// * 车辆唯一标识 vehicle_unid
	// */
	// private List<VehicleAlarmBean>
	// updateVehicleAlarmStatus(ObjectModelOfKafka omok, String device) {
	//
	// String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "alarm" + device;
	// List<VehicleAlarmBean> alarmList = new
	// CopyOnWriteArrayList<VehicleAlarmBean>();
	//
	// Map<String, String> map = util.hgetall(vehicleStatus);
	// // 判斷redis中是否有數據
	// if (map == null || map.size() == 0) {
	// // redis 中沒有數據，從數據庫中讀取並複製
	// map = setRedis(device);
	// if (map.size() > 0) {
	// util.hmset(vehicleStatus, map);
	// }
	// }
	// Pair vehiclePair = omok.getVehicle_UNID();
	// String unid = vehiclePair.getValue();
	// String VehilceKey = "BIG_VEHICLE:" + unid;
	// String lat = util.hget(VehilceKey, "LAT_D");
	// String lng = util.hget(VehilceKey, "LAT_D");
	// String Vehiclekey = Conf.PERFIX + unid;
	// String domainId = util.hget(Vehiclekey, "domain_unid");
	// String date = omok.getDATIME_RX();
	// Iterator<String> it = map.keySet().iterator();
	// while (it.hasNext()) {
	// String key;
	// String valueStr;
	// key = it.next().toString();
	// valueStr = map.get(key);
	// VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr,
	// VehicleStatusBean.class);
	// if (statusBean != null) {
	// // 根据key获取数据值
	// Pair pair = omok.getPairByCode(statusBean.getCODE());
	// String code = statusBean.getCODE();
	// String errorName = statusBean.getALARM_NAME();
	// Integer level = statusBean.getALARM_LEVEL();
	// if (pair != null) {
	// String value = pair.getValue();
	// Boolean isTrue = statusBean.checkStatus(value);
	// VehicleAlarmBean alarm = new VehicleAlarmBean();
	// alarm.setVehicleUnid(unid);
	// alarm.setDomainId(domainId);
	// alarm.setDateTime(date);
	// alarm.setErrorName(errorName);
	// alarm.setLat(lat);
	// alarm.setLng(lng);
	// alarm.setLevel(level);
	// alarm.setCode(code);
	// Date now;
	// try {
	// now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
	// //now.setMonth(2);
	// alarm.setTableSuf(new SimpleDateFormat("yyyyMM").format(now));
	// } catch (ParseException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// if (isTrue) {
	// if (key.equals("1")) {
	// alarm.setIsBegin(true);
	// alarm.setUnid(UNID.getUnid());
	// String aiid = util.hget(aiid_key + unid, alarm.getCode() +
	// alarm.getLevel());
	// if (aiid == null) {
	//
	// alarmList.add(alarm);
	//
	// util.hset(aiid_key + unid, alarm.getCode() + alarm.getLevel(),
	// alarm.getUnid());
	// util.hset(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf",
	// alarm.getTableSuf());
	// // alertBegin(unid, errorName, level,
	// // omok.getDATIME_RX(), lat, lng);
	//
	// }
	// } else {
	//
	// String aiid = util.hget(aiid_key + unid, alarm.getCode() +
	// alarm.getLevel());
	// String tableSuf = util.hget(aiid_key + unid, alarm.getCode() +
	// alarm.getLevel() + "suf");
	// if (aiid != null) {
	// alarm.setUnid(aiid);
	// alarm.setIsBegin(false);
	// alarm.setTableSuf(tableSuf);
	// alarmList.add(alarm);
	//
	// }
	//
	// // alertEnd(unid, omok.getDATIME_RX());
	// }
	//
	// } else {
	// String aiid = util.hget(aiid_key + unid, alarm.getCode() +
	// alarm.getLevel());
	// String tableSuf = util.hget(aiid_key + unid, alarm.getCode() +
	// alarm.getLevel() + "suf");
	// if (aiid != null) {
	// alarm.setUnid(aiid);
	// alarm.setIsBegin(false);
	// alarm.setTableSuf(tableSuf);
	// alarmList.add(alarm);
	//
	// }
	// util.hdel(aiid_key + unid, alarm.getCode() + alarm.getLevel());
	// util.hdel(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf");
	// }
	// }
	// }
	// }
	//
	// return alarmList;
	// }
	//
	// /**
	// * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	// * 设定文件 @return void 返回类型 @throws
	// */
	// private Map<String, String> setRedis(String vehicleUnid) {
	//
	// String id = Conf.PERFIX + vehicleUnid;
	// String field = "fiber_unid";
	// String sql = "SELECT code,option,value,VALUE_LAST
	// ,status,REMARKS,ALARM_LEVEL,ALARM_NAME FROM cube.PDA_CUSTOM_SETUP where
	// fiber_unid=? and type=2 and flag_del=0 order by INX desc";
	// List<Object> params = new CopyOnWriteArrayList<Object>();
	// String fiber_unid = util.hget(id, field);
	// params.add(fiber_unid);
	// List<VehicleStatusBean> list = null;
	// try {
	// JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
	// list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params,
	// VehicleStatusBean.class);
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// Map<String, String> map = new ConcurrentHashMap<String, String>();
	// for (VehicleStatusBean vsbean : list) {
	// map.put(vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));
	// }
	//
	// return map;
	//
	// }

}
