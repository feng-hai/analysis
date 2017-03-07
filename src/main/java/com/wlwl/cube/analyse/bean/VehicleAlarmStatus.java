package com.wlwl.cube.analyse.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;

import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;

public class VehicleAlarmStatus {
	private ObjectModelOfKafka omokObject = null;
	private RedisUtils util = null;

	private static Map<String, String> alarmKeys = new ConcurrentHashMap<>();

	private static final String aiid_key = "ALARM_AIID:";
	Map<String, List<VehicleStatusBean>> statusMap = null;

	public VehicleAlarmStatus(ObjectModelOfKafka omok, Map<String, List<VehicleStatusBean>> status) {
		this.omokObject = omok;
		util = RedisSingleton.instance();
		this.statusMap = status;
	}

	public List<VehicleAlarmBean> getAlarmBean() {

		List<VehicleAlarmBean> alarmList = new ArrayList<VehicleAlarmBean>();
		try {
			Pair vehiclePair = this.omokObject.getVehicle_UNID();
			if (vehiclePair == null) {
				return alarmList;
			}
			String unid = vehiclePair.getValue();
			if (unid == null) {
				return alarmList;
			}
			String VehilceKey = "BIG_VEHICLE:" + unid;
			List<String> lastValue = util.hmget(VehilceKey, "LAT_D", "LON_D", "domain_unid", "fiber_unid");
			if (lastValue != null && lastValue.size() == 4) {
				String lat = lastValue.get(0);
				String lng = lastValue.get(1);
				String domainId = lastValue.get(2);
				String fiber_unid = lastValue.get(3);
				if (lat != null && lng != null && domainId != null && fiber_unid != null) {
					// util.hget(VehilceKey, "fiber_unid");
					if (this.statusMap.containsKey(fiber_unid)) {
						List<VehicleStatusBean> statusList = this.statusMap.get(fiber_unid);
						String date = this.omokObject.getDATIME_RX();
						if (date == null) {
							return alarmList;
						}
						for (VehicleStatusBean statusBean : statusList) {
							Pair pair = this.omokObject.getPairByCode(statusBean.getCODE());
							if (pair == null) {
								continue;
							}
							if (pair != null) {
								String value = pair.getValue();
								if (value == null) {
									continue;
								}
								Boolean isTrue = statusBean.checkStatus(value);
								String code = statusBean.getCODE();
								if(code==null)
								{
									continue;
								}
								String errorName = statusBean.getALARM_NAME();
								if(errorName==null)
								{
									continue;
								}
								Integer level = statusBean.getALARM_LEVEL();
								if(level==null)
								{
									continue;
								}
								VehicleAlarmBean alarm = new VehicleAlarmBean();
								alarm.setVehicleUnid(unid);
								alarm.setDomainId(domainId);
								alarm.setDateTime(date);
								alarm.setErrorName(errorName);
								alarm.setLat(lat);
								alarm.setLng(lng);
								alarm.setLevel(level);
								alarm.setCode(code);
								// 设置表后缀如：201702
								String[] dataArray = date.split("-");
								if (dataArray.length <2) {
									continue;
								}
								alarm.setTableSuf(dataArray[0] + dataArray[1]);
								if (isTrue && statusBean.getStatus() == 1) {
									
									if (!alarmKeys.containsKey(aiid_key + unid + code + level)) {
										alarm.setIsBegin(true);
										alarm.setUnid(UNID.getUnid());
										alarmList.add(alarm);
										alarmKeys.put(aiid_key + unid + code + level, alarm.getUnid());
										alarmKeys.put(aiid_key + unid + code + level + "suf", alarm.getTableSuf());
									}

								} else {
									if (alarmKeys.containsKey(aiid_key + unid + code + level)
											&& alarmKeys.containsKey(aiid_key + unid + code + level + "suf")) {
										String id = alarmKeys.get(aiid_key + unid + code + level);
										String suf = alarmKeys.get(aiid_key + unid + code + level + "suf");
										if (id != null && suf != null) {
											alarm.setUnid(id);
											alarm.setIsBegin(false);
											alarm.setTableSuf(suf);
											alarmList.add(alarm);
											alarmKeys.remove(aiid_key + unid + code + level);
											alarmKeys.remove(aiid_key + unid + code + level + "suf");
										}
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			System.out.println("錯誤" + ex.getMessage());
		}

		return alarmList;
	}

}

/**
 * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
 *         设定文件 @return void 返回类型 @throws
 */
// private List<VehicleStatusBean> setRedis(String fiber_unid) {
//
// // String id = Conf.PERFIX + vehicleUnid;
// // String field = "fiber_unid";
// String sql = "SELECT
// code,option,value,VALUE_LAST,status,REMARKS,ALARM_LEVEL,ALARM_NAME FROM
// cube.PDA_CUSTOM_SETUP where fiber_unid=? and type=2 and flag_del=0 order by
// INX desc";
// List<Object> params = new CopyOnWriteArrayList<Object>();
// // String fiber_unid = util.hget(id, field);
// params.add(fiber_unid);
// List<VehicleStatusBean> list = new CopyOnWriteArrayList<>();
// try {
// jdbcUtils = SingletonJDBC.getJDBC();
// list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params,
// VehicleStatusBean.class);
// } catch (Exception e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
// // Map<String, String> map = new ConcurrentHashMap<String, String>();
// // map.put(fiber_unid, JsonUtils.serialize(list));
// return list;
//
// }

/**
 * @Title: updateCondition @Description: TODO固定的时间更新一下判断条件 @param 设定文件 @return
 *         void 返回类型 @throws
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
// // redis 中沒有數據，從數據庫中讀取並複製
//
// List<VehicleStatusBean> statusList = new ArrayList<>();
// statusList = setRedis(device);
// if (statusList.size() > 0) {
// util.set(vehicleStatus, JsonUtils.serialize(statusList));
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
// }
