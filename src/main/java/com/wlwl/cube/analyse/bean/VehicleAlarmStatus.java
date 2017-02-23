package com.wlwl.cube.analyse.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;

public class VehicleAlarmStatus {
	private ObjectModelOfKafka omokObject=null;
	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private static final String aiid_key = "ALARM_AIID:";
	public VehicleAlarmStatus(ObjectModelOfKafka omok)
	{	
		this.omokObject=omok;
		util = RedisSingleton.instance();
	}
	public List<VehicleAlarmBean> getAlarmBean()
	{
		String device=this.omokObject.getVehicle_UNID().getValue();
		String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "alarm" + device;
		List<VehicleAlarmBean> alarmList = new CopyOnWriteArrayList<VehicleAlarmBean>();
        util.del(vehicleStatus);
        util.del(Conf.VEHICLE_CONDITION_STATUS+"*");
        
		Map<String, String> map = util.hgetall(vehicleStatus);
		// 判斷redis中是否有數據
		if (map == null || map.size() == 0) {
			// redis 中沒有數據，從數據庫中讀取並複製
			map = setRedis(device);
			if (map.size() > 0) {
				util.hmset(vehicleStatus, map);
			}
		}
		Pair vehiclePair = this.omokObject.getVehicle_UNID();
		String unid = vehiclePair.getValue();
		String VehilceKey = "BIG_VEHICLE:" + unid;
		String lat = util.hget(VehilceKey, "LAT_D");
		String lng = util.hget(VehilceKey, "LAT_D");
		String Vehiclekey = Conf.PERFIX + unid;
		String domainId = util.hget(Vehiclekey, "domain_unid");
		String date = this.omokObject.getDATIME_RX();
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key;
			String valueStr;
			key = it.next().toString();
			valueStr = map.get(key);
			VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr, VehicleStatusBean.class);
			if (statusBean != null) {
				// 根据key获取数据值
				//System.out.println("获取数据"+valueStr);
				Pair pair = this.omokObject.getPairByCode(statusBean.getCODE());
				String code = statusBean.getCODE();
				String errorName = statusBean.getALARM_NAME();
				Integer level = statusBean.getALARM_LEVEL();
				if (pair != null) {
					String value = pair.getValue();
					Boolean isTrue = statusBean.checkStatus(value);
					VehicleAlarmBean alarm = new VehicleAlarmBean();
					alarm.setVehicleUnid(unid);
					alarm.setDomainId(domainId);
					alarm.setDateTime(date);
					alarm.setErrorName(errorName);
					alarm.setLat(lat);
					alarm.setLng(lng);
					alarm.setLevel(level);
					alarm.setCode(code);
					Date now;
					try {
						now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
						//now.setMonth(2);
						alarm.setTableSuf(new SimpleDateFormat("yyyyMM").format(now));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isTrue) {
						if (key.equals("1")) {
							alarm.setIsBegin(true);
							alarm.setUnid(UNID.getUnid());
							String aiid = util.hget(aiid_key + unid, alarm.getCode() + alarm.getLevel());
							if (aiid == null) {

								alarmList.add(alarm);

								util.hset(aiid_key + unid, alarm.getCode() + alarm.getLevel(), alarm.getUnid());
								util.hset(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf",
										alarm.getTableSuf());

							}
						} else {

							String aiid = util.hget(aiid_key + unid, alarm.getCode() + alarm.getLevel());
							String tableSuf = util.hget(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf");
							if (aiid != null) {
								alarm.setUnid(aiid);
								alarm.setIsBegin(false);
								alarm.setTableSuf(tableSuf);
								alarmList.add(alarm);

							}

							// alertEnd(unid, omok.getDATIME_RX());
						}

					} else {
						String aiid = util.hget(aiid_key + unid, alarm.getCode() + alarm.getLevel());
						String tableSuf = util.hget(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf");
						if (aiid != null) {
							alarm.setUnid(aiid);
							alarm.setIsBegin(false);
							alarm.setTableSuf(tableSuf);
							alarmList.add(alarm);

						}
						util.hdel(aiid_key + unid, alarm.getCode() + alarm.getLevel());
						util.hdel(aiid_key + unid, alarm.getCode() + alarm.getLevel() + "suf");
					}
				}
			}
		}

		return alarmList;
	}
	/**
	 * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private Map<String, String> setRedis(String vehicleUnid) {

		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME  FROM  cube.PDA_CUSTOM_SETUP where fiber_unid=? and type=2 and flag_del=0 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		String fiber_unid = util.hget(id, field);
		params.add(fiber_unid);
		List<VehicleStatusBean> list = null;
		try {
			jdbcUtils = SingletonJDBC.getJDBC();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Map<String, String> map = new ConcurrentHashMap<String, String>();
		for (VehicleStatusBean vsbean : list) {
			map.put(vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));
		}

		return map;

	}
}
