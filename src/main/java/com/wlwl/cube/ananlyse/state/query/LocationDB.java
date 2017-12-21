package com.wlwl.cube.ananlyse.state.query;

import java.sql.SQLException;
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
import com.wlwl.cube.analyse.bean.VehicleInfo;
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
	 * @Fields PERFIX : TODO(用一句话描述这个变量表示什么)
	 */
	private long lastTime;
	private static long lastTimeForUpdateRedis;
	private static long lastTimeOnline;
	private RedisUtils util = null;
	// private JdbcUtils jdbcUtils = null;
	private static final Logger log = LoggerFactory.getLogger(LocationDB.class);
	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Map<String, List<VehicleStatusBean>> statusData = null;

	private static Map<String, VehicleInfo> vehicleCatch = new ConcurrentHashMap<>();

	public LocationDB() {
		util = RedisSingleton.instance();
		// jdbcUtils = SingletonJDBC.getJDBC();
		lastTime = System.currentTimeMillis();
		lastTimeForUpdateRedis = System.currentTimeMillis();
		lastTimeOnline = System.currentTimeMillis();
	}

	public LocationDB(Map<String, List<VehicleStatusBean>> map) {
		util = RedisSingleton.instance();
		// jdbcUtils = SingletonJDBC.getJDBC();
		this.statusData = map;
		lastTime = System.currentTimeMillis();
		lastTimeForUpdateRedis = System.currentTimeMillis();
		lastTimeOnline = System.currentTimeMillis();
	}

	public void beginCommit(Long txid) {

	}

	public void commit(Long txid) {

	}

	public void setLocationsBulk(List<ObjectModelOfKafka> vehicleIds) {

		long currentTime = System.currentTimeMillis();
		Map<String, Map<String, String>> vehicles = new ConcurrentHashMap<>();
		for (ObjectModelOfKafka omok : vehicleIds) {
			if (omok == null) {
				continue;
			}
			// 更新车辆在线状态
			try {

				Map<String, Map<String, String>> temp = updateVehicleSatusNew(omok);
				if (temp != null) {
					vehicles.putAll(temp);
				}
				if (currentTime - lastTime > 1000 * 60 * 5) {
					this.lastTime = currentTime;
					loadData();
				}
				// updateNoOnline();

			} catch (Exception ex) {
				log.error("错误", ex);
			}
		}

		// long time = System.currentTimeMillis();
		// if(vehicles.size()>2000||time-lastTimeForUpdateRedis>10000)
		// {
		// lastTimeForUpdateRedis=time;
		util.setKeys(vehicles);
		// vehicles.clear();
		// }

	}

	public List<String> bulkGetLocations(List<Long> userIds) {
		return null;
	}

	/**
	 * 检查5分钟没有上线的车辆，并把车辆设置为离线状态
	 */
	// private void updateNoOnline() {

	// String timekey = Conf.STORM_TIMER + "status" + "ONLINETIER";
	// String timer = util.hget(timekey, Conf.ACTIVE_ONLINE_TIMER + "status");
	// if (timer != null) {
	// Date date = StateUntils.strToDate(timer);
	// if (date != null) {
	// long m =System.currentTimeMillis() - lastTimeOnline;
	// if (m > 1000 * 60 * 5) {
	// lastTimeOnline=System.currentTimeMillis() ;
	// // util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status",
	// StateUntils.formate(new Date()));
	// // 更新数据
	// checkOnLine();
	//
	// }
	// }
	// else {
	// checkOnLine();
	// util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status",
	// StateUntils.formate(new Date()));
	// }

	// } else {
	// checkOnLine();
	// util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER + "status",
	// StateUntils.formate(new Date()));
	// }

	// }

	/**
	 * @Title: checkOnLine @Description:
	 *         TODO长时间不更新的数据，状态更新为离线，此方法定时运行，五分钟一次 @param 设定文件 @return void
	 *         返回类型 @throws
	 */
	// public void checkOnLine() {
	// Map<String, Map<String, String>> vehicles=new ConcurrentHashMap <>();
	// Set<String> set = util.keys(Conf.PERFIX + "*");
	// for (String str : set) {
	// // String status = util.hget(str, Conf.ACTIVE_STATUS);
	// // if (status != null && status != "0") {
	// if(str==null)
	// {
	// continue;
	// }
	//
	// Map<String, String> temp=new HashMap<>();
	// String time = util.hget(str, Conf.DATIME_RX);
	// // String status = util.hget(str, Conf.ACTIVE_STATUS);
	// // if (status == null || !status.equals("0")) {
	// if (time != null) {// 如果时间存在
	// if (new Date().getTime() - StateUntils.strToDate(time).getTime() > 1000 *
	// 60 * 5) {
	//
	//
	// //util.hset(str, Conf.ACTIVE_STATUS, "0");
	// temp.put(Conf.ACTIVE_STATUS, "0");
	// vehicles.put(str, temp);
	// // String unid = util.hget(str, "unid");
	// // alertEnd(unid);
	// }
	// } else {// 如果时间不存在
	// //util.hset(str, Conf.ACTIVE_STATUS, "0");
	// temp.put(Conf.ACTIVE_STATUS, "0");
	// vehicles.put(str, temp);
	// // String unid = util.hget(str, "unid");
	// //alertEnd(unid);
	// }
	// //}
	// // }
	// }
	// util.setKeys(vehicles);
	// set = null;
	// }

	private static final String aiid_key = "ALARM_AIID:";

	// private void alertEnd(String vehicleUnid) {
	//
	// JdbcUtils jdbcUtils =new JdbcUtils();
	// Map<String, String> result = util.hgetall(aiid_key + vehicleUnid);
	// if(result==null)
	// {
	// return;
	// }
	// for (String key : result.keySet()) {
	// if(key==null)
	// {
	// continue;
	// }
	// if(!result.containsKey(key)||!result.containsKey(key + "beginTime"))
	// {
	// continue;
	// }
	// String aiid = result.get(key);
	// String dateStr = result.get(key + "beginTime");
	// if (aiid != null &&!aiid.isEmpty()&& dateStr != null&&!dateStr.isEmpty())
	// {
	//
	// StringBuilder update = new StringBuilder();
	// update.append("CALL `sensor`.`offAlarmting`('");
	// update.append(aiid);
	// update.append("', '");
	// update.append(StateUntils.formate(new Date()));
	// update.append("', '");
	// update.append(dateStr);
	// update.append("')");
	// try {
	// jdbcUtils.updateByPreparedStatement(update.toString(), new
	// ArrayList<Object>());
	// } catch (SQLException e) {
	// // TODO Auto-generated catch block
	// log.error("错误",e);
	// }
	// }
	// }
	// jdbcUtils.releaseConn();
	// util.del(aiid_key + vehicleUnid);
	// }

	private Map<String, Map<String, String>> updateVehicleSatusNew(ObjectModelOfKafka omok) {

		if (omok.getVehicle_UNID() == null) {
			return null;
		}

		Map<String, Map<String, String>> cMap = new HashMap<>();
		Map<String, String> pairsMap2 = new HashMap<>();

		pairsMap2.put(Conf.ACTIVE_STATUS, "2");

		String vehicleUnid = omok.getVehicle_UNID().getValue();
		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String fiber_unid = null;
		String currentStatus = null;

		if (!vehicleCatch.containsKey(id)) {
			VehicleInfo vehicle = new VehicleInfo();
			Map<String, String> values = util.hgetall(id);
			fiber_unid = values.get(field);
			currentStatus = values.get(Conf.ACTIVE_STATUS);
			vehicle.setFiberid(fiber_unid);
			vehicle.setUnid(currentStatus);
			vehicleCatch.put(id, vehicle);
		} else {
			VehicleInfo vehicle = vehicleCatch.get(id);
			fiber_unid = vehicle.getFiberid();
			currentStatus = vehicle.getUnid();
		}

		// log.info("当前车辆状态："+currentStatus);
		// if (currentStatus == null || (currentStatus != null &&
		// currentStatus.equals("0"))) {
		// // util.hset(id, Conf.ACTIVE_STATUS, "2");
		// pairsMap2.put(Conf.ACTIVE_STATUS, "2");
		// vehicle.setUnid("2");
		// cMap.put(id, pairsMap2);
		// // log.info("更新车辆状态：2");
		// return cMap;
		// }
		// if (fiber_unid == null) {
		// System.out.println("车辆数据字典为空：" + id);
		// return null;
		// }
		// Boolean isMatch = false;

		// Map<String, String> pairsMap3 = new HashMap<>();
		if (fiber_unid != null && this.statusData.containsKey(fiber_unid)) {
			// LOG.info("信息02"+JsonUtils.serialize(omok));
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
				if (isTrue) {
					// isMatch = true;

					pairsMap2.put(Conf.ACTIVE_STATUS, String.valueOf(status));
					// vehicle.setUnid(String.valueOf(status));
					// pairsMap3.put( "default", StateUntils.formate(new
					// Date()));
					// LOG.info("信息03:"+status+JsonUtils.serialize(omok));
					// util.hset(id, Conf.ACTIVE_STATUS,
					// String.valueOf(status));
					// util.hset(timekey, "default", StateUntils.formate(new
					// Date()));
					// cMap.put(id, pairsMap2);
					// cMap.put(timekey, pairsMap3);
					break;
				}
			}
		}
		cMap.put(id, pairsMap2);
		// if (!isMatch)// 设置上线默认值
		// {
		// // String timer = util.hget(timekey, "default");
		// // if (timer != null) {
		// // Date date = StateUntils.strToDate(timer);
		// // if (date != null) {
		// long m = System.currentTimeMillis() - lastTimeForUpdateRedis;
		// if (m > 1000 * 60) {
		// lastTimeForUpdateRedis = System.currentTimeMillis();
		// pairsMap2.put(Conf.ACTIVE_STATUS, "2");
		// vehicle.setUnid("2");
		// cMap.put(id, pairsMap2);
		// // util.hset(id, Conf.ACTIVE_STATUS, "2");
		// // LOG.info("信息04:2-"+JsonUtils.serialize(omok));
		// // util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
		// }
		// // }
		// // } else {
		// // util.hset(timekey, "default", StateUntils.formate(new Date()));
		// // pairsMap3.put( "default", StateUntils.formate(new Date()));
		// // cMap.put(timekey, pairsMap3);
		// // }
		// }
		return cMap;
	}

	// /**
	// *
	// * 加载数据库中数据，安装数据字典存储
	// */
	private void loadData() {
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME,fiber_unid  FROM  cube.PDA_CUSTOM_SETUP where type=1 and flag_del=0 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		List<VehicleStatusBean> list = null;
		JdbcUtils jdbcUtils = null;
		try {
			// JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
			jdbcUtils = new JdbcUtils();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
			Log.info("加载数据成功！");
			;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Log.error("错误-加载数据", e);
		} finally {

			jdbcUtils.releaseConn();
			jdbcUtils = null;
		}
		// Map<String, List<VehicleStatusBean>> map = new ConcurrentHashMap<>();
		//
		if (list == null) {
			return;
		}
		if (list.size() > 0) {
			this.statusData.clear();
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
