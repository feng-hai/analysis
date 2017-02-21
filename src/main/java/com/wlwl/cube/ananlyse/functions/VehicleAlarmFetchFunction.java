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
import java.text.SimpleDateFormat;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.UNID;
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
	private RedisUtils util = null;

	private static final Logger LOG = LoggerFactory.getLogger(VehicleStatusFunction.class);
	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	SimpleDateFormat DEFAULT_DATE = new SimpleDateFormat("yyyyMM");
	private static final String aiid_key = "ALARM_AIID:";

	private Map<String, VehicleStatisticBean> vehiclesMap;
	// private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private long lastTime;

	// private Map<String, VehicleStatisticBean> lastCharges = null;
	// private long lastTime;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {

		jdbcUtils = SingletonJDBC.getJDBC();
		vehiclesMap = new HashMap<String, VehicleStatisticBean>();
		lastTime = System.currentTimeMillis();
		util = RedisSingleton.instance();

	}

	@Override
	public void cleanup() {

		jdbcUtils.releaseConn();
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

			long currentTime = System.currentTimeMillis();

			String device = tuple.getStringByField("deviceId");

			ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");

			updateVehicleStatus(omok, device);
			updateCondition(device);

			VehicleStatisticBean vehicle = new VehicleStatisticBean();

			vehicle.setVehicle_unid(device);

			vehicle.setStatisticDateTime(omok.getTIMESTAMP());

			vehicle.setWorkTimeDateTime_end(omok.getTIMESTAMP());

			collector.emit(new Values(vehicle));

			saveMap(vehicle);
			// System.out.println("计时-开始" + StateUntils.formate(new
			// Date(currentTime)) + "-"
			// + StateUntils.formate(new Date(lastTime)));

			if (currentTime >= lastTime + 1000 * 60 * 30) {

				lastTime = currentTime;

				updateAlarm();

				cleanMap();

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * @Title: saveMap @Description: 保存在线车辆缓存 @param @param vehicle 设定文件 @return
	 * void 返回类型 @throws
	 */
	private void saveMap(VehicleStatisticBean vehicle) {
		String id = vehicle.getVehicle_unid() + StateUntils.formateDay(vehicle.getStatisticDateTime());
		if (!vehiclesMap.containsKey(id)) {
			vehiclesMap.put(id, vehicle);
		}
	}

	/**
	 * @Title: cleanMap @Description: 清空缓存 @param 设定文件 @return void 返回类型 @throws
	 */
	private void cleanMap() {
		vehiclesMap.clear();
	}

	private void updateAlarm() {
		Iterator<String> it = vehiclesMap.keySet().iterator();
		while (it.hasNext()) {
			String key;

			key = it.next().toString();

			VehicleStatisticBean vehicle = vehiclesMap.get(key);

			insertDataForAlarm(vehicle);
		}
	}

	/**
	 * @Title: insertDataForMile @Description: TODO 里程更新 @param @param vehicle
	 *         设定文件 @return void 返回类型 @throws
	 */
	private void insertDataForAlarm(VehicleStatisticBean vehicle) {

		// String sql = "SELECT count(0) alarmCount FROM
		// sensor.ANA_VEHICLE_EVENT where entity_unid=? and DATIME_BEGIN between
		// STR_TO_DATE(?,\"%Y-%m-%d %H:%i:%s\") and STR_TO_DATE(?,\"%Y-%m-%d
		// %H:%i:%s\")";
		// List<Object> params = new ArrayList<Object>();
		// params.add(vehicle.getVehicle_unid());
		//
		// params.add(new
		// SimpleDateFormat("yyyy-MM-dd").format(vehicle.getStatisticDateTime())
		// + " 00:00:00");
		// params.add(new
		// SimpleDateFormat("yyyy-MM-dd").format(vehicle.getStatisticDateTime())
		// + " 23:59:59");
		//
		// Map<String, Object> list = null;
		// try {
		// list = jdbcUtils.findSimpleResult(sql, params);
		//
		// HBaseUtils.insert(Conf.TABLENAME,
		// TimeBaseRowStrategy.getRowKeyForHase(vehicle), Conf.FAMILY,
		// "alarmCount", list.get("alarmCount").toString());
		//
		// } catch (SQLException e) {
		// e.printStackTrace();
		// jdbcUtils.releaseConn();
		// } finally{
		// //
		// }

	}

	/**
	 * @Title: updateCondition @Description: TODO固定的时间更新一下判断条件 @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private void updateCondition(String device) {
		String timekey = Conf.STORM_TIMER + "alarm" + device;
		String timer = util.hget(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm");
		if (timer != null) {
			Date date = StateUntils.strToDate(timer);
			if (date != null) {
				long m = new Date().getTime() - date.getTime();
				if (m > 1000 * 60 * 5) {
					util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm", StateUntils.formate(new Date()));
					// 更新数据
					String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "alarm" + device;
					Map<String, String> map = setRedis(device);
					if (map.size() > 0) {
						util.del(vehicleStatus);
						util.hmset(vehicleStatus, map);
					}
				}
			} else {
				util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm", StateUntils.formate(new Date()));
			}

		} else {
			util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER + "alarm", StateUntils.formate(new Date()));
		}

	}

	/**
	 * @param omok
	 *            读取kafka的实体类对象
	 * @param device
	 *            车辆唯一标识 vehicle_unid
	 */
	private void updateVehicleStatus(ObjectModelOfKafka omok, String device) {

		String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + "alarm" + device;
		String id = Conf.PERFIX + device;
		Map<String, String> map;
		// 從redis中獲取數據
		if (util.exists(vehicleStatus)) {
			map = util.hgetall(vehicleStatus);
			// 判斷redis中是否有數據
			if (map.size() == 0) {
				// redis 中沒有數據，從數據庫中讀取並複製
				map = setRedis(device);
				if (map.size() > 0) {
					util.hmset(vehicleStatus, map);
				}
			}
		} else {
			map = setRedis(device);
			if (map.size() > 0) {
				util.hmset(vehicleStatus, map);
			}
		}

		Boolean isMatch = false;
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key;
			String valueStr;
			key = it.next().toString();
			valueStr = map.get(key);
			VehicleStatusBean statusBean = JsonUtils.deserialize(valueStr, VehicleStatusBean.class);
			LOG.debug(JsonUtils.serialize(statusBean));
			if (statusBean != null) {
				// 根据key获取数据值
				Pair pair = omok.getPairByCode(statusBean.getCODE());
				Pair vehiclePair = omok.getVehicle_UNID();
				String unid = vehiclePair.getValue();
				String VehilceKey = "BIG_VEHICLE:" + unid;
				String lat = util.hget(VehilceKey, "LAT_D");
				String lng = util.hget(VehilceKey, "LAT_D");

				String errorName = statusBean.getREMARKS();

				if (pair != null) {
					String value = pair.getValue();
					Boolean isTrue = statusBean.checkStatus(value);
					if (isTrue) {
						isMatch = true;
						if (key.equals("1")) {
							String aiid = util.hget(aiid_key + unid, "StormCode");
							if (aiid != null) {
								alertEnd(unid, DEFAULT_DATE_SIMPLEDATEFORMAT.format(new Date()));
							}

							alertBegin(unid, errorName, DEFAULT_DATE_SIMPLEDATEFORMAT.format(new Date()), lat, lng);

						} else if (key.equals("2")) {
							alertEnd(unid, DEFAULT_DATE_SIMPLEDATEFORMAT.format(new Date()));
						}
						// util.hset(id, Conf.ACTIVE_STATUS, key);
						// util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
						//break;
					}
				}
			}
		}
		// if (!isMatch)// 设置上线默认值
		// {
		//
		// // String currentStatus = util.hget(id, Conf.ACTIVE_STATUS);
		// // System.out.println(omok.getDATIME_RX());
		// // System.out.println(currentStatus);
		// // if (currentStatus ==null ||(currentStatus != null &&
		// // currentStatus.equals("0"))) {
		// util.hset(id, Conf.ACTIVE_STATUS, "2");
		// util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
		// // }
		// isMatch = false;
		//
		// }

	}

	/**
	 * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private Map<String, String> setRedis(String vehicleUnid) {

		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS  FROM  cube.PDA_CUSTOM_SETUP where fiber_unid=? and type=2 and flag_del=0 order by INX desc";
		List<Object> params = new ArrayList<Object>();
		String fiber_unid = util.hget(id, field);

		LOG.debug("数据字典id" + fiber_unid);
		params.add(fiber_unid);
		List<VehicleStatusBean> list = null;
		try {
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.debug("数据库中数据" + list);
		Map<String, String> map = new HashMap<String, String>();
		for (VehicleStatusBean vsbean : list) {
			map.put(vsbean.getStatus().toString(), JsonUtils.serialize(vsbean));
		}

		return map;

	}

	private void alertEnd(String unid, String date) {
		String aiid = util.hget(aiid_key + unid, "StormCode");
		String dateStr = util.hget(aiid_key + unid, "beginTime");
		if (aiid != null) {
			StringBuilder update = new StringBuilder();
			update.append("update sensor.ANA_VEHICLE_EVENT_" + dateStr + " set FLAG_DID=1,DATIME_END=");
			update.append("'").append(date).append("'");
			update.append(" where AIID=").append(aiid);

			try {
				jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			util.hdel(aiid_key + unid, "StormCode");

		}
	}

	private void alertBegin(String unidin, String errorName, String date, String lat, String lng) {

		String alarmKey = "VEHICLE_CONDITION_ALARM_STATUS:";
		String unid = unidin; // 车辆唯一标识//snapshot.getEntity().getUnid();
		String key = Conf.PERFIX + unid;

		// String fiberId = util.hget(key, "fiber_unid");

		// List<String> listStr = util.hmget(alarmKey, fiberId + "_StormCode" );
		// ErrorCode errorCode = null;// 获取故障代码库数据 //findErrorCode(
		// snapshot.getFiberUnid(), event.getCode()
		// );
		// if (listStr.size() > 0&&listStr.get(0)!=null) {
		// errorCode = JsonUtils.deserialize(listStr.get(0), ErrorCode.class);
		// } else {
		// Map<String, String> map = setRedis();
		// if (map.size() > 0) {
		// util.del(alarmKey);
		// util.hmset(alarmKey, map);
		// }
		// listStr = util.hmget(alarmKey, fiberId + "_" + event.getCode());
		// errorCode = JsonUtils.deserialize(listStr.get(0), ErrorCode.class);
		// }
		String domainId = util.hget(key, "domain_unid");
		try {
			// connection = jdbc.getConnection();
			String sql = "insert into sensor.ANA_VEHICLE_EVENT_" + DEFAULT_DATE.format(new Date())
					+ "(UNID,ENTITY_UNID,DOMAIN_UNID,SUMMARY,EVENT_TYPE,LAT_D,LON_D,CONTEXT,LEVEL,ERROR_CODE) values(?,?,?,?,?,?,?,?,?,?)";

			List<Object> params = new ArrayList<Object>();
			params.add(UNID.getUnid());
			params.add(unid);
			params.add(domainId);
			params.add(errorName);
			params.add("");
			params.add(lat);
			params.add(lng);
			params.add("");
			params.add(0);
			params.add("");

			int aiid = jdbcUtils.insertByPreparedStatement(sql, params);

			util.hset(aiid_key + unid, "StormCode", String.valueOf(aiid));
			util.hset(aiid_key + unid, "beginTime", DEFAULT_DATE.format(new Date()));
			// util.hset(aiid_key + unid, "beginTime", String.valueOf(aiid));
			// PreparedStatement pstmt = connection.prepareStatement( sql,
			// Statement.RETURN_GENERATED_KEYS );
			// pstmt.setString( 1, UNID.getUnid() );
			// pstmt.setString( 2, unid );
			// pstmt.setString( 3, snapshot.getEntity().getDomainUnid() );
			// pstmt.setString( 4, errorCode != null ? errorCode.getName() : ""
			// );
			// pstmt.setString( 5, eventType );
			// pstmt.setDouble( 6, snapshot.getLatitudeDeviated() );
			// pstmt.setDouble( 7, snapshot.getLongitudeDeviated() );
			// pstmt.setString( 8, event.getHex() );// TODO
			// pstmt.setInt( 9, errorCode != null ? errorCode.getLevel() : 0 );
			// pstmt.setString( 10, event.getCode() );// TODO
			//
			// //((Object) LOG).fine( "insert event: " + pstmt.toString() );
			// pstmt.execute();
			// ResultSet rs = pstmt.getGeneratedKeys();
			// if (rs.next())
			// {
			// int aiid = rs.getInt( 1 );
			// LOG.info( "alert id:" + aiid );
			// AlertEvent alert = new AlertEvent();
			// alert.setAiid( aiid );
			// alert.setDatimeBegin( DateHelper.getDateYYYY_MM_DD() );
			// alert.setEntityUnid( unid );
			// alert.setEventType( Event.EVENT_ALERT );
			// alert.setTag( event.getCode() );
			// String alertKey = unid + "_" + aiid;
			// addEvent( snapshot, alert );
			// }
			// pstmt.close();
		} catch (SQLException e) {
			// LOG.severe( "get connection锛� " + e.getLocalizedMessage() );
		} finally {
			/// closeConnection( connection );
			// jdbc.releaseConn();
		}

		// long alertCount = 0;
		// ShardedJedis redis = REDIS_POOL.getResource();
		// if (redis != null)
		// {
		// final String alertCounterKey = ALERT_COUNTER_KEY_PREFIX + unid;
		// alertCount = redis.incr( alertCounterKey );
		// String redisKey = PREFIX_VEHICLE + unid;
		// redis.hset( redisKey, KEY_LEVEL, "" + (errorCode != null ?
		// errorCode.getLevel() : 5) );
		// redis.close();
		// }

		StringBuilder update = new StringBuilder();
		update.append("update sensor.ANA_SNAPSHOT set DATIME_ALERT=");
		update.append("'").append(date).append("'");
		update.append(",COUNT_ALERT=").append(1);
		update.append(",LEVEL_ALERT=").append((0));
		// update.append(",NODE_UNID='").append("").append("'");
		update.append(" where UNID='").append(unid).append("'");
		try {
			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// DBHelper.commitSQL( update.toString() );
	}

}
