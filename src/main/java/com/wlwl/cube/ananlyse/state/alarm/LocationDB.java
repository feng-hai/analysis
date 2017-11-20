package com.wlwl.cube.ananlyse.state.alarm;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;
import com.wlwl.cube.analyse.bean.VehicleAlarmStatus;
import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;
public class LocationDB implements State {

	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private static final Logger log = LoggerFactory.getLogger(LocationDB.class);
	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Map<String, List<VehicleStatusBean>> statusData = null;
	public LocationDB() {
		util = RedisSingleton.instance();
	}
//	public LocationDB(Map<String, List<VehicleStatusBean>> status)
//	{
//		util = RedisSingleton.instance();
//		//this.statusData=status;
//	}

	public void beginCommit(Long txid) {

	}
	public void commit(Long txid) {

	}
	public void setLocationsBulk(List<List<VehicleAlarmBean>> omokList) {
		//Collections.reverse(omokList);
		for (List<VehicleAlarmBean> omok : omokList) {
			try {
				for (VehicleAlarmBean alarm : omok) {
					try {
						if (alarm.getIsBegin()) {
							alertBegin(alarm);
						} else {
							alertEnd(alarm);
						}
					} catch (Exception ex) {
						log.error("错误",ex);
					}
				}
			} catch (Exception ex) {
				log.error("错误",ex);
			}
		}
		omokList=null;

	}

	public List<List<VehicleAlarmBean>> bulkGetLocations(List<ObjectModelOfKafka> omokList) {
		List<List<VehicleAlarmBean>> alarmList = new ArrayList<>();
		for (ObjectModelOfKafka omok : omokList) {
			try {
				VehicleAlarmStatus vehicleAlarm = new VehicleAlarmStatus(omok,this.statusData);
				List<VehicleAlarmBean> alarm = vehicleAlarm.getAlarmBean();
				alarmList.add(alarm);
			} catch (Exception ex) {
				log.error("错误",ex);
			}
		}
		return alarmList;
	}
	
	
	private void alertEnd(VehicleAlarmBean alarm) {

//		StringBuilder update = new StringBuilder();
//		update.append("update sensor.ANA_VEHICLE_EVENT_" + alarm.getTableSuf() + " set FLAG_DID=1,DATIME_END=");
//		update.append("'").append(alarm.getDateTime()).append("'");
//		update.append(" where unid=").append("'").append(alarm.getUnid()).append("' and datime_begin<str_to_date('").append(alarm.getDateTime()).append("','%Y-%m-%d %H:%i:%s')");
//		try {
//			jdbcUtils = SingletonJDBC.getJDBC();
//			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
//		} catch (SQLException e) {
//			 log.error("错误",e);
//		}
		
		StringBuilder update = new StringBuilder();
		update.append("CALL `sensor`.`offAlarmting`('");
		update.append(alarm.getUnid());
		update.append("', '");
		update.append(alarm.getDateTime());
		update.append("', '");
		update.append(alarm.getTableSuf());
		update.append("')");
		try {
			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void alertBegin(VehicleAlarmBean alarm) {

		try {
			// connection = jdbc.getConnection();
			//String sql = "insert into sensor.ANA_VEHICLE_EVENT_" + alarm.getTableSuf()
				//	+ "(UNID,ENTITY_UNID,DOMAIN_UNID,SUMMARY,EVENT_TYPE,LAT_D,LON_D,CONTEXT,LEVEL,ERROR_CODE,DATIME_BEGIN) values(?,?,?,?,?,?,?,?,?,?,?)";
			
			String sql = "CALL `sensor`.`insertAlarmEvent`(?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?,?)";

			List<Object> params = new ArrayList<Object>();
			params.add(alarm.getUnid());
			params.add(alarm.getVehicleUnid());
			params.add(alarm.getDomainId());
			params.add(alarm.getDateTime());
			params.add(Double.parseDouble(alarm.getLng()));
			params.add(Double.parseDouble(alarm.getLat()));
			params.add("");//故障代码
			params.add("0");//故障代码unid
			params.add(alarm.getErrorName());
			params.add("alert");
			params.add("");//原始码流
			params.add(alarm.getLevel());
			params.add(alarm.getTableSuf());
			params.add("0");
			// System.out.println("CALL
			// `sensor`.`insertAlarmEvent`("+alamUnid+", "+unid+", "+domainId+",
			// "+event.getDatimeBegin()+",
			// "+alert.getLongitude()+","+alert.getLatitude()+",
			// "+event.getCode()+", "+errorCode.getUNID()+", "+errorCode != null
			// ? errorCode.getNAME() : ""+", "+eventType+", "+event.getHex()+",
			// "+errorCode.getLEVEL()+", "+tabeSuf+")");
			
			jdbcUtils = SingletonJDBC.getJDBC();
			jdbcUtils.updateByPreparedStatement(sql, params);
		} catch (SQLException e) {
			 log.error("错误",e);
		} finally {
		}
//		StringBuilder update = new StringBuilder();
//		update.append("update sensor.ANA_SNAPSHOT set DATIME_ALERT=");
//		update.append("'").append(alarm.getDateTime()).append("'");
//		update.append(",COUNT_ALERT=").append(1);
//		update.append(",LEVEL_ALERT=").append(alarm.getLevel());
//		// update.append(",NODE_UNID='").append("").append("'");
//		update.append(" where UNID='").append(alarm.getUnid()).append("'");
//		try {
//			jdbcUtils = SingletonJDBC.getJDBC();
//			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

}
