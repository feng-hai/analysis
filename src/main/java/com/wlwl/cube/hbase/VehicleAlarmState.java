/**  
* @Title: RedisState.java
* @Package com.wlwl.cube.redis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月25日 上午6:36:56
* @version V1.0.0  
*/
package com.wlwl.cube.hbase;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.storm.trident.state.State;

import com.wlwl.cube.analyse.bean.UNID;
import com.wlwl.cube.analyse.bean.VehicleAlarmBean;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;

/**
 * @ClassName: RedisState
 * @Description: TODO更新redis的具体类
 * @author fenghai
 * @date 2016年9月25日 上午6:36:56
 *
 */
public class VehicleAlarmState implements State {

	private JdbcUtils jdbcUtils = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.storm.trident.state.State#beginCommit(java.lang.Long)
	 */
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.storm.trident.state.State#commit(java.lang.Long)
	 */
	public void commit(Long txid) {
		// TODO Auto-generated method stub

	}

	public void setVehicleBulk(List<VehicleAlarmBean> alarmList) {
		for (VehicleAlarmBean alarm : alarmList) {
			if (alarm.getIsBegin()) {
					alertBegin(alarm);
			} else {
				alertEnd(alarm);
			}
		}
	}

	public List<VehicleAlarmBean> bulkGetVehicles(List<String> vehicleIDs) {

		return null;
	}

	private void alertEnd(VehicleAlarmBean alarm) {

		StringBuilder update = new StringBuilder();
		update.append("update sensor.ANA_VEHICLE_EVENT_" + alarm.getTableSuf() + " set FLAG_DID=1,DATIME_END=");
		update.append("'").append(alarm.getDateTime()).append("'");
		update.append(" where unid=").append("'").append(alarm.getUnid()).append("'");
		try {
			jdbcUtils = SingletonJDBC.getJDBC();
			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void alertBegin(VehicleAlarmBean alarm)  {

		try {
			// connection = jdbc.getConnection();
			String sql = "insert into sensor.ANA_VEHICLE_EVENT_" + alarm.getTableSuf()
					+ "(UNID,ENTITY_UNID,DOMAIN_UNID,SUMMARY,EVENT_TYPE,LAT_D,LON_D,CONTEXT,LEVEL,ERROR_CODE) values(?,?,?,?,?,?,?,?,?,?)";
			List<Object> params = new ArrayList<Object>();
			params.add(alarm.getUnid());
			params.add(alarm.getVehicleUnid());
			params.add(alarm.getDomainId());
			params.add(alarm.getErrorName());
			params.add("");
			params.add(alarm.getLat());
			params.add(alarm.getLng());
			params.add("");
			params.add(alarm.getLevel());
			params.add("");
			jdbcUtils = SingletonJDBC.getJDBC();
			int aiid = jdbcUtils.insertByPreparedStatement(sql, params);

		} catch (SQLException e) {
			// LOG.severe( "get connection锛� " + e.getLocalizedMessage() );
		} finally {
			/// closeConnection( connection );
			// jdbc.releaseConn();
		}
		StringBuilder update = new StringBuilder();
		update.append("update sensor.ANA_SNAPSHOT set DATIME_ALERT=");
		update.append("'").append(alarm.getDateTime()).append("'");
		update.append(",COUNT_ALERT=").append(1);
		update.append(",LEVEL_ALERT=").append(alarm.getLevel());
		// update.append(",NODE_UNID='").append("").append("'");
		update.append(" where UNID='").append(alarm.getUnid()).append("'");
		try {
			jdbcUtils = SingletonJDBC.getJDBC();
			jdbcUtils.updateByPreparedStatement(update.toString(), new ArrayList<Object>());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
