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

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.ananlyse.state.TimeBaseRowStrategy;
import com.wlwl.cube.hbase.HBaseUtils;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;

/**
 * @ClassName: VehicleAlarmFetchFunction
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月18日 下午9:42:59
 *
 */
public class VehicleAlarmFetchFunction extends BaseFunction {

	private static final long serialVersionUID = 8414621340097218898L;

	

    private Map<String ,VehicleStatisticBean> vehiclesMap;
//	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private long lastTime;
	//private Map<String, VehicleStatisticBean> lastCharges = null;
	//private long lastTime;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {

		jdbcUtils =SingletonJDBC.getJDBC();
		vehiclesMap=new HashMap<String ,VehicleStatisticBean>();
		lastTime = System.currentTimeMillis();
		
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
		
		String device = tuple.getStringByField("deviceId"); 
		
		ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");
		
		VehicleStatisticBean vehicle = new VehicleStatisticBean();
	
		vehicle.setVehicle_unid(device);
		
		vehicle.setStatisticDateTime(omok.getTIMESTAMP());
		
		collector.emit(new Values(vehicle));
		
		saveMap(vehicle);
		
		if (currentTime >= lastTime + 1000 * 60 *30) {
			
			lastTime=currentTime;

			updateAlarm();
			
			cleanMap();
				
		}	
		
	}
	
	/**
	* @Title: saveMap
	* @Description: 保存在线车辆缓存
	* @param @param vehicle    设定文件
	* @return void    返回类型
	* @throws
	*/ 
	private void saveMap(VehicleStatisticBean vehicle)
	{
		String id=vehicle.getVehicle_unid()+StateUntils.formateDay(vehicle.getStatisticDateTime());
		if(!vehiclesMap.containsKey(id))
		{
		  vehiclesMap.put(id,vehicle);
		}
	}
	/**
	* @Title: cleanMap
	* @Description: 清空缓存
	* @param     设定文件
	* @return void    返回类型
	* @throws
	*/ 
	private void cleanMap()
	{
		vehiclesMap.clear();
	}
	
	private void updateAlarm()
	{
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
	 * 设定文件 @return void 返回类型 @throws
	 */
	private void insertDataForAlarm(VehicleStatisticBean vehicle) {
		
		String sql = "SELECT count(*) alarmCount FROM sensor.ANA_VEHICLE_EVENT where unid=?  DATIME_BEGIN between  STR_TO_DATE(?,\"%Y-%m-%d %H:%i:%s\") and  STR_TO_DATE(?,\"%Y-%m-%d %H:%i:%s\")";
		List<Object> params = new ArrayList<Object>();
		params.add(vehicle.getVehicle_unid());
		params.add(StateUntils.formateDay(vehicle.getStatisticDateTime()) + " 00:00:00");
		params.add(StateUntils.formateDay(vehicle.getStatisticDateTime()) + " 23:59:59");
		Map<String, Object> list = null;
		try {
			list = jdbcUtils.findSimpleResult(sql, params);

			HBaseUtils.insert(Conf.TABLENAME, TimeBaseRowStrategy.getRowKeyForHase(vehicle), Conf.FAMILY,
					"alarmCount", list.get("alarmCount").toString());
			
		} catch (SQLException e) {
			e.printStackTrace();
			jdbcUtils.releaseConn();
		} finally{
			//
		}

	}

}
