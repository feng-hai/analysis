/**  
* @Title: VehicleStatusFunction.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(��һ�仰�������ļ���ʲô)
* @author fenghai  
* @date 2016��9��16�� ����10:13:29
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;

import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;

import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.mysql.SingletonJDBC;
import com.wlwl.cube.redis.RedisSingleton;
import com.wlwl.cube.redis.RedisUtils;

/**
 * @ClassName: VehicleStatusFetchFunction
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月18日 下午9:42:33
 *
 */
public class VehicleStatusFunction extends BaseFunction {

	private static final long serialVersionUID = -353300203591905325L;

	private RedisUtils util = null;
	private JdbcUtils jdbcUtils = null;
	private static final Logger LOG = LoggerFactory.getLogger(VehicleStatusFunction.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		util = RedisSingleton.instance();
		jdbcUtils = SingletonJDBC.getJDBC();
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.trident.operation.BaseOperation#cleanup()
	 * 退出时清理链接
	 */
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
			ObjectModelOfKafka omok = (ObjectModelOfKafka) tuple.getValueByField("vehicle");
			// LOG.debug(JsonUtils.serialize(omok));

			String device = tuple.getStringByField("deviceId");

			// 更新车辆在线状态
			updateVehicleStatus(omok, device);
			// 定时更新判断条件
			updateCondition(device);
			// 定时更新在线状态
			updateNoOnline();

			collector.emit(new Values(omok));

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	/**
	 * @Title: updateCondition @Description: TODO固定的时间更新一下判断条件 @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private void updateCondition(String device) {
		String timekey = Conf.STORM_TIMER + device;
		String timer = util.hget(timekey, Conf.ACTIVE_CONDITION_TIMER);
		if (timer != null) {
			Date date = StateUntils.strToDate(timer);
			if (date != null) {
				long m = new Date().getTime() - date.getTime();
				if (m > 1000 * 60 * 2) {
					util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER, StateUntils.formate(new Date()));
					// 更新数据
					String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + device;
					Map<String, String> map = setRedis(device);
					if (map.size() > 0) {
						util.del(vehicleStatus);
						util.hmset(vehicleStatus, map);
					}
				}
			} else {
				util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER, StateUntils.formate(new Date()));
			}

		} else {
			util.hset(timekey, Conf.ACTIVE_CONDITION_TIMER, StateUntils.formate(new Date()));
		}

	}

	/**
	 * 检查5分钟没有上线的车辆，并把车辆设置为离线状态
	 */
	private void updateNoOnline() {
		String timekey = Conf.STORM_TIMER + "ONLINETIER";
		String timer = util.hget(timekey, Conf.ACTIVE_ONLINE_TIMER);
		if (timer != null) {
			Date date = StateUntils.strToDate(timer);
			if (date != null) {
				long m = new Date().getTime() - date.getTime();
				if (m > 1000 * 60 * 5) {
					util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER, StateUntils.formate(new Date()));
					// 更新数据
					checkOnLine();
				}
			} else {
				checkOnLine();
				util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER, StateUntils.formate(new Date()));
			}

		} else {
			checkOnLine();
			util.hset(timekey, Conf.ACTIVE_ONLINE_TIMER, StateUntils.formate(new Date()));
		}

	}

	/**
	 * @param omok    读取kafka的实体类对象
	 * @param device  车辆唯一标识 vehicle_unid
	 */
	private void updateVehicleStatus(ObjectModelOfKafka omok, String device) {

		String vehicleStatus = Conf.VEHICLE_CONDITION_STATUS + device;
		String id = Conf.PERFIX + device;

		// 從redis中獲取數據
		Map<String, String> map = util.hgetall(vehicleStatus);
		// 判斷redis中是否有數據
		if (map.size() == 0) {
			// redis 中沒有數據，從數據庫中讀取並複製
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

				if (pair != null) {
					String value = pair.getValue();
					Boolean isTrue = statusBean.checkStatus(value);
					if (isTrue) {
						isMatch = true;
						util.hset(id, Conf.ACTIVE_STATUS, key);
						util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
						break;
					}
				}
			}
		}
		if (!isMatch)// 设置上线默认值
		{

			//String currentStatus = util.hget(id, Conf.ACTIVE_STATUS);
			//System.out.println(omok.getDATIME_RX());
			//System.out.println(currentStatus);
			//if (currentStatus ==null ||(currentStatus != null && currentStatus.equals("0"))) {
				util.hset(id, Conf.ACTIVE_STATUS, "2");
				util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
			//}
			isMatch = false;

		}

	}

	/**
	 * @return @Title: setRedis @Description: TODO(这里用一句话描述这个方法的作用) @param
	 *         设定文件 @return void 返回类型 @throws
	 */
	private Map<String, String> setRedis(String vehicleUnid) {

		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String sql = "SELECT code,option,value,VALUE_LAST ,status  FROM cube.PDA_VEHICLE_DETAIL where fiber_unid=? and type=1 order by INX desc";
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

	/**
	 * @Title: checkOnLine @Description:
	 *         TODO长时间不更新的数据，状态更新为离线，此方法定时运行，五分钟一次 @param 设定文件 @return void
	 *         返回类型 @throws
	 */
	public void checkOnLine() {
		Set<String> set = util.keys(Conf.PERFIX + "*");
		for (String str : set) {
			String status = util.hget(str, Conf.ACTIVE_STATUS);
			if (status != null && status != "0") {
				String time = util.hget(str, Conf.DATIME_RX);
				if (time != null) {// 如果时间存在
					if (new Date().getTime() - StateUntils.strToDate(time).getTime() > 1000 * 60 * 2) {
						util.hset(str, Conf.ACTIVE_STATUS, "0");
					}
				} else {// 如果时间不存在
					util.hset(str, Conf.ACTIVE_STATUS, "0");
				}
			}
		}
		set = null;
	}
}
