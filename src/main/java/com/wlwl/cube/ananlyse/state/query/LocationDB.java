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

import com.esotericsoftware.minlog.Log;
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

//	private static final String tableName = "DataAnalysis";
//	private static final String tableName_Charge = "DATAANALYSIS_CHARGE";
//	private static final String family_charge = "record";
//	private static final String family = "count";
//	private static final String CHARGERQUANTITY = "ChargeQuantity";
//	private static final String CHARGECOUNT = "ChargeCount";
	/**
	 * @Fields PERFIX : TODO(用一句话描述这个变量表示什么)
	 */
//	private static final String PERFIX = "DATAANALYSIS:";
	// private Map<String, VehicleStatisticBean> lastCharges = null;
	private long lastTime;

	private RedisUtils util = null;
	//private JdbcUtils jdbcUtils = null;
	private static final Logger log = LoggerFactory.getLogger(VehicleStatusFunction.class);
	SimpleDateFormat DEFAULT_DATE_SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Map<String, List<VehicleStatusBean>> statusData = null;

	public LocationDB() {
		util = RedisSingleton.instance();
		//jdbcUtils = SingletonJDBC.getJDBC();
		lastTime = System.currentTimeMillis();
	}

	public LocationDB(Map<String, List<VehicleStatusBean>> map) {
		util = RedisSingleton.instance();
		//jdbcUtils = SingletonJDBC.getJDBC();
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
				if (currentTime - lastTime > 1000 * 60 *5) {
					this.lastTime = currentTime;
					//updateNoOnline();
					loadData();
				}

			} catch (Exception ex) {
				log.error("错误",ex);
			}
		}

		
	}

	public List<String> bulkGetLocations(List<Long> userIds) {
		return null;
	}

	

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
					if (new Date().getTime() - StateUntils.strToDate(time).getTime() > 1000 * 60 * 5) {
						util.hset(str, Conf.ACTIVE_STATUS, "0");
					//	String unid = util.hget(str, "unid");
					//	alertEnd(unid);
					}
				} else {// 如果时间不存在
					util.hset(str, Conf.ACTIVE_STATUS, "0");
				//	String unid = util.hget(str, "unid");
					//alertEnd(unid);
				}
			}
			// }
		}
		set = null;
	}

	private static final String aiid_key = "ALARM_AIID:";

	private void alertEnd(String vehicleUnid) {
		
		JdbcUtils jdbcUtils =new JdbcUtils();
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
					log.error("错误",e);
				}
			}
		}
		jdbcUtils.releaseConn();
		util.del(aiid_key + vehicleUnid);
	}

	private void updateVehicleSatusNew(ObjectModelOfKafka omok) {
		
		log.info("信息01"+JsonUtils.serialize(omok));
		if (omok.getVehicle_UNID() == null) {
			return;
		}
		String vehicleUnid = omok.getVehicle_UNID().getValue();
		String timekey = Conf.STORM_TIMER + "status" + "ONLINETIERDEFAULT";
		String id = Conf.PERFIX + vehicleUnid;
		String field = "fiber_unid";
		String fiber_unid = util.hget(id, field);
		String currentStatus = util.hget(id, Conf.ACTIVE_STATUS);
		log.info("当前车辆状态："+currentStatus);
		if (currentStatus == null || (currentStatus != null && currentStatus.equals("0"))) {
			util.hset(id, Conf.ACTIVE_STATUS, "2");
			log.info("更新车辆状态：2");
			return;
		}
		if (fiber_unid == null) {
			System.out.println("车辆数据字典为空："+id);
			return;
		}
		Boolean isMatch = false;
		if (this.statusData.containsKey(fiber_unid)) {
			//LOG.info("信息02"+JsonUtils.serialize(omok));
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
					isMatch = true;
					
					//LOG.info("信息03:"+status+JsonUtils.serialize(omok));
					util.hset(id, Conf.ACTIVE_STATUS, String.valueOf(status));
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
						//LOG.info("信息04:2-"+JsonUtils.serialize(omok));
						//util.hset(id, Conf.DATIME_RX, omok.getDATIME_RX());
					}
				}
			} else {
				util.hset(timekey, "default", StateUntils.formate(new Date()));
			}
		}
	}


	// /**
	// *
	// * 加载数据库中数据，安装数据字典存储
	// */
	private void loadData() {
		String sql = "SELECT code,option,value,VALUE_LAST ,status,REMARKS,ALARM_LEVEL,ALARM_NAME,fiber_unid  FROM  cube.PDA_CUSTOM_SETUP where type=1 and flag_del=0 order by INX desc";
		List<Object> params = new CopyOnWriteArrayList<Object>();
		List<VehicleStatusBean> list = null;
		JdbcUtils jdbcUtils=null;
		try {
			//JdbcUtils jdbcUtils = SingletonJDBC.getJDBC();
			 jdbcUtils=new JdbcUtils();
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
			Log.info("加载数据成功！");;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Log.error("错误-加载数据",e);
		}finally{
			
			jdbcUtils.releaseConn();
			jdbcUtils=null;
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
