package com.wlwl.cube.analyse.common;

/**
* @ClassName: Conf
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月14日 上午11:23:23
*
*/ 
/**
* @ClassName: Conf
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月14日 上午11:46:52
*
*/ 
/**
* @ClassName: Conf
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月14日 下午2:11:50
*
*/ 
/**
* @ClassName: Conf
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月16日 上午10:25:58
*
*/ 
public class Conf2 {
	/* Redis 设置*/
	public static final String REDIS_HOST_KEY = "";
	public static final String REDIS_PORT_KEY = "";
	/* Kafka设置 */
	public static final String BOOTSTRAP_SERVERS = "";
	/* hbase 设置 */
	public static final String HBASE_HOST_KEY = "";
	public static final String HBASE_PORT_KEY = "";
	//
	/**
	* @Fields PERFIX : 保存车辆在线信息的前缀信息
	*/ 
	public  static final String PERFIX = "BIG_VEHICLE:";
	
	/**
	* @Fields ACTIVE_STATUS : 定义车辆在线状态在redis中 的key值
	*/ 
	public  static final String ACTIVE_STATUS = "ACTIVE_STATUS";
	
	/**
	* @Fields DATIME_RX : 在綫狀態更新時間
	*/ 
	public  static final String DATIME_RX = "DATIME_RX";
	/**
	* @Fields STORM_TIMER : 保存Storm临时时间
	*/ 
	public  static final String STORM_TIMER="STORM_TIMER:";
	
	/**
	* @Fields ACTIVE_TIMER :定义  Storm_timer 临时时间中，定时更换判断条件的时间key，
	*/ 
	public  static final String ACTIVE_CONDITION_TIMER = "ACTIVE_CONDITION_TIMER";
	/**
	* @Fields ACTIVE_CONDITION_ALARM : 临时时间中，定时查询报警数据的判断条件 存在redis中的key值
	*/ 
	public  static final String ACTIVE_CONDITION_ALARM = "ACTIVE_CONDITION_ALARM";
    /**
    * @Fields ACTIVE_ONLINE_TIMER : 定时更新在线状态的定时器
    */ 
    public  static final String ACTIVE_ONLINE_TIMER="ACTIVE_ONLINE_TIMER";
  
    /**
    * @Fields ACTIVE_CHARGE_TIMER :充电判断条件加载控制时间
    */ 
    public   static final String ACTIVE_CHARGE_TIMER="ACTIVE_CHARGE_TIMER";
	
	/**
	* @Fields VEHICLE_STATUS : 定义车辆状态判断条件，在redis中存储的前缀
	*/ 
	public static final String VEHICLE_CONDITION_STATUS = "VEHICLE_STATUS_STATE:";
	
	/**
	* @Fields VEHICLE_CONDITION_CHARGE : 车辆充电的判断条件，存储key
	*/ 
	public static final String VEHICLE_CONDITION_CHARGE= "VEHICLE_STATUS_STATE:";
	
//	/**
//	* @Fields ZKURL : zookeeper 地址
//	*/ 
//	public static final String  ZKURL="dn00-dev.wlwl.com:2181,dn01-dev.wlwl.com:2181,dn02-dev.wlwl.com:2181,dn03-dev.wlwl.com:2181,dn04-dev.wlwl.com:2181";
//	/**
//	* @Fields BROKERURL : storm节点地址BrokerUrl
//	*/ 
//	public static final String BROKERURL="dn00-dev.wlwl.com:9092,dn01-dev.wlwl.com:9092,dn02-dev.wlwl.com:9092,dn03-dev.wlwl.com:9092,dn04-dev.wlwl.com:9092";
//	
	/**
	* @Fields ZKURL : zookeeper 地址
	*/ 
	public static final String  ZKURL="namenode.cube:2181,maria.cube:2181,hyperrouter1.cube:2181,hyperrouter2.cube:2181";
	/**
	* @Fields BROKERURL : storm节点地址BrokerUrl
	*/ 
	public static final String BROKERURL="maria.cube:9092,namenode.cube:9092,datanode1.cube:9092,hyperrouter1.cube:9092,hyperrouter2.cube:9092,hyperrouter4.cube:9092";
	
	
	/**
	* @Fields TABLENAME : 统计数据HBASE表名称
	*/ 
	public static final String TABLENAME="DataAnalysis";
	
	
	/**
	* @Fields FAMILY : 统计数据列族信息名称
	*/ 
	public static final String FAMILY="count";
	
	
	public static final String REDISURL="10.117.17.40";
	
	
	public static final String MYSQLURL="10.252.248.176:3306";//10.60.60.90:3306
	
	public static final String  MYSQLPASSWORD="STu2YW1bDF4p.pjA";


}
