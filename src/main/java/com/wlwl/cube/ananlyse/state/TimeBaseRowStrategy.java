package com.wlwl.cube.ananlyse.state;

import java.io.Serializable;
import java.util.Date;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

public class TimeBaseRowStrategy implements Serializable {

	private static final long serialVersionUID = 2425204078500442631L;

	/**
	 * @Title: getRowKeyForRedis @Description: TODO组装rediskey @param @param
	 *         unid @param @return 设定文件 @return String 返回类型 @throws
	 */
	public static String getRowKeyForRedis(String time, String unid) {
		return StateUntils.formateDay(StateUntils.strToDate(time)) + "-" + unid;
	}

	/**
	 * @Title: getRowKeyForRedis @Description: 昨天rediskey
	 * @param @param
	 * unid @param @return 设定文件 @return String 返回类型 @throws
	 */
	public static String getRowKeyForRedisBefore(String time, String unid) {
		Date date = new Date(StateUntils.strToDate(time).getTime() - 1000 * 60 * 60 * 24);
		return StateUntils.formateDay(date) + "-" + unid;
	}

	/**
	 * @Title: getRowKeyForRedis @Description: TODO组装rediskey @param @param
	 *         unid @param @return 设定文件 @return String 返回类型 @throws
	 */
	public static String getRowKeyForHase(VehicleStatisticBean vehicle) {
		return StateUntils.formateDay(vehicle.getWorkTimeDateTime_end()) + "-" + vehicle.getVehicle_unid();
	}
	
	/**
	 * @Title: getRowKeyForRedis @Description: TODO组装rediskey @param @param
	 *         unid @param @return 设定文件 @return String 返回类型 @throws
	 */
	public static String getRowKeyFor2Hase(VehicleStatisticBean vehicle) {
		return Long.toString(vehicle.getWorkTimeDateTime_end().getTime()) + "-" + vehicle.getVehicle_unid();
	}

}
