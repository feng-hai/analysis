/**  
* @Title: SaveValueToRedisFunction.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月27日 下午7:04:17
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import java.util.Date;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.analyse.bean.Pair;
import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cube.ananlyse.state.TimeBaseRowStrategy;
import com.wlwl.cube.mysql.JdbcUtils;
import com.wlwl.cube.redis.RedisUtils;
import com.wlwl.cuble.analyse.storager.IStorager;
import com.wlwl.cuble.analyse.storager.RedisInstance;
import com.wlwl.cuble.analyse.storager.StoragerSingleton;

/**
 * @ClassName: SaveValueToRedisFunction
 * @Description: TODO统计里程数据
 * @author fenghai
 * @date 2016年9月27日 下午7:04:17
 *
 */
public class SaveValueToRedisFunction extends BaseFunction {

	private static final long serialVersionUID = 4608482736186526306L;
	private static final String preKey = "BIG_ANALYSIS:";

	// private RedisUtils util = null;
	private IStorager<VehicleStatisticBean> redis = null;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// util = new RedisUtils();
		redis = StoragerSingleton.getInstance();
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

			Pair totalMile = omok.getAllMile();
			Pair totalEnergy = omok.getAllEnergy();
			Pair totalFule = omok.getTotalFuel();
			String time = omok.getDATIME_RX();
			String device = tuple.getStringByField("deviceId");

			String id = preKey + TimeBaseRowStrategy.getRowKeyForRedis(time, device);

			// 判断key是否存在，如果不存在，创建，如果存在累计
			if (redis.keyExists(id)) {
				// 获取Redis中上次存储的值
				VehicleStatisticBean vehicle = redis.getStorager(id);
				// 更新公共数据
				setPublicValue(vehicle, omok, device);
				
				
				vehicle.setWorkTimeDateTime_temp(0);

				// 结束时间为空时
				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != ""
						&& vehicle.getWorkTimeDateTime_end() == null) {
					vehicle.setWorkTimeDateTime_end(StateUntils.strToDate(omok.getDATIME_RX()));
				}

				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != ""
						&& vehicle.getWorkTimeDateTime_end_t() == null) {
					vehicle.setWorkTimeDateTime_end_t(StateUntils.strToDate(omok.getDATIME_RX()));
				}

				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != ""
						&& vehicle.getWorkTimeDateTime_start_t() == null) {
					vehicle.setWorkTimeDateTime_start_t(StateUntils.strToDate(omok.getDATIME_RX()));
				}

				// 工作时间统计
				// 工作时间统计
				Date tempDate = StateUntils.strToDate(omok.getDATIME_RX());
				if (tempDate.getTime() > vehicle.getWorkTimeDateTime_end_t().getTime()) {
					
					if (tempDate.getTime() - vehicle.getWorkTimeDateTime_end_t().getTime() >= 1000 * 60 * 5)// 判断是两次工作
					{
						vehicle.setWorkTimeDateTime_start_t(tempDate);
						vehicle.setWorkTimeDateTime_min_t(tempDate);
					}
					vehicle.setWorkTimeDateTime_end_t(tempDate);
					vehicle.setWorkTimeCount(vehicle.getWorkTimeCount() + vehicle.getWorkTimeDateTime_end_t().getTime()
							- vehicle.getWorkTimeDateTime_start_t().getTime());
					//设置差值
					vehicle.setWorkTimeDateTime_temp(vehicle.getWorkTimeDateTime_end_t().getTime()
							- vehicle.getWorkTimeDateTime_start_t().getTime());
					
					vehicle.setWorkTimeDateTime_start_t(vehicle.getWorkTimeDateTime_end_t());
					
					if (vehicle.getWorkTimeCount() / 3600000 > 24) {
						vehicle.setWorkTimeCount(vehicle.getWorkTimeDateTime_end().getTime()
								- vehicle.getWorkTimeDateTime_start().getTime());
					}
					//System.out.println(vehicle.getWorkTimeCount());

				} else if (tempDate.getTime() < vehicle.getWorkTimeDateTime_min_t().getTime()) {
					vehicle.setWorkTimeCount(vehicle.getWorkTimeCount() + vehicle.getWorkTimeDateTime_min_t().getTime()
							- tempDate.getTime());
					//设置差值
					vehicle.setWorkTimeDateTime_temp(vehicle.getWorkTimeDateTime_min_t().getTime()
							- tempDate.getTime());
					vehicle.setWorkTimeDateTime_min_t(tempDate);
				}

				//System.out.println(vehicle.getWorkTimeCount());

				// 判断当前数据和redis中数据时间对比，如果大于更新最大值，如果小于 更新最小值
				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != "" && StateUntils
						.strToDate(omok.getDATIME_RX()).getTime() > vehicle.getWorkTimeDateTime_end().getTime()) {

					// 更新结束时间
					vehicle.setWorkTimeDateTime_end(StateUntils.strToDate(omok.getDATIME_RX()));
				}
				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != "" && StateUntils
						.strToDate(omok.getDATIME_RX()).getTime() < vehicle.getWorkTimeDateTime_start().getTime()) {
					// 更新结束时间
					vehicle.setWorkTimeDateTime_start(StateUntils.strToDate(omok.getDATIME_RX()));
				}

				if (totalMile != null && Double.parseDouble(totalMile.getValue()) > 0
						&& Double.parseDouble(totalMile.getValue()) - vehicle.getWorkMile_start() >= 0) {
					// 里程 最大值保存 过滤不正确的里程值
					if (Double.parseDouble(totalMile.getValue()) > vehicle.getWorkMile_end()) {
						vehicle.setWorkMile_end(Double.parseDouble(totalMile.getValue()));
						vehicle.setWorkTotalMile(Double.parseDouble(totalMile.getValue()));
						if (vehicle.getWorkMile_start() == 0) {
							vehicle.setWorkMile_start(vehicle.getWorkMile_end());
						}
					}

					// 里程 最小值保存
					if (Double.parseDouble(totalMile.getValue()) < vehicle.getWorkMile_start()) {
						vehicle.setWorkMile_start(Double.parseDouble(totalMile.getValue()));
					}

					if (vehicle.getWorkMile_end() - vehicle.getWorkMile_start() > 0
							&& vehicle.getWorkMile_end() - vehicle.getWorkMile_start() < 500) {
						// 更新当日累计里程
						vehicle.setWorkMileCount(vehicle.getWorkMile_end() - vehicle.getWorkMile_start());
					}

				}

				if (totalEnergy != null && Double.parseDouble(totalEnergy.getValue()) > 0
						&& Double.parseDouble(totalEnergy.getValue()) - vehicle.getWorkEnergy_start() >= 0) {
					// 电耗 最大值保存
					if (Double.parseDouble(totalEnergy.getValue()) > vehicle.getWorkEnergy_end()) {
						vehicle.setWorkEnergy_end(Double.parseDouble(totalEnergy.getValue()));
						if (vehicle.getWorkEnergy_start() == 0) {
							vehicle.setWorkEnergy_start(vehicle.getWorkEnergy_end());
						}
					}
					// 电耗 最小值保存
					if (Double.parseDouble(totalEnergy.getValue()) < vehicle.getWorkEnergy_start()) {
						vehicle.setWorkEnergy_start(Double.parseDouble(totalEnergy.getValue()));

					}
					if (vehicle.getWorkEnergy_end() - vehicle.getWorkEnergy_start() > 0) {
						// 更新当日 电耗
						vehicle.setWorkEnergyCount(vehicle.getWorkEnergy_end() - vehicle.getWorkEnergy_start());
					}

				}

				if (totalFule != null && Double.parseDouble(totalFule.getValue()) > 0
						&& Double.parseDouble(totalFule.getValue()) - vehicle.getWorkFule_start() >= 0) {
					// 能耗 最大值保存
					if (Double.parseDouble(totalFule.getValue()) > vehicle.getWorkFule_end()) {
						vehicle.setWorkFule_end(Double.parseDouble(totalFule.getValue()));
						if (vehicle.getWorkFule_start() == 0) {
							vehicle.setWorkFule_start(vehicle.getWorkFule_end());
						}
					}
					// 能耗 最小值保存
					if (Double.parseDouble(totalMile.getValue()) < vehicle.getWorkFule_start()) {
						vehicle.setWorkFule_start(Double.parseDouble(totalFule.getValue()));
					}
					if (vehicle.getWorkFule_end() - vehicle.getWorkFule_start() > 0) {
						// 更新当日 能耗
						vehicle.setWorkFuleCount(vehicle.getWorkFule_end() - vehicle.getWorkFule_start());
					}
				}
				redis.setStorager(id, vehicle);
				collector.emit(new Values(vehicle));

			} else {

				//
				// String yestodayID=preKey +
				// TimeBaseRowStrategy.getRowKeyForRedisBefore(time,device);
				// if (redis.keyExists(yestodayID)) {
				// // 获取Redis中上次存储的值
				// VehicleStatisticBean vehicleY =
				// redis.getStorager(yestodayID);
				// vehicleY.setWorkTimeCount(vehicleY.getWorkTimeDateTime_end_t().getTime()-vehicleY.getWorkTimeDateTime_start_t().getTime());
				// collector.emit(new Values(vehicleY));
				// }

				// 添加新的记录
				VehicleStatisticBean vehicle = new VehicleStatisticBean();
				// 更新公共数据
				setPublicValue(vehicle, omok, device);

				// 开始时间
				if (totalMile != null && Double.parseDouble(totalMile.getValue()) > 0.0) {
					vehicle.setWorkMile_start(Double.parseDouble(totalMile.getValue()));
					vehicle.setWorkMile_end(Double.parseDouble(totalMile.getValue()));
					vehicle.setWorkTotalMile(Double.parseDouble(totalMile.getValue()));
				}

				if (totalEnergy != null && Double.parseDouble(totalEnergy.getValue()) > 0.0) {
					// 开始能耗
					vehicle.setWorkEnergy_start(Double.parseDouble(totalEnergy.getValue()));
					vehicle.setWorkEnergy_end(Double.parseDouble(totalEnergy.getValue()));
				}
				if (totalFule != null && Double.parseDouble(totalFule.getValue()) > 0.0) {
					// 开始油耗
					vehicle.setWorkFule_start(Double.parseDouble(totalFule.getValue()));
					vehicle.setWorkFule_end(Double.parseDouble(totalFule.getValue()));
				}

				if (omok.getDATIME_RX() != null && omok.getDATIME_RX() != "") {
					vehicle.setWorkTimeDateTime_start(StateUntils.strToDate(omok.getDATIME_RX()));
					vehicle.setWorkTimeDateTime_end(StateUntils.strToDate(omok.getDATIME_RX()));
					// 获取最后一次工作时间
					vehicle.setWorkTimeDateTime_end_t(StateUntils.strToDate(omok.getDATIME_RX()));
					vehicle.setWorkTimeDateTime_start_t(StateUntils.strToDate(omok.getDATIME_RX()));
					redis.setStorager(id, vehicle);
					collector.emit(new Values(vehicle));
					// 删除昨天的缓存信息
					String yestodayID = preKey + TimeBaseRowStrategy.getRowKeyForRedisBefore(time, device);
					if (redis.keyExists(yestodayID)) {
						redis.deleteByKey(yestodayID);
					}
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * @Title: setPublicValue @Description: TODO更新充电、车辆unid @param @param
	 *         vehicle @param @param omok @param @param unid 设定文件 @return void
	 *         返回类型 @throws
	 */
	private void setPublicValue(VehicleStatisticBean vehicle, ObjectModelOfKafka omok, String unid) {
		Pair chargerStatus = omok.getChargeStatus();
		Pair chargeAll = omok.getChargeAll();
		vehicle.setVehicle_unid(unid);
		if (Double.parseDouble(chargeAll.getValue()) > 0) {
			vehicle.setChargeAll(Double.parseDouble(chargeAll.getValue()));
		}
		vehicle.setChargeStatus(chargerStatus.getValue());

		vehicle.setStatisticDateTime(omok.getTIMESTAMP());
	}

}
