/**  
* @Title: VehicleStatistic.java
* @Package com.wlwl.cube.analyse.bean
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月19日 上午10:58:20
* @version V1.0.0  
*/
package com.wlwl.cube.analyse.bean;

import java.io.Serializable;
import java.util.Date;

import com.wlwl.cube.ananlyse.state.JsonUtils;

/**
 * @ClassName: VehicleStatistic
 * @Description: TODO车辆统计类
 * @author fenghai
 * @date 2016年9月19日 上午10:58:20
 *
 */
public class VehicleStatisticBean implements Serializable {

	private static final long serialVersionUID = -2947639427000962521L;
	// 统计日期
	private Date statisticDateTime;

	private String vehicle_unid;
	// 工作时间
	private float workTimeCount = 0;
	private Date workTimeDateTime_start;
	private Date workTimeDateTime_end;
	



	private Date workTimeDateTime_start_t;
	private Date workTimeDateTime_end_t;
	private Date workTimeDateTime_min_t;
	public Date getWorkTimeDateTime_min_t() {
		return workTimeDateTime_min_t;
	}

	public void setWorkTimeDateTime_min_t(Date workTimeDateTime_min_t) {
		this.workTimeDateTime_min_t = workTimeDateTime_min_t;
	}


	// 里程
	private Double workMileCount = 0.0;
	private Double workMile_start = 0.0;
	private Double workMile_end = 0.0;
	private Double workTotalMile = 0.0;
	// 电耗
	private Double workEnergyCount = 0.0;
	private Double workEnergy_start = 0.0;
	private Double workEnergy_end = 0.0;
	
	// 油耗
	private Double workFuleCount = 0.0;
	

	private Double workFule_start = 0.0;
	private Double workFule_end = 0.0;
	
	//充电状态
	private String chargeStatus="";
	
    private Double ChargeAll=0.0;
   
	
	
	/**
	* @return workTimeDateTime_start_t
	*/
	public Date getWorkTimeDateTime_start_t() {
		return workTimeDateTime_start_t;
	}

	/**
	* @param workTimeDateTime_start_t 要设置的 workTimeDateTime_start_t
	*/
	public void setWorkTimeDateTime_start_t(Date workTimeDateTime_start_t) {
		this.workTimeDateTime_start_t = workTimeDateTime_start_t;
	}

	/**
	* @return workTimeDateTime_end_t
	*/
	public Date getWorkTimeDateTime_end_t() {
		return workTimeDateTime_end_t;
	}

	/**
	* @param workTimeDateTime_end_t 要设置的 workTimeDateTime_end_t
	*/
	public void setWorkTimeDateTime_end_t(Date workTimeDateTime_end_t) {
		this.workTimeDateTime_end_t = workTimeDateTime_end_t;
	}

	/**
	* @return chargeAll
	*/
	public Double getChargeAll() {
		return ChargeAll;
	}

	/**
	* @param chargeAll 要设置的 chargeAll
	*/
	public void setChargeAll(Double chargeAll) {
		ChargeAll = chargeAll;
	}

	/**
	* @return chargeStatus 获取充电状态
	*/
	public String getChargeStatus() {
		return chargeStatus;
	}

	/**
	* @param chargeStatus 要设置的 chargeStatus 充电状态
	*/
	public void setChargeStatus(String chargeStatus) {
		this.chargeStatus = chargeStatus;
	}

	
	//故障次数
	private long  alarmCount=0;

	/**
	* @return alarmCount
	*/
	public long getAlarmCount() {
		return alarmCount;
	}

	/**
	* @param alarmCount 要设置的 alarmCount
	*/
	public void setAlarmCount(long alarmCount) {
		this.alarmCount = alarmCount;
	}

	/**
	 * @return statisticDateTime 统计时间
	 */
	public Date getStatisticDateTime() {
		return statisticDateTime;
	}

	/**
	 * @param statisticDateTime
	 *            设置统计时间
	 */
	public void setStatisticDateTime(Date statisticDateTime) {
		this.statisticDateTime = statisticDateTime;
	}

	/**
	 * @return vehicle_vin
	 */
	public String getVehicle_unid() {
		return vehicle_unid;
	}

	/**
	 * @param vehicle_vin
	 *            车辆vin号
	 */
	public void setVehicle_unid(String vehicle_vin) {
		this.vehicle_unid = vehicle_vin;
	}

	/**
	 * @return workTimeCount 工作总时间
	 */
	public float getWorkTimeCount() {
		return workTimeCount;
	}

	/**
	 * @param workTimeCount
	 *            设置工作总时间
	 */
	public void setWorkTimeCount(float workTimeCount) {
		this.workTimeCount = workTimeCount;
	}

	/**
	 * @return workTimeDateTime_start 最后一次工作开始时间
	 */
	public Date getWorkTimeDateTime_start() {
		return workTimeDateTime_start;
	}

	/**
	 * @param workTimeDateTime_start
	 *            设置最后一次开始时间
	 */
	public void setWorkTimeDateTime_start(Date workTimeDateTime_start) {
		this.workTimeDateTime_start = workTimeDateTime_start;
	}

	/**
	 * @return workTimeDateTime_end 最后一次结束时间
	 */
	public Date getWorkTimeDateTime_end() {
		return workTimeDateTime_end;
	}

	/**
	 * @param workTimeDateTime_end
	 *            设置最后一次结束时间
	 */
	public void setWorkTimeDateTime_end(Date workTimeDateTime_end) {
		this.workTimeDateTime_end = workTimeDateTime_end;
	}

	/**
	 * @return workMileCount 里程增量
	 */
	public Double getWorkMileCount() {
		return workMileCount;
	}

	/**
	 * @param workMileCount
	 *            设置里程增量
	 */
	public void setWorkMileCount(Double workMileCount) {
		this.workMileCount = workMileCount;
	}

	/**
	 * @return workMile_start 当天开始里程
	 */
	public Double getWorkMile_start() {
		return workMile_start;
	}

	/**
	 * @param workMile_start
	 *            要设置的 当天开始里程
	 */
	public void setWorkMile_start(Double workMile_start) {
		this.workMile_start = workMile_start;
	}

	/**
	 * @return workMile_end 当天结束里程
	 */
	public Double getWorkMile_end() {
		return workMile_end;
	}

	/**
	 * @param workMile_end
	 *            要设置的 当天结束里程
	 */
	public void setWorkMile_end(Double workMile_end) {
		this.workMile_end = workMile_end;
	}

	/**
	 * @return workEnergyCount能耗
	 */
	public Double getWorkEnergyCount() {
		return workEnergyCount;
	}

	/**
	 * @param workEnergyCount
	 *            要设置的 能耗
	 */
	public void setWorkEnergyCount(Double workEnergyCount) {
		this.workEnergyCount = workEnergyCount;
	}

	/**
	 * @return workEnergy_start 当天开始能耗
	 */
	public Double getWorkEnergy_start() {
		return workEnergy_start;
	}

	/**
	 * @param workEnergy_start
	 *            要设置的 当天开始能耗
	 */
	public void setWorkEnergy_start(Double workEnergy_start) {
		this.workEnergy_start = workEnergy_start;
	}

	/**
	 * @return workEnergy_end 结束能耗
	 */
	public Double getWorkEnergy_end() {
		return workEnergy_end;
	}

	/**
	 * @param workEnergy_end
	 *            要设置的 结束能耗
	 */
	public void setWorkEnergy_end(Double workEnergy_end) {
		this.workEnergy_end = workEnergy_end;
	}



	/**
	 * @return workTotalMile
	 */
	public Double getWorkTotalMile() {
		return workTotalMile;
	}

	/**
	 * @param workTotalMile
	 *            要设置的 workTotalMile
	 */
	public void setWorkTotalMile(Double workTotalMile) {
		this.workTotalMile = workTotalMile;
	}

	@Override
	public String toString() {
		return JsonUtils.serialize(this);

	}
	
	/**
	* @return workFuleCount
	*/
	public Double getWorkFuleCount() {
		return workFuleCount;
	}

	/**
	* @param workFuleCount 要设置的 workFuleCount
	*/
	public void setWorkFuleCount(Double workFuleCount) {
		this.workFuleCount = workFuleCount;
	}

	/**
	* @return workFule_start
	*/
	public Double getWorkFule_start() {
		return workFule_start;
	}

	/**
	* @param workFule_start 要设置的 workFule_start
	*/
	public void setWorkFule_start(Double workFule_start) {
		this.workFule_start = workFule_start;
	}

	/**
	* @return workFule_end
	*/
	public Double getWorkFule_end() {
		return workFule_end;
	}

	/**
	* @param workFule_end 要设置的 workFule_end
	*/
	public void setWorkFule_end(Double workFule_end) {
		this.workFule_end = workFule_end;
	}

}
