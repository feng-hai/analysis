/**  
* @Title: ChargeBean.java
* @Package com.wlwl.cube.analyse.bean
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月12日 下午6:57:30
* @version V1.0.0  
*/ 
package com.wlwl.cube.analyse.bean;

import java.util.Date;

/**
* @ClassName: ChargeBean
* @Description: TODO 每次充电量
* @author fenghai
* @date 2016年10月12日 下午6:57:30
*
*/
public class ChargeBean {
	
	
	private Date startDate=new Date();
	private Date endDate=new Date();
	private Double startCharge=0.0;
	private Double endCharger=0.0;
	private Double startSOC=0.0;
	/**
	* @return startSOC
	*/
	public Double getStartSOC() {
		return startSOC;
	}
	/**
	* @param startSOC 要设置的 startSOC
	*/
	public void setStartSOC(Double startSOC) {
		this.startSOC = startSOC;
	}
	/**
	* @return endSOC
	*/
	public Double getEndSOC() {
		return endSOC;
	}
	/**
	* @param endSOC 要设置的 endSOC
	*/
	public void setEndSOC(Double endSOC) {
		this.endSOC = endSOC;
	}
	private Double endSOC=0.0;
	/**
	* @return startDate
	*/
	public Date getStartDate() {
		return startDate;
	}
	/**
	* @param startDate 要设置的 startDate
	*/
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	/**
	* @return endDate
	*/
	public Date getEndDate() {
		return endDate;
	}
	/**
	* @param endDate 要设置的 endDate
	*/
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	

	/**
	* @return startCharge
	*/
	public Double getStartCharge() {
		return startCharge;
	}
	/**
	* @param startCharge 要设置的 startCharge
	*/
	public void setStartCharge(Double startCharge) {
		this.startCharge = startCharge;
	}
	/**
	* @return endCharger
	*/
	public Double getEndCharger() {
		return endCharger;
	}
	/**
	* @param endCharger 要设置的 endCharger
	*/
	public void setEndCharger(Double endCharger) {
		this.endCharger = endCharger;
	}


}
