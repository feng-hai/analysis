package com.wlwl.cube.analyse.bean;

import java.io.Serializable;



public class VehicleAlarmBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String unid;
	private String vehicleUnid;
	private String domainId;
	private String lat;
	private String lng;
	private Integer level;
	private String errorName;
	private String dateTime;
	private String code;
	//表后缀
	private String tableSuf;
	
	public String getTableSuf() {
		return tableSuf;
	}
	public void setTableSuf(String tableSuf) {
		this.tableSuf = tableSuf;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public Boolean getIsBegin() {
		return isBegin;
	}
	public void setIsBegin(Boolean isBegin) {
		this.isBegin = isBegin;
	}
	private Boolean isBegin=true;
	
	public String getUnid() {
		return unid;
	}
	public void setUnid(String unid) {
		this.unid = unid;
	}
	public String getVehicleUnid() {
		return vehicleUnid;
	}
	public void setVehicleUnid(String vehicleUnid) {
		this.vehicleUnid = vehicleUnid;
	}
	public String getDomainId() {
		return domainId;
	}
	public void setDomainId(String domainId) {
		this.domainId = domainId;
	}
	public String getLat() {
		return lat;
	}
	public void setLat(String lat) {
		this.lat = lat;
	}
	public String getLng() {
		return lng;
	}
	public void setLng(String lng) {
		this.lng = lng;
	}
	public Integer getLevel() {
		return level;
	}
	public void setLevel(Integer level) {
		this.level = level;
	}
	public String getErrorName() {
		return errorName;
	}
	public void setErrorName(String errorName) {
		this.errorName = errorName;
	}
	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	
	

}
