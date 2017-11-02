/**  
* @Title: vehicleStatusBean.java
* @Package com.wlwl.cube.analyse.bean
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月13日 上午11:14:43
* @version V1.0.0  
*/ 
package com.wlwl.cube.analyse.bean;

import java.io.Serializable;


/**
* @ClassName: vehicleStatusBean
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年10月13日 上午11:14:43
*
*/

public class VehicleStatusBean implements Serializable {
	
	public VehicleStatusBean() {  
        // TODO Auto-generated constructor stub  
    } 
	/**
	* @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	*/ 
	private static final long serialVersionUID = 944184066314613673L;
	private String code="";
	private String option="=";
	private Integer value=0;
	private Integer value_last=0;
	private Integer status=0;
	private String fiber_unid="";
	private String remarks="";
	private Integer alarm_level=0;
	private String alarm_name="";
	public Integer getALARM_LEVEL() {
		return alarm_level;
	}

	public void setALARM_LEVEL(Integer aLARM_LEVEL) {
		alarm_level = aLARM_LEVEL;
	}

	public String getALARM_NAME() {
		return alarm_name;
	}

	public void setALARM_NAME(String aLARM_NAME) {
		alarm_name = aLARM_NAME;
	}

	

	
	
	public String getREMARKS() {
		return remarks;
	}

	public void setREMARKS(String rEMARKS) {
		remarks = rEMARKS;
	}

	public String getFIBER_UNID() {
		return fiber_unid;
	}

	public void setFIBER_UNID(String fIBER_UNID) {
		fiber_unid = fIBER_UNID;
	}

	/**
	* @return cODE
	*/
	public String getCODE() {
		return code;
	}

	/**
	* @param cODE 要设置的 cODE
	*/
	public void setCODE(String cODE) {
		code = cODE;
	}



	/**
	* @return oPTION
	*/
	public String getOPTION() {
		return option;
	}



	/**
	* @param oPTION 要设置的 oPTION
	*/
	public void setOPTION(String oPTION) {
		option = oPTION;
	}



	/**
	* @return vALUE
	*/
	public Integer getVALUE() {
		return value;
	}



	/**
	* @param vALUE 要设置的 vALUE
	*/
	public void setVALUE(Integer vALUE) {
		value = vALUE;
	}


	/**
	* @return status
	*/
	public Integer getStatus() {
		return status;
	}


	/**
	* @param status 要设置的 status
	*/
	public void setStatus(Integer status) {
		this.status = status;
	}

	/**
	* @return vALUE_LAST
	*/
	public Integer getVALUE_LAST() {
		return value_last;
	}


	/**
	* @param vALUE_LAST 要设置的 vALUE_LAST
	*/
	public void setVALUE_LAST(Integer vALUE_LAST) {
		value_last = vALUE_LAST;
	}

	
	
	
	public Boolean checkStatus(String value)
	{
		switch(option)
		{
		case "=":
			if(Double.parseDouble(value)==this.value)
			{
				return true;
			}
			break;
		case ">":
			if(Double.parseDouble(value)>this.value)
			{
				return true;
			}
			break;
		case ">=":
			if(Double.parseDouble(value)>=this.value)
			{
				return true;
			}
			break;
		case "<":
			if(Double.parseDouble(value)<this.value)
			{
				return true;
			}
			break;
		case "<=":
			if(Double.parseDouble(value)<=this.value)
			{
				return true;
			}
			break;
		case "[between]":
			if(Double.parseDouble(value)>=this.value&&Double.parseDouble(value)<=this.value_last)
			{
				return true;
			}
			break;
		case "(between)":
			if(this.value<Double.parseDouble(value)&&this.value_last>Double.parseDouble(value))
			{
				return true;
			}
			break;
		case "(between]":
			
			if(this.value<Double.parseDouble(value)&&this.value_last>=Double.parseDouble(value))
			{
				return true;
			}
			break;
		case "[between)":
			if(this.value<=Integer.parseInt(value)&&this.value_last>Integer.parseInt(value))
			{
				return true;
			}
			break;
			
			default:return false;
		
		}
	  return false;
	}

	

}
