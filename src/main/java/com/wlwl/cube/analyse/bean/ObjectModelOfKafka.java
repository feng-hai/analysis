/**  
* @Title: ObjectModelOfKafka.java
* @Package com.wlwl.cube.analyse.bean
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 下午4:13:31
* @version V1.0.0  
*/
package com.wlwl.cube.analyse.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.wlwl.cube.ananlyse.state.JsonUtils;

/**
 * @ClassName: ObjectModelOfKafka
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年9月24日 下午4:13:31
 *
 */

public class ObjectModelOfKafka implements Serializable {
	private static final long serialVersionUID = 477223109817573790L;
	private Date TIMESTAMP = new Date();
	private List<Pair> pairs = new ArrayList<Pair>();
	private String DATIME_RX = "";

	/**
	 * @return tIMESTAMP
	 */
	public Date getTIMESTAMP() {
		return TIMESTAMP;
	}

	/**
	 * @param tIMESTAMP
	 *            要设置的 tIMESTAMP
	 */
	public void setTIMESTAMP(Date tIMESTAMP) {
		TIMESTAMP = tIMESTAMP;
	}

	/**
	 * @return pairs
	 */
	public List<Pair> getPairs() {
		return pairs;
	}

	/**
	 * @param pairs
	 *            要设置的 pairs
	 */
	public void setPairs(List<Pair> pairs) {
		this.pairs = pairs;
	}

	/**
	 * @return dATIME_RX
	 */
	public String getDATIME_RX() {
		return DATIME_RX;
	}

	/**
	 * @param dATIME_RX
	 *            要设置的 dATIME_RX
	 */
	public void setDATIME_RX(String dATIME_RX) {
		DATIME_RX = dATIME_RX;
	}

	@Override
	public String toString() {
		return JsonUtils.serialize(this);
	}

	/**
	 * @Title: getDevice @Description: TODO获取当前信息的车辆id @param @return
	 *         设定文件 @return Pair 返回类型 @throws
	 */
	public Pair getVehicle_UNID() {
		Pair pair = null;
		for (Pair p : pairs) {
			if (p.getCode().equals("ENTITY_UNID")) {
				pair = p;
				break;
			}
		}
		return pair;
	}
	
	/**
	* @Title: getDeviceId
	* @Description: 获取终端id
	* @param @return    设定文件
	* @return Pair    返回类型
	* @throws
	*/ 
	public Pair getDeviceId()
	{
		Pair pair = null;
		for (Pair p : pairs) {
			if (p.getAlias().equals("DEVICE_ID")) {
				pair = p;
				break;
			}
		}
		return pair;
	}

	/**
	 * @Title: getSpeed @Description: TODO速度 @param @return 设定文件 @return Pair
	 * 返回类型 @throws
	 */
	public Pair getSpeed() {
		Pair pair = null;
		for (Pair p : pairs) {
			if (p.getAlias().equals("SPEED_GPS")) {
				pair = p;
				break;
			}
		}
		return pair;

	}

	/**
	 * @Title: getTotalFuel @Description: TODO车辆总油耗 @param @return 设定文件 @return
	 * Pair 返回类型 @throws
	 */
	public Pair getTotalFuel() {
		Pair pair = null;
		for (Pair p : pairs) {
			if (p.getAlias().equals("FUEL_CONSUM_TOTAL")) {
				pair = p;
				break;
			}
		}
		return pair;

	}

	/**
	 * @Title: getAllMile @Description: TODO总里程 @param @return 设定文件 @return Pair
	 *         返回类型 @throws
	 */
	public Pair getAllMile() {
		for (Pair p : pairs) {
			if (p.getAlias().equals("MILEAGE")) {
				return p;
			}
		}
		return new Pair();

	}

	/**
	 * @Title: getAllEnergy
	 * @Description: TODO车辆总电耗
	 * @param
	 * @return 设定文件 @return Pair 返回类型 @throws
	 */
	public Pair getAllEnergy() {
		for (Pair p : pairs) {
			if (p.getAlias() .equals("POWER_CONSUM_TOTAL")) {
				return p;
			}
		}
		return new Pair();
	}
	
	
	/**
	* @Title: getInCharge
	* @Description: 总充电量
	* @param @return    设定文件
	* @return Pair    返回类型
	* @throws
	*/ 
	public Pair getInCharge()
	{
		for (Pair p : pairs) {
			if (p.getAlias() .equals("POWER_CHARING_TOTAL")) {
				return p;
			}
		}
		return new Pair();	
    }
	
	/**
	* @Title: getSOC
	* @Description: 车辆SOC
	* @param @return    设定文件
	* @return Pair    返回类型
	* @throws
	*/ 
	public Pair getSOC()
	{
		for (Pair p : pairs) {
			if (p.getAlias() .equals("SOC")) {
				return p;
			}
		}
		return new Pair();	
		
	}

	/**
	 * @Title: getChargeStatus @Description: TODO充电状态 @param @return
	 * 设定文件 @return Pair 返回类型 @throws
	 */
	public Pair getChargeStatus() {
		for (Pair p : pairs) {
			if (p.getAlias().equals ("ChargeStatus")) {
				// 判断状态， 默认为false，为true时正在充电
				p.setValue("false");
				return p;
			}
		}
		return new Pair();
	}

	/**
	 * @Title: getSOC @Description: TODO总电量 @param @return 设定文件 @return Pair
	 * 返回类型 @throws
	 */
	public Pair getChargeAll() {

		for (Pair p : pairs) {
			if (p.getAlias() .equals("ChargeAll")) {
				return p;
			}
		}
		return new Pair();
	}

	/**
	* @Title: getPairByCode
	* @Description: TODO 获取对应code的数据
	* @param @param code
	* @param @return    设定文件
	* @return Pair    返回类型
	* @throws
	*/ 
	public Pair getPairByCode(String code) {
		for (Pair p : pairs) {
			String pAlias = p.getAlias();
			if (pAlias.equals(code)) {
				return p;
			}
			String pCode = p.getCode();
			if (pCode.equals(code)) {
				return p;
			}
		}
		return null;
	}

}
