/**  
* @Title: HBaseInstance.java
* @Package com.wlwl.cuble.analyse.storager
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月29日 下午12:57:37
* @version V1.0.0  
*/ 
package com.wlwl.cuble.analyse.storager;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;

/**
* @ClassName: HBaseInstance
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月29日 下午12:57:37
*
*/
public class HBaseInstance implements IStorager<VehicleStatisticBean>{

	/* (non-Javadoc)
	 * @see com.wlwl.cuble.analyse.storager.IStorager#getStorager(java.lang.String)
	 */
	public VehicleStatisticBean getStorager(String key) {
		
		return null;
	}

	/* (non-Javadoc)
	 * @see com.wlwl.cuble.analyse.storager.IStorager#setStorager(java.lang.String, java.lang.Object)
	 */
	public void setStorager(String key, VehicleStatisticBean obj) {
	
		
	}

	/* (non-Javadoc)
	 * @see com.wlwl.cuble.analyse.storager.IStorager#keyExists(java.lang.String)
	 */
	public Boolean keyExists(String key) {
	
		return null;
	}

	/* (non-Javadoc)
	 * @see com.wlwl.cuble.analyse.storager.IStorager#deleteByKey(java.lang.String)
	 */
	@Override
	public void deleteByKey(String key) {
		// TODO Auto-generated method stub
		
	}

}
