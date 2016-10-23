/**  
* @Title: Pair.java
* @Package com.wlwl.cube.analyse.bean
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 下午4:18:53
* @version V1.0.0  
*/
package com.wlwl.cube.analyse.bean;

import java.io.Serializable;

import com.wlwl.cube.ananlyse.state.JsonUtils;

/**
 * @ClassName: Pair
 * @Description: TODO(这里用一句话描述这个类的作用) "code": "guqi2ed", "alias": "", "title":
 *               "包头", "value": "true"
 * @author fenghai
 * @date 2016年9月24日 下午4:18:53
 *
 */
public class Pair implements Serializable {

	private static final long serialVersionUID = 8418206139459599885L;
	private String code="";
	private String alias="";
	private String title="";
	private String value="0.0";

	/**
	 * @return code
	 */
	public String getCode() {
		return code;
	}

	/**
	 * @param code
	 *            要设置的 code
	 */
	public void setCode(String code) {
		this.code = code;
	}

	/**
	 * @return alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param alias
	 *            要设置的 alias
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            要设置的 title
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value
	 *            要设置的 value
	 */
	public void setValue(String value) {
		this.value = value;
	}

    @Override
	public String toString()
    {
    	
    	return JsonUtils.serialize(this);
    }

}
