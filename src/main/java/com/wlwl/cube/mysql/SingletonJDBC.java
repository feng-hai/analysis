/**  
* @Title: SingletonJDBC.java
* @Package com.wlwl.cube.mysql
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月20日 下午8:59:50
* @version V1.0.0  
*/
package com.wlwl.cube.mysql;

import java.sql.Connection;

/**
 * @ClassName: SingletonJDBC
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月20日 下午8:59:50
 *
 */

public class SingletonJDBC {
	private SingletonJDBC() {
	}

	private static JdbcUtils jdbcUtils = null;
	private static Connection connection = null;

	public static JdbcUtils getJDBC() {

		if (connection == null) {
			try {
				jdbcUtils = new JdbcUtils();
				connection = jdbcUtils.getConnection();
			} catch (Exception e) {
				e.printStackTrace();
				connection=null;
				jdbcUtils=null;
			}
		}
		return jdbcUtils;
	}

	public static void clean() {
		jdbcUtils.releaseConn();
		jdbcUtils = null;

	}

}
