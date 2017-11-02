/**  
* @Title: ConnectionDB.java
* @Package com.wlwl.cube.mysql
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月9日 下午4:12:14
* @version V1.0.0  
*/
package com.wlwl.cube.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.wlwl.cube.analyse.bean.VehicleStatusBean;
import com.wlwl.cube.analyse.common.Conf;
import com.wlwl.cube.redis.RedisUtils;



import java.lang.reflect.Field;

/**
 * @ClassName: JdbcUtils
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月9日 下午4:23:47
 *
 */
public class JdbcUtils {
	// 数据库用户名
	private static final String USERNAME = "cube";
	// 数据库密码
	private static final String PASSWORD = Conf.MYSQLPASSWORD;
	// 驱动信息
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	// 数据库地址
	private static final String URL = "jdbc:mysql://" + Conf.MYSQLURL + "/cube";
	private Connection connection;
	//private PreparedStatement pstmt;
	/**
	 * @Fields resultSet : TODO(用一句话描述这个变量表示什么)
	 */
	//private ResultSet resultSet;

	/**
	 * <p>
	 * Title:
	 * </p>
	 * <p>
	 * Description:
	 * </p>
	 */
	public JdbcUtils() {
		// TODO Auto-generated constructor stub
		try {
			Class.forName(DRIVER);
			System.out.println("数据库连接成功！");
			getConnection();

		} catch (Exception e) {

		}
	}

	/**
	 * 获得数据库的连接
	 * 
	 * @return
	 */
	public Connection getConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				return connection;
			}
			connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}

	/**
	 * 增加、删除、改
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public boolean updateByPreparedStatement(String sql, List<Object> params) throws SQLException {
		boolean flag = false;
		int result = -1;
		// if(connection.isClosed())
		// {
		getConnection();
		// }
		PreparedStatement	pstmt = connection.prepareStatement(sql);
		int index = 1;
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		result = pstmt.executeUpdate();
		flag = result > 0 ? true : false;
		if(pstmt!=null)
		{
			try {
				pstmt.close();
				pstmt=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 查询单条记录
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> findSimpleResult(String sql, List<Object> params) throws SQLException {
		Map<String, Object> map = new HashMap<String, Object>();
		int index = 1;
		// if (connection.isClosed()) {
		getConnection();
		// }
		PreparedStatement pstmt = connection.prepareStatement(sql);
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		ResultSet resultSet = pstmt.executeQuery();// 返回查询结果
		ResultSetMetaData metaData = resultSet.getMetaData();
		int col_len = metaData.getColumnCount();
		while (resultSet.next()) {
			for (int i = 0; i < col_len; i++) {
				String cols_name = metaData.getColumnName(i + 1);
				Object cols_value = resultSet.getObject(cols_name);
				if (cols_value == null) {
					cols_value = "";
				}
				map.put(cols_name, cols_value);
			}
		}
		if (resultSet != null) {
			try {
				resultSet.close();
				resultSet=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(pstmt!=null)
		{
			try {
				pstmt.close();
				pstmt=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return map;
	}

	public int insertByPreparedStatement(String sql, List<Object> params) throws SQLException {
		boolean flag = false;
		int result = -1;
		// if (connection.isClosed()) {
		getConnection();
		// }
		PreparedStatement pstmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		int index = 1;
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		pstmt.execute();
		ResultSet rs = pstmt.getGeneratedKeys();
		if (rs.next()) {
			int aiid = rs.getInt(1);
			pstmt.close();
			return aiid;

		}
		pstmt.close();
		return -1;
	}

	/**
	 * 查询多条记录
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> findModeResult(String sql, List<Object> params) throws SQLException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		int index = 1;
		// if (connection.isClosed()) {
		getConnection();
		// }
		PreparedStatement pstmt = connection.prepareStatement(sql);
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		ResultSet resultSet = pstmt.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		while (resultSet.next()) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (int i = 0; i < cols_len; i++) {
				String cols_name = metaData.getColumnName(i + 1);
				Object cols_value = resultSet.getObject(cols_name);
				if (cols_value == null) {
					cols_value = "";
				}
				map.put(cols_name, cols_value);
			}
			list.add(map);
		}
		if (resultSet != null) {
			try {
				resultSet.close();
				resultSet=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(pstmt!=null)
		{
			try {
				pstmt.close();
				pstmt=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * 通过反射机制查询单条记录
	 * 
	 * @param sql
	 * @param params
	 * @param cls
	 * @return
	 * @throws Exception
	 */
	public <T> T findSimpleRefResult(String sql, List<Object> params, Class<T> cls) throws Exception {
		T resultObject = null;
		int index = 1;
		// if (connection.isClosed()) {
		getConnection();
		// }
		PreparedStatement pstmt = connection.prepareStatement(sql);
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		ResultSet resultSet = pstmt.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		while (resultSet.next()) {
			// 通过反射机制创建一个实例
			resultObject = cls.newInstance();
			for (int i = 0; i < cols_len; i++) {
				String cols_name = metaData.getColumnName(i + 1);
				Object cols_value = resultSet.getObject(cols_name);
				if (cols_value == null) {
					cols_value = "";
				}
				Field field = cls.getDeclaredField(cols_name);
				field.setAccessible(true); // 打开javabean的访问权限
				field.set(resultObject, cols_value);
			}
		}
		if (resultSet != null) {
			try {
				resultSet.close();
				resultSet=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(pstmt!=null)
		{
			try {
				pstmt.close();
				pstmt=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return resultObject;

	}

	/**
	 * 通过反射机制查询多条记录
	 * 
	 * @param sql
	 * @param params
	 * @param cls
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> findMoreRefResult(String sql, List<Object> params, Class<T> cls) throws Exception {
		List<T> list = new ArrayList<T>();
		int index = 1;
		// if (connection.isClosed()) {
		getConnection();
		// }
		PreparedStatement pstmt = connection.prepareStatement(sql);
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(index++, params.get(i));
			}
		}
		ResultSet resultSet = pstmt.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		while (resultSet.next()) {
			// 通过反射机制创建一个实例
			T resultObject = cls.newInstance();
			for (int i = 0; i < cols_len; i++) {
				String cols_name = metaData.getColumnName(i + 1);
				Object cols_value = resultSet.getObject(cols_name);
				if (cols_value == null) {
					cols_value = "";
				}
				
				Field field = cls.getDeclaredField(cols_name.toLowerCase());
				field.setAccessible(true); // 打开javabean的访问权限
				field.set(resultObject, cols_value);
			}
			list.add(resultObject);
		}
		if (resultSet != null) {
			try {
				resultSet.close();
				resultSet=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(pstmt!=null)
		{
			try {
				pstmt.close();
				pstmt=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return list;
	}

	/**
	 * 释放数据库连接
	 */
	public void releaseConn() {
		
		try {
			if(connection!=null&&!connection.isClosed())
			{
					connection.close();
					connection=null;	
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void checkOnLine() {

		// 获取所以车辆最新时间

		// 更新车辆在线状态

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws SQLException {
		RedisUtils util = new RedisUtils();
		Set<String> set = util.keys(Conf.PERFIX + "*");

		for (String str : set) {
			System.out.println(str);
		}

		// TODO Auto-generated method stub
		JdbcUtils jdbcUtils = new JdbcUtils();
		jdbcUtils.getConnection();

		/******************* 增 *********************/
		/*
		 * String sql =
		 * "insert into userinfo (username, pswd) values (?, ?), (?, ?), (?, ?)"
		 * ; List<Object> params = new ArrayList<Object>(); params.add("小明");
		 * params.add("123xiaoming"); params.add("张三"); params.add("zhangsan");
		 * params.add("李四"); params.add("lisi000"); try { boolean flag =
		 * jdbcUtils.updateByPreparedStatement(sql, params);
		 * System.out.println(flag); } catch (SQLException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		/******************* 删 *********************/
		// 删除名字为张三的记录
		/*
		 * String sql = "delete from userinfo where username = ?"; List<Object>
		 * params = new ArrayList<Object>(); params.add("小明"); boolean flag =
		 * jdbcUtils.updateByPreparedStatement(sql, params);
		 */

		/******************* 改 *********************/
		// 将名字为李四的密码改了
		/*
		 * String sql = "update userinfo set pswd = ? where username = ? ";
		 * List<Object> params = new ArrayList<Object>();
		 * params.add("lisi88888"); params.add("李四"); boolean flag =
		 * jdbcUtils.updateByPreparedStatement(sql, params);
		 * System.out.println(flag);
		 */

		/******************* 查 *********************/

		String sql = "SELECT code,option,value,VALUE_LAST,status  FROM  cube.PDA_CUSTOM_SETUP where fiber_unid='27A67D545CFF4AE3AD4DF45AB94A7C18' and type=1 order by INX desc";
		List<Object> params = new ArrayList<Object>();

		List<VehicleStatusBean> list = null;
		try {
			list = (List<VehicleStatusBean>) jdbcUtils.findMoreRefResult(sql, params, VehicleStatusBean.class);
			System.out.println(list);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 不利用反射查询多个记录
		// String sql2 = "SELECT code,option,value,value_last valueLast,status
		// FROM cube.PDA_VEHICLE_DETAIL ";
		// List<Map<String, Object>> list = jdbcUtils.findModeResult(sql2,
		// null);
		// System.out.println(list);
		//
		// //利用反射查询 单条记录
		// String sql = "select * from userinfo where username = ? ";
		// List<Object> params = new ArrayList<Object>();
		// params.add("李四");
		// UserInfo userInfo;
		// try {
		// userInfo = jdbcUtils.findSimpleRefResult(sql, params,
		// UserInfo.class);
		// System.out.print(userInfo);
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

}