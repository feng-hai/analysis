
package com.wlwl.cube.ananlyse.state;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
* @ClassName: StateUntils
* @Description: TODO通用的工具类
* @author fenghai
* @date 2016年9月18日 下午9:41:27
*
*/ 

public class StateUntils {
	

	/**
	* @Title: formateHour
	* @Description: TODO对应日期生产固定的字符串格式
	* @param @param date
	* @param @return    返回yyyyMMdd格式
	* @return String    返回类型为字符串
	* @throws
	*/ 
	public static String formateDay(Date date)
	{
		 return new SimpleDateFormat("yyyyMMdd").format(date);
	}
	
	public  static String formate(Date date)
	{
		
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
	}
	
	/**
	* @Title: strToDate
	* @Description: TODO 根据日期字符串返回日期
	* @param @param dateStr  格式为"yyyy-MM-dd HH:mm:ss"
	* @param @return    设定文件
	* @return Date    返回类型
	* @throws
	*/ 
	public static Date strToDate(String dateStr)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");                

		 try {
			Date date = sdf.parse(dateStr);
			return date;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null;
	}
	

}
