/**  
* @Title: Split.java
* @Package com.wlwl.cube.ananlyse.functions
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 下午1:01:54
* @version V1.0.0  
*/
package com.wlwl.cube.ananlyse.functions;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;

import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.ananlyse.state.StateUntils;

/**
 * @ClassName: Split
 * @Description: TODO从kafka中获取字符串，并反序列化为对象，传输出去
 * @author fenghai
 * @date 2016年9月24日 下午1:01:54
 *
 */
public class CreateVehicleModelFunction extends BaseFunction {
	/**
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	 */
	private static final long serialVersionUID = -888818998763391563L;
	private ObjectModelOfKafka vehicle = null;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String sentence = tuple.getString(0);

			vehicle = JsonUtils.deserialize(
					sentence.replace("TIMESTAMP", "timestamp").replaceAll("DATIME_RX", "datime_RX"),
					ObjectModelOfKafka.class);

			if (vehicle != null) {

				collector.emit(new Values(vehicle));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}