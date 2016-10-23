/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wlwl.cube.ananlyse.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.wlwl.cube.analyse.bean.VehicleStatisticBean;
import com.wlwl.cube.ananlyse.state.StateUntils;
import com.wlwl.cuble.analyse.storager.IStorager;
import com.wlwl.cuble.analyse.storager.RedisInstance;

import java.util.Calendar;
import java.util.Date;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4978923603802010379L;
	SpoutOutputCollector _collector;
	Random _rand;
	String[] strs = new String[] { "B20393", "B20394", "B20395" };

	Double mile = 2.0;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(100);
		IStorager<Double> redis = new RedisInstance<Double>(Double.class);
		Date tempDate = new Date();// getNextDay(new Date(),
									// _rand.nextInt(strs.length));
		String device = strs[_rand.nextInt(strs.length)];
		// if (redis.keyExists(device)) {
		// mile = redis.getStorager(device) + 1;
		// } else {
		// mile = mile + 1.0;
		// }
		if (mile < 1000) {
			mile = mile + 1.0;
			device = "B20395";

			
			String sentence = "	 {" + " \"TIMESTAMP\": \"" + tempDate.getTime() + "\"," + "\"pairs\": ["
					+ "{\"code\": \"0x2926\",\"alias\": \"MILEAGE\",\"title\": \"SR_A_BatteryCurrent\",\"value\": \""
					+ mile.toString() + "\" },"
					+ "{\"code\": \"ENTITY_UNID\",\"alias\": \"\",\"title\": \"\",\"value\": \""
					+ "6F0B2FC09EFF43979BD2D5E8B7A5F4B4" + "\" },"
					+ "{\"code\":\"pkyvciu\",\"alias\":\"SPEED_GPS\",\"title\":\"速度\",\"value\":\"1.0\"},"
					+ "{\"code\":\"test\",\"alias\":\"\",\"title\":\"速度\",\"value\":\"1\"},"
					+ "{\"code\": \"dt8mwiq\",\"alias\": \"DEVICE_ID\",\"title\": \"终端ID\",\"value\": \"" + device
					+ "\"}" + "]," + "\"DATIME_RX\": \"" + StateUntils.formate(tempDate) + "\"" + "}";
			
	//		sentence="{\"pairs\":[{\"code\":\"guqi2ed\",\"alias\":\"\",\"title\":\"包头\",\"value\":\"true\"},{\"code\":\"gta82ei\",\"alias\":\"\",\"title\":\"终端消息命令号\",\"value\":\"385\"},{\"code\":\"wxqh8dh\",\"alias\":\"\",\"title\":\"终端消息长度\",\"value\":\"32\"},{\"code\":\"wk41xde\",\"alias\":\"\",\"title\":\"CAN网络消息命令号\",\"value\":\"0\"},{\"code\":\"yb0baj0\",\"alias\":\"\",\"title\":\"CAN网络消息长度\",\"value\":\"0\"},{\"code\":\"pz76x72\",\"alias\":\"\",\"title\":\"消息流水号\",\"value\":\"6\"},{\"code\":\"dt8mwiq\",\"alias\":\"DEVICE_ID\",\"title\":\"终端ID\",\"value\":\"B10792\"},{\"code\":\"qyfwd0y\",\"alias\":\"\",\"title\":\"子设备代码\",\"value\":\"2\"},{\"code\":\"us3b3j3\",\"alias\":\"\",\"title\":\"GPS供应商代码\",\"value\":\"1\"},{\"code\":\"ipbs70h\",\"alias\":\"\",\"title\":\"密钥\",\"value\":\"0\"},{\"code\":\"lqcx4in\",\"alias\":\"\",\"title\":\"上报原因\",\"value\":\"0\"},{\"code\":\"no4tefg\",\"alias\":\"LON\",\"title\":\"经度\",\"value\":\"115.96508299999999\"},{\"code\":\"nhfiasm\",\"alias\":\"LAT\",\"title\":\"纬度\",\"value\":\"38.7803\"},{\"code\":\"pkyvciu\",\"alias\":\"SPEED_GPS\",\"title\":\"速度\",\"value\":\"0.0\"},{\"code\":\"vjbr204\",\"alias\":\"ALT\",\"title\":\"海拔\",\"value\":\"7\"},{\"code\":\"jx7nzuf\",\"alias\":\"BEARING\",\"title\":\"夹角\",\"value\":\"0\"},{\"code\":\"hzwwjbp\",\"alias\":\"MILEAGE_GPS\",\"title\":\"里程\",\"value\":\"1427.094\"},{\"code\":\"phka2ek\",\"alias\":\"VERSION_HW\",\"title\":\"硬件版本号\",\"value\":\"6008\"},{\"code\":\"jqxfx9z\",\"alias\":\"VERSION_FW\",\"title\":\"软件版本号\",\"value\":\"1.1.17.1\"},{\"code\":\"zcyfwgl\",\"alias\":\"DATIME_GPS\",\"title\":\"时间\",\"value\":\"2016-10-10 14:57:55\"},{\"code\":\"xubfzol\",\"alias\":\"\",\"title\":\"校验\",\"value\":\"42\"},{\"code\":\"dzhbzq1\",\"alias\":\"\",\"title\":\"消息尾\",\"value\":\"true\"},{\"code\":\"datime_RX\",\"alias\":\"\",\"title\":\"\",\"value\":\"2016-10-11 16:53:02\"},{\"code\":\"ENTITY_UNID\",\"alias\":\"\",\"title\":\"\",\"value\":\"0034A66059DC413398BE43E3FE32C0A1\"}],\"timestamp\":1476175982595,\"device\":{\"code\":\"dt8mwiq\",\"alias\":\"DEVICE_ID\",\"title\":\"终端ID\",\"value\":\"B10792\"},\"allMile\":{\"code\":\"\",\"alias\":\"\",\"title\":\"\",\"value\":\"\"},\"datime_RX\":\"2016-10-11 16:53:02\",\"allEnergy\":{\"code\":\"\",\"alias\":\"\",\"title\":\"\",\"value\":\"\"}}{\"code\":\"dt8mwiq\",\"alias\":\"DEVICE_ID\",\"title\":\"终端ID\",\"value\":\"B10792\"}";


			_collector.emit(new Values(sentence));
		}
	}

	public static Date getNextDay(Date date, int x) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -1 * x);
		date = calendar.getTime();
		return date;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
