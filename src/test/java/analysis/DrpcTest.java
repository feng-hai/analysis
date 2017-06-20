/**  
* @Title: DrpcTest.java
* @Package analysis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月25日 下午2:22:50
* @version V1.0.0  
*/
package analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

import com.wlwl.cube.analyse.bean.ObjectModelOfKafka;
import com.wlwl.cube.ananlyse.functions.VehicleAlarmFetchFunction;
import com.wlwl.cube.ananlyse.state.JsonUtils;
import com.wlwl.cube.redis.RedisUtils;
import com.wlwl.cuble.analyse.storager.StoragerSingleton;

/**
 * @ClassName: DrpcTest
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月25日 下午2:22:50
 *
 */
public class DrpcTest {

	public static void main(String[] args) throws Exception {
		
	
		//		RedisUtils redis=new RedisUtils();
//	
//		List<String> keys=new ArrayList<String>();
//		keys.add("BIG_VEHICLE:00331C16D968481DB24C879B6381358E");
//		keys.add("BIG_VEHICLE:0034A66059DC413398BE43E3FE32C0A1");
//	  Map<String,Map<String,String>> result=	redis.getAllKeys(keys);
//		
//		
//		redis.del("test:test");
//		Config conf = new Config();
//		DRPCClient drpc = new DRPCClient(conf, "dn00-dev.wlwl.com", 3772,6000);
//		System.out.println("REACH: " + drpc.execute("reach", "aaa"));
//		System.out.println("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
//		System.out.println("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));
		//VehicleAlarmFetchFunction test=new  		VehicleAlarmFetchFunction();
		//test.setRedis("00FDD418C09646549BA049E51614BAC4");
	}

}
