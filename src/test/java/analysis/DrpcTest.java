/**  
* @Title: DrpcTest.java
* @Package analysis
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年10月25日 下午2:22:50
* @version V1.0.0  
*/
package analysis;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * @ClassName: DrpcTest
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author fenghai
 * @date 2016年10月25日 下午2:22:50
 *
 */
public class DrpcTest {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		DRPCClient drpc = new DRPCClient(conf, "dn00-dev.wlwl.com", 3772,6000);
		System.out.println("REACH: " + drpc.execute("reach", "aaa"));
		System.out.println("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
		System.out.println("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));

	}

}
