/**  
* @Title: JsonUtils.java
* @Package com.wlwl.cube.ananlyse.state
* @Description: TODO(用一句话描述该文件做什么)
* @author fenghai  
* @date 2016年9月24日 上午10:18:09
* @version V1.0.0  
*/ 
package com.wlwl.cube.ananlyse.state;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.wlwl.cube.analyse.bean.VehicleStatusBean;

/**
* @ClassName: JsonUtils
* @Description: TODO(这里用一句话描述这个类的作用)
* @author fenghai
* @date 2016年9月24日 上午10:18:09
*
*/
public class JsonUtils {
	 private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);  
	  
	    private static ObjectMapper objectMapper = new ObjectMapper();  
	    /** 
	     * 将对象序列化为JSON字符串 
	     *  
	     * @param object 
	     * @return JSON字符串 
	     */  
	    public static String serialize(Object object) {  
	        Writer write = new StringWriter();  
	        try {  
	            objectMapper.writeValue(write, object);  
	        } catch (JsonGenerationException e) {  
	          logger.error("JsonGenerationException when serialize object to json", e);  
	        } catch (JsonMappingException e) {  
	          logger.error("JsonMappingException when serialize object to json", e);  
	        } catch (IOException e) {  
	            logger.error("IOException when serialize object to json", e);  
	        }  
	        return write.toString();  
	    }  
	  
	    /** 
	     * 将JSON字符串反序列化为对象 
	     *  
	     * @param object 
	     * @return JSON字符串 
	     */  
	    @SuppressWarnings("unchecked")
		public static <T> T deserialize(String json, Class<T> clazz) {  
	        Object object = null;  
	        try {  
	            object = objectMapper.readValue(json, TypeFactory.rawClass(clazz));  
	        } catch (JsonParseException e) {  
	            logger.error("JsonParseException when serialize object to json", e);  
	        } catch (JsonMappingException e) {  
	            logger.error("JsonMappingException when serialize object to json", e);  
	        } catch (IOException e) {  
	            logger.error("IOException when serialize object to json", e);  
	        }  
	        return (T) object;  
	    }  
	  
	    /** 
	     * 将JSON字符串反序列化为对象 
	     *  
	     * @param object 
	     * @return JSON字符串 
	     */  
	    @SuppressWarnings("unchecked")
		public static <T> T deserialize(String json, TypeReference<T> typeRef) {  
	        try {  
	            return (T) objectMapper.readValue(json, typeRef);  
	        } catch (JsonParseException e) {  
	            logger.error("JsonParseException when deserialize json", e);  
	        } catch (JsonMappingException e) {  
	            logger.error("JsonMappingException when deserialize json", e);  
	        } catch (IOException e) {  
	            logger.error("IOException when deserialize json", e);  
	        }  
	        return null;  
	    }

		public static List<VehicleStatusBean> mapperObject(String string,
				org.apache.htrace.fasterxml.jackson.core.type.TypeReference<List<VehicleStatusBean>> typeReference) {
			 ObjectMapper mapper = new ObjectMapper();
		        //mapper.enableDefaultTyping();
		        try {
		            return mapper.readValue(string, new TypeReference<List<VehicleStatusBean>>() {});
		        } catch (IOException e) {
		            e.printStackTrace();
		            return null;
		        }
			
		}

}
