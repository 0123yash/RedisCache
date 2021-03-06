package features;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;
import com.google.gson.JsonArray;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.lambdaworks.redis.codec.RedisCodec;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import resources.PropertyFile;

public class LettuceClusterServiceImpl {
	private String redisURLsStr;
	private String keySuffix;
	private boolean isAsync;
	private boolean isString;
	
	private RedisClusterClient clusterClient;
	
	private StatefulRedisClusterConnection<String, String> connection;
	private RedisAdvancedClusterCommands<String, String> sync;
	private RedisAdvancedClusterAsyncCommands<String, String> async;
	
	private StatefulRedisClusterConnection<String, byte[]> connectionByte;
	private RedisAdvancedClusterCommands<String, byte[]> syncByte;
	private RedisAdvancedClusterAsyncCommands<String, byte[]> asyncByte;
	
	private boolean isConnectionActive=false;	//as a flag 
	
	public LettuceClusterServiceImpl(){
		redisURLsStr = PropertyFile.redisURLsStr;
		keySuffix = PropertyFile.keySuffix;
		isAsync = PropertyFile.isAsync;
		isString = PropertyFile.isString;
	}
	
	public LettuceClusterServiceImpl(boolean isAsyncParam, boolean isStringParam){
		redisURLsStr = PropertyFile.redisURLsStr;
		keySuffix = PropertyFile.keySuffix;
		isAsync = isAsyncParam;
		isString = isStringParam;
	}
	
	public LettuceClusterServiceImpl(String redisURLsStrParam, String keySuffixParam, boolean isAsyncParam, boolean isStringParam){
		redisURLsStr = redisURLsStrParam;
		keySuffix = keySuffixParam;
		isAsync = isAsyncParam;
		isString = isStringParam;
	}
	
//	------------------------------------------------------------------------------------------------------------------------
//	Public (Abstracted) Functions here
//	------------------------------------------------------------------------------------------------------------------------

	public void initConnection(){
		initializeClusterClient();
        if(isString){
        	connection = clusterClient.connect();
			if(isAsync){
	        	async = connection.async();
	        }else{
	        	sync = connection.sync();
	        }
        }else{
        	RedisCodec<String, byte[]> codec = new StringByteArrayCodec();
        	connectionByte = clusterClient.connect(codec);
        	if(isAsync){
	        	asyncByte = connectionByte.async();
	        }else{
	        	syncByte = connectionByte.sync();
	        }
        }
        isConnectionActive = true;
	}
	
	public void killConnection(){
		if(connection!=null){
			connection.close();
		}
		if(connectionByte!=null){
			connectionByte.close();
		}
		if(clusterClient!=null){
			clusterClient.shutdown();
		}
		isConnectionActive = false;
	}
	
	public Object get(LinkedHashMap<String,String> keyMap){
		String key = getKeyFromMap(keyMap);
		return get(key);
	}
	
	public Object get(String key){
		if(!isConnectionActive){
			System.out.println("Connection is not active");
			return null;
		}
		String finalKey = addSuffix(key);
		
		String returnVal = getExact(finalKey);
		//getStr is only null if the key does not exist, it is [] if empty
		if(returnVal!=null){
			Object returnObj = deSerialize(returnVal);
			return returnObj;
    	}
		return null;
	}
	
	
	public void set(LinkedHashMap<String,String> keyMap, Object obj) throws IOException{
		String key = getKeyFromMap(keyMap);
		set(key,obj);
	}
	
	public void set(String key, Object obj) throws IOException{
		if(!isConnectionActive){
			System.out.println("Connection is not active");
			return;
		}
		String finalKey = addSuffix(key);
		
		setExact(finalKey, serialize(obj));
	}

//	//raw - string key, string value
//	public void set(String key, String str) throws IOException{
//		if(!isConnectionActive){
//			System.out.println("Connection is not active");
//			return;
//		}
//		String finalKey = addSuffix(key);
//		setExact(finalKey, str);
//	}
//	
//	public String get(String key){
//		if(!isConnectionActive){
//			System.out.println("Connection is not active");
//			return null;
//		}
//		String finalKey = addSuffix(key);
//		return getExact(finalKey);
//	}
	
//	------------------------------------------------------------------------------------------------------------------------
//	Private Functions here
//	------------------------------------------------------------------------------------------------------------------------
	 	
	//converts any object to json string using the external library json-io
	private String serialize(Object x){
		return JsonWriter.objectToJson(x);
	}
	
	//converts back any json string converted using serialize function into object type - can be cast into whatever was the original data type
	private Object deSerialize(String json){
		return JsonReader.jsonToJava(json);
	}
	
	//sets key and listofDbObj as key , value  
	public void setExact(String key, String value) throws IOException{
		byte[] valueByte = value.getBytes("UTF-8");
		if(isString){
			if(isAsync){
				async.setex(key, 1800, value);
			}else{
				sync.setex(key, 1800, value);
			}
		}else{
//			System.out.println(value);
			if(isAsync){
				asyncByte.setex(key, 1800, valueByte);
			}else{
				syncByte.setex(key, 1800, valueByte);
			}
		}
	}

	//retrieves value of the key and returns as object
	//returns the value Object if key is available, o/w, returns null
	public String getExact(String key){
		String getStr=null;
		byte[] getStrByte=null; 
		if(isString){
			if(isAsync){
				try {
				    RedisFuture<String> future = async.get(key);
				    getStr = future.get(1, TimeUnit.MINUTES);
				} catch (Exception e) {
				    e.printStackTrace();
				}
			}else{
				getStr = sync.get(key);
			}	
		}else{
			if(isAsync){
				try {
				    RedisFuture<byte[]> futureByte = asyncByte.get(key);
				    getStrByte = futureByte.get(1, TimeUnit.MINUTES);
				} catch (Exception e) {
				    e.printStackTrace();
				}
			}else{
				getStrByte= syncByte.get(key);
			}	
			if(getStrByte!=null){
				try {
					getStr = new String(getStrByte, "UTF-8");
				} catch (Exception e) {
				    e.printStackTrace();
				}
			}
//			System.out.println(getStr);
		}
		return getStr;	
	}

	//initialize the private global variable clusterClient
	private void initializeClusterClient(){
		List<RedisURI> redisNodes = new ArrayList<RedisURI>();
        
        for(String node : redisURLsStr.split(",")){
            String[] keys = node.split(":");
            redisNodes.add(RedisURI.Builder.redis(keys[0], Integer.valueOf(keys[1])).build());
        }
        clusterClient = RedisClusterClient.create(redisNodes);
	}
	
	//converts LinkedHashMap query params of key to String key
	private String getKeyFromMap(LinkedHashMap<String,String> map){
		if(map==null){
			return null;
		}
		Set<String> keys = map.keySet();
		String returnStr;
		StringBuilder a = new StringBuilder();
		for(String k:keys){
			a.append(k+"="+map.get(k));
			a.append("&");
		}
		returnStr = a.toString();
		returnStr = returnStr.substring(0,returnStr.length()-1);
		return returnStr;
	}
	
	//adds suffix to the key 
	private String addSuffix (String key){
		return (key + "_" + keySuffix);
	}
}
