package features;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import resources.PropertyFile;

public class LettuceClusterServiceImpl {
	private String[] redisURLs;
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
		redisURLs = PropertyFile.redisURLs;
		keySuffix = PropertyFile.keySuffix;
		isAsync = PropertyFile.isAsync;
		isString = PropertyFile.isString;
	}
	
	public LettuceClusterServiceImpl(String[] redisURLsParam, String keySuffixParam, boolean isAsyncParam, boolean isStringParam){
		redisURLs = redisURLsParam;
		keySuffix = keySuffixParam;
		isAsync = isAsyncParam;
		isString = isStringParam;
	}
	
//	------------------------------------------------------------------------------------------------------------------------
//	Public (Abstracted) Functions here
//	------------------------------------------------------------------------------------------------------------------------

	public void initConnection(){
		initializeClusterClient();
		connection = clusterClient.connect();
        if(isString){
			if(isAsync){
	        	async = connection.async();
	        }else{
	        	sync = connection.sync();
	        }
        }else{
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
	
	public List<DBObject> get(LinkedHashMap<String,String> keyMap){
		String key = getKeyFromMap(keyMap);
		return get(key);
	}
	
	public List<DBObject> get(String key){
		if(!isConnectionActive){
			System.out.println("Connection is not active");
		}
		String finalKey = addSuffix(key);
		return getExact(finalKey);
	}
	
	
	public void set(LinkedHashMap<String,String> keyMap, List<DBObject> listDBObj) throws IOException{
		String key = getKeyFromMap(keyMap);
		set(key,listDBObj);
	}
	
	public void set(String key, List<DBObject> listDBObj) throws IOException{
		if(!isConnectionActive){
			System.out.println("Connection is not active");
		}
		String finalKey = addSuffix(key);
		setExact(finalKey, listDBObj);
	}
	
	public List<DBObject> returnData (String msid) throws IOException{
		String URL = "http://192.168.36.102/mytimes/getFeed/Activity?curpg=1&appkey=TOI&sortcriteria=CreationDate&order=asc&size=30&pagenum=1&withReward=false&alternate=true&msid=";
		String url1 = (URL+msid);
		JsonArray jsonArray = ServiceLayer.readJsonFromUrl(url1);
		System.out.println(jsonArray);
		return toListDBObj(jsonArray.toString());
	}
	
//	------------------------------------------------------------------------------------------------------------------------
//	Private Functions here
//	------------------------------------------------------------------------------------------------------------------------
	 
//	private void setExact(String key, List<DBObject> listDBObj) throws IOException{
//		String BaseURL = "myt.indiatimes.com/mytimes/getFeed/Activity?";
//		String URL = BaseURL + key;
//		JsonArray jsonArray = ServiceLayer.readJsonFromUrl(URL);
//		sync.setex(key, 1800, jsonArray.toString());
//	}
	
	//sets key and listofDbObj as key , value  
	private void setExact(String key, List<DBObject> listDBObj) throws IOException{
		JSONArray jsonArray = toJsonArray(listDBObj);
		String value = jsonArray.toString();
		byte[] valueByte = value.getBytes();
		if(isString){
			if(isAsync){
				async.setex(key, 1800, value);
			}else{
				sync.setex(key, 1800, value);
			}
		}else{
			if(isAsync){
				asyncByte.setex(key, 1800, valueByte);
			}else{
				syncByte.setex(key, 1800, valueByte);
			}
		}
	}

	//retrieves value of the key and converts it to list of DbObj
	//returns the listOfDbObj if key is available, o/w, returns null
	private List<DBObject> getExact(String key){
		String getStr;
		if(isString){
			if(isAsync){
				getStr = async.get(key).toString();
			}else{
				getStr = sync.get(key);
			}	
		}else{
			if(isAsync){
				getStr = asyncByte.get(key).toString();
			}else{
				getStr = syncByte.get(key).toString();
			}	
		}
		List<DBObject> listDbObj;
		
		//getStr is only null if the key does not exist, it is [] if empty
		if(getStr!=null){
    		listDbObj = toListDBObj(getStr);
    		return listDbObj;
    	}
		return null;
	}
	
	//initialize the private global variable clusterClient
	private void initializeClusterClient(){
		List<RedisURI> redisNodes = new ArrayList<RedisURI>();
        
        for(String node : redisURLs){
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
	
	//converts list of DBObjects to org.json.JSONArray
	private JSONArray toJsonArray(List<DBObject> listDBObj) {
		JSONArray jsonArr = new JSONArray();
		JSONObject jsonObj;
		for (int i=0;i<listDBObj.size();i++){
			jsonObj = (JSONObject) listDBObj.get(i);
			jsonArr.put(jsonObj);
		}
		return jsonArr;
	}
		
	//converts from org.json.JSONArray.toString to List of DBObject
	private List<DBObject> toListDBObj(String str){
		 JSONArray jsonArr = new JSONArray(str);
		 JSONObject jsonObj;
		 List<DBObject> listDbObj=new ArrayList<DBObject>();
		 DBObject dbObj;
		 
		 for (int index=0;index<jsonArr.length();index++){
			 jsonObj = jsonArr.getJSONObject(index);
			 dbObj = (DBObject)JSON.parse(jsonObj.toString());
			 listDbObj.add(dbObj);
		 }
		 return listDbObj; 
	}
}
