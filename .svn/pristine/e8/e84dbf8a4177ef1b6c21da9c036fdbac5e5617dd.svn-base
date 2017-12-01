package features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.lambdaworks.redis.codec.RedisCodec;
import com.mongodb.*;

public class LettuceCluster {
	
	public static List<String> listOfMsid = null;
	public static String FOLDER_LOCATION = "/Users/yash.dalmia/TIL/Webro/MyTimes/MSID100list>20";
	public static String URL = "http://192.168.36.102/mytimes/getFeed/Activity?curpg=1&appkey=TOI&sortcriteria=CreationDate&order=asc&size=30&pagenum=1&withReward=false&alternate=true&msid=";
	public static String[] redisURLs = {"192.168.27.158:7000","192.168.27.158:7001", "192.168.27.158:7002", "192.168.27.158:7003", "192.168.27.158:7004", "192.168.27.158:7005", "192.168.27.158:7006"};
	static{
		 listOfMsid = ServiceLayer.readFromTextFile(FOLDER_LOCATION);
	}
	
	public static void cluster() throws IOException{
        RedisClusterClient clusterClient = null;
        List<RedisURI> redisNodes = new ArrayList<RedisURI>();
        
        for(String node : redisURLs){
            String[] keys = node.split(":");
            redisNodes.add(RedisURI.Builder.redis(keys[0], Integer.valueOf(keys[1])).build());
        }
        clusterClient = RedisClusterClient.create(redisNodes);
        
        StatefulRedisClusterConnection<String, String> connection2 = clusterClient.connect();
        
        System.out.println("Connected to Redis");
        
//        connection2.setReadFrom(ReadFrom.SLAVE);

//        RedisAdvancedClusterCommands<String, String> sync = connection2.sync();
        RedisAdvancedClusterCommands<String, String> sync = connection2.sync();
        
        //for set commands
//        setList(sync);
        getList(sync);

        connection2.close();
        clusterClient.shutdown();
	}
	
	public static void byteCluster() throws IOException{
        RedisClusterClient clusterClient = null;
//        RedisClient client = null;
//        RedisAdvancedClusterAsyncCommands<String, String> connection = null;
        List<RedisURI> redisNodes = new ArrayList<RedisURI>();
        
        for(String node : redisURLs){
            String[] keys = node.split(":");
            redisNodes.add(RedisURI.Builder.redis(keys[0], Integer.valueOf(keys[1])).build());
        }
        clusterClient = RedisClusterClient.create(redisNodes);
        
        RedisCodec<String, byte[]> codec = new StringByteArrayCodec();
        StatefulRedisClusterConnection<String, byte[]> connection2 = clusterClient.connect(codec);
        
        System.out.println("Connected to Redis");
        
        RedisAdvancedClusterCommands<String, byte[]> sync = connection2.sync();
        
//        byteSetList(sync);
        byteGetList(sync);
        
        connection2.close();
        clusterClient.shutdown();
	}
	
	private static void setList(RedisAdvancedClusterCommands<String, String> sync) throws IOException{
		String url1;
		String key;
		
		for(String str:listOfMsid){
			url1 = (URL+str);
			JsonArray jsonArray = ServiceLayer.readJsonFromUrl(url1);
			key = str;
			sync.setex(key, 1800, jsonArray.toString());
			System.out.println("Key "+key+" stored in <String, String[]>");
		}
	}
	
	public static String returnData (String msid) throws IOException{
		String url1 = (URL+msid);
		JsonArray jsonArray = ServiceLayer.readJsonFromUrl(url1);
		return jsonArray.toString();
	}
	
	private static void getList(RedisAdvancedClusterCommands<String, String> sync){
		long startTime, endTime, timeTaken,avg=0;
		String getStr;
		List<DBObject> listDbObj=new ArrayList<DBObject>();
		
		for (String str:listOfMsid){
        	startTime = System.currentTimeMillis();
        	getStr = sync.get(str);
        	if(getStr!=null){
        		listDbObj = toListDBObj(getStr);
        	}else{
        		System.out.println("Empty string returned");
        		continue;
        	}
        	endTime = System.currentTimeMillis();
        	timeTaken = endTime - startTime;
        	System.out.println("Time Taken for "+str+" : <String, String> stored -> "+ timeTaken);
        	avg +=timeTaken;
        }
		System.out.println("Average time for <String, String> : " + (avg*1.0/listOfMsid.size()));
	}
	
	private static void byteSetList(RedisAdvancedClusterCommands<String, byte[]> sync) throws IOException{
		String url1;
		String key;
		byte[] bytecoded; 
		for(String str:listOfMsid){
			url1 = (URL+str);
			JsonArray jsonArray = ServiceLayer.readJsonFromUrl(url1);
			bytecoded = jsonArray.toString().getBytes();
			key = str+"_byte";
			sync.setex(key, 1800, bytecoded);
			System.out.println("Key "+key+" stored in <String, byte[]>");
		}
	}
	
	private static void byteGetList(RedisAdvancedClusterCommands<String, byte[]> sync){
		long startTime, endTime, timeTaken,avg=0;
		List<DBObject> listDbObj=new ArrayList<DBObject>();
		
		for (String str:listOfMsid){
        	startTime = System.currentTimeMillis();
//        	System.out.println(sync.get(str+"_byte"));
//        	jsonArr = toJSONArray(sync.get(str+"_byte"));
        	listDbObj = toListDBObj(new String(sync.get(str+"_byte")));
        	
        	endTime = System.currentTimeMillis();
        	timeTaken = endTime - startTime;
        	System.out.println("Time Taken for "+ str +" : <String, byte[]> stored -> "+ timeTaken);
        	avg +=timeTaken;
        }
		System.out.println("Average time for <String, byte[]> : " + (avg*1.0/listOfMsid.size()));
	}
	

	private static List<DBObject> toListDBObj(String str){
		 JSONArray jsonArr = new JSONArray(str);
		 JSONObject jsonObj;
		 List<DBObject> listDbObj=new ArrayList<DBObject>();
		 DBObject dbObj;
		 
		 for (int index=0;index<jsonArr.length();index++){
			 jsonObj = jsonArr.getJSONObject(index);
			 dbObj = (DBObject)jsonObj;
			 listDbObj.add(dbObj);
		 }
		 return listDbObj; 
	}
	
	private static JSONArray toJSONArray(byte[] bArr){
		 return new JSONArray(new String(bArr));
	}
}