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

public class Tester {
	
	public static List<String> listOfMsid = null;
	public static String FOLDER_LOCATION = "/Users/yash.dalmia/TIL/Webro/MyTimes/MSID100list>20";
//	public static String FOLDER_LOCATION = "/Users/yash.dalmia/TIL/Webro/MyTimes/All100MsidList";
	public static String URL = "http://192.168.36.102/mytimes/getFeed/Activity?curpg=1&appkey=TOI&sortcriteria=CreationDate&order=asc&size=30&pagenum=1&withReward=false&alternate=true&msid=";
	static{
		 listOfMsid = ServiceLayer.readFromTextFile(FOLDER_LOCATION);
	}
	public static List<String> timeList = new ArrayList<String>();;
	
	public static void testNow() throws IOException{
		LettuceClusterServiceImpl fact = new LettuceClusterServiceImpl();
		fact.initConnection();
		System.out.println("async and string");
		setList(fact);
//		String key = "mapObject";
//		fact.setExact(key,ad());
		getList(fact);
		
		fact = new LettuceClusterServiceImpl(false, true);
		fact.initConnection();
//		setList(fact);
		System.out.println("sync and string");
		getList(fact);
		
		fact = new LettuceClusterServiceImpl(true , false);
		fact.initConnection();
//		setList(fact);
		System.out.println("async and byte");
		getList(fact);
		
		fact = new LettuceClusterServiceImpl(false, false);
		fact.initConnection();
//		setList(fact);
		System.out.println("sync and byte");
		getList(fact);
		
		System.out.println("\n"+timeList);
		
//		Object obj = fact.get(key);
//		Map<Long,DBObject> map1= (Map<Long ,DBObject>)obj;
//		System.out.println("object : "+map1);
//		System.out.println("object : "+map1.get(new Long(11111)));
		fact.killConnection();
	}
	
	
//	------------------------------------------------------------------------------------------------------------------------
//	Private Functions here
//	------------------------------------------------------------------------------------------------------------------------

	private static Map<Long ,DBObject> ad(){
		List<DBObject> listDB = new ArrayList<DBObject>();
		BasicDBObject db1 = new BasicDBObject();
		Map<Long ,DBObject> mapped = new HashMap<Long, DBObject>();
	    db1.append("key", "val");
	    db1.append("key2", "val2");
	    mapped.put(new Long(22222), db1);
	    listDB.add(db1);
	    db1 = new BasicDBObject();
	    db1.append("key", "val");
	    db1.append("key2", "val2");
//	    listDB.add(db1);
	    mapped.put(new Long(11111), db1);
	    return mapped;
	    
	}
	
	private static void setList(LettuceClusterServiceImpl fact) throws IOException{
		long startTime, endTime, timeTaken,avg=0;
		String key;	
		for(String str:listOfMsid){
			key = str;
			JsonArray jsonArray = returnData(str);
			startTime = System.currentTimeMillis();
//			fact.set(key, jsonArray.toString());
			fact.set(key, jsonArray);
			endTime = System.currentTimeMillis();
			timeTaken = endTime-startTime;
//			fact.setExact(key, jsonArray.toString());
			System.out.println("Key "+key+" stored");
			avg+=timeTaken;
		}
		double finalAvg=(avg*1.0/listOfMsid.size());
		System.out.println("Average time : " + finalAvg);
		timeList.add(Double.toString(finalAvg));
	}
	
	private static void getList(LettuceClusterServiceImpl fact){
		long startTime, endTime, timeTaken,avg=0;
//		String getStr;
		Object getObj;
//		List<DBObject> listDbObj=new ArrayList<DBObject>();
		
		for (String str:listOfMsid){
        	startTime = System.currentTimeMillis();
        	getObj = fact.get(str);
//        	getObj = fact.getExact(str);
        	endTime = System.currentTimeMillis();
        	timeTaken = endTime - startTime;
        	if(getObj==null){
        		System.out.println("Key does not exist");
        	}
        	System.out.println("Time Taken for "+str+" : "+ timeTaken);
        	avg +=timeTaken;
        }
		double finalAvg=(avg*1.0/listOfMsid.size());
		System.out.println("Average time : " + finalAvg);
		timeList.add(Double.toString(finalAvg));
	}
	
	private static JsonArray returnData (String msid) throws IOException{
		String url1 = (URL+msid);
		JsonArray jsonArray = ServiceLayer.readJsonFromUrl(url1);
		return jsonArray;
	}
	
}