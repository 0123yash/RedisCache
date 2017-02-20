package main;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.mongodb.DBObject;

import features.LettuceCluster;
import features.LettuceClusterServiceImpl;
import features.RedisFactory;
import features.ServiceLayer;

public class Main {
	public static void main(String args[]) throws IOException{
//		LettuceCluster.cluster();
//		LettuceCluster.byteCluster();
		
//		RedisFactory redisFactory = new RedisFactory();
//		RedisAdvancedClusterCommands<String, String> conn = redisFactory.getConnection();
//		conn.set("515962332232", "abc");
//		System.out.println(conn.get("515962332232"));
//		System.out.println(conn.get("23"));
//		JsonArray jsonArray = ServiceLayer.readJsonFromUrl("http://myt.indiatimes.com/mytimes/getFeed/Activity?msid=515962332232&curpg=1&appkey=TOI&sortcriteria=CreationDate&order=asc&size=100&pagenum=1&withReward=true");
//		conn.setex("515962332232", 600, jsonArray.toString());
		
		LettuceClusterServiceImpl a = new LettuceClusterServiceImpl();
		a.initConnection();
		String msid = "51596291";
		a.set(msid,a.returnData(msid));
		System.out.println(a.get(msid));
		a.killConnection();
	}
}
