package features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.mongodb.DBObject;

import resources.PropertyFile;

public class Wrapper {
	//will use this clusterCommands for commands
	private RedisAdvancedClusterCommands<String, String> sync;
	
	public void connectToCommands(RedisAdvancedClusterCommands<String, String> sync1){
		sync = sync1;
	}
	
	public List<DBObject> get(LinkedHashMap<String,String> keyMap){
		String key = getKeyFromMap(keyMap);
		String finalKey = finalKey(key);
		String getStr = sync.get(finalKey);
		List<DBObject> listDbObj;
		
		//getStr is only null if the key does not exist, it is [] if empty
		if(getStr!=null){
    		listDbObj = toListDBObj(getStr);
    		return listDbObj;
    	}
		return null;
	}
	
	public int set(LinkedHashMap<String,String> keyMap, List<DBObject> listDBObj) throws IOException{
		String key = getKeyFromMap(keyMap);
		String finalKey = finalKey(key);
		String BaseURL = "myt.indiatimes.com/mytimes/getFeed/Activity?";
		String URL = BaseURL + key;
		JsonArray jsonArray = ServiceLayer.readJsonFromUrl(URL);
		try{
			sync.setex(finalKey, 1800, jsonArray.toString());
			return 1;
		}catch(Exception e){
			return 0;
		}
	}
	
	public String getKeyFromMap(LinkedHashMap<String,String> map){
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
	
	public String finalKey (String key){
		return (key + PropertyFile.keySuffix);
	}
	
	private List<DBObject> toListDBObj(String str){
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
}