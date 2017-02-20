package resources;

import java.util.HashMap;
import java.util.Map;

public class PropertyFile {
	public static String[] redisURLs = {"192.168.27.158:7000","192.168.27.158:7001", "192.168.27.158:7002", "192.168.27.158:7003", "192.168.27.158:7004", "192.168.27.158:7005", "192.168.27.158:7006"};
	//byte[] or string 
	public static String keySuffix = "MYT";
	public static boolean isAsync = false;
	public static boolean isString = true;
	
//	public static Map<String,String> suffixes;
//	static
//	    {
//	        suffixes = new HashMap<String, String>();
//	        suffixes.put("TOI", "TIMESOFINDIA");
//	        suffixes.put("ET", "EconomicTimes");
//	        suffixes.put("MYT", "MyTimes");
//	    }
}
