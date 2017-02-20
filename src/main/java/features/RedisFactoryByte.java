package features;
import java.util.ArrayList;
import java.util.List;

import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.lambdaworks.redis.codec.RedisCodec;

import resources.PropertyFile;

public class RedisFactoryByte {
	
	private String[] redisURLs;
	private RedisClusterClient clusterClient;
	private StatefulRedisClusterConnection<String, byte[]> connection;
	
	public RedisFactoryByte(){
		redisURLs = PropertyFile.redisURLs;
		initializeClusterClient();
	}
	
	public void setRedisURLs(String[] arr){
		redisURLs = arr;
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
	
	public RedisAdvancedClusterCommands<String, byte[]> getConnection(){
        RedisCodec<String, byte[]> codec = new StringByteArrayCodec();
		connection = clusterClient.connect(codec);
        RedisAdvancedClusterCommands<String, byte[]> sync = connection.sync();
        return sync;
	}
	
	public void killConnection(){
		connection.close();
        clusterClient.shutdown();
	}
}
