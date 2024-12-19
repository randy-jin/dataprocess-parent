package com.tl.handes.redistodatahup;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;

//Redis Client
public class intoredis {
	/* String host = "127.0.0.1";
	 int port = 6379;
	 Jedis client = new Jedis(host,port);
	 String result = client.set("aa", "hello");
	放弃使用单个redis*/
	    private static Jedis jedis;//非切片额客户端连接
	    private static JedisPool jedisPool;//非切片连接池
	    private static ShardedJedis shardedJedis;//切片额客户端连接
	    private static ShardedJedisPool shardedJedisPool;//切片连接池
	
	    public void RedisClient()
	    { 
	        initialPool(); 
	        initialShardedPool(); 
	        shardedJedis = shardedJedisPool.getResource(); 
	        jedis = jedisPool.getResource(); 
	        
	        
	    }
	    
	    /**
	     * 初始化非切片池
	     */
	    private void initialPool()
	    { 
	        // 池基本配置 
	        JedisPoolConfig config = new JedisPoolConfig();
	        config.setMaxTotal(20); //连接总数
	        config.setMaxIdle(5); 
	        config.setMaxWaitMillis(1000l); //最大等待时间
	        config.setTestOnBorrow(false); 
	        //密码为null
//	        jedisPool = new JedisPool(config,"127.0.0.1",6379,null);
	    }
	    
	    /** 
	     * 初始化切片池 
	     */ 
	    private void initialShardedPool() 
	    { 
	        // 池基本配置 
	        JedisPoolConfig config = new JedisPoolConfig();
	        config.setMaxTotal(20); 
	        config.setMaxIdle(5); 
	        config.setMaxWaitMillis(2000l); 
	        config.setTestOnBorrow(false); 
	        // slave链接  // 生成多机连接信息列表
	        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	        shards.add(new JedisShardInfo("127.0.0.1", 6379));
	        //shards.add(new JedisShardInfo("127.0.0.1", 6379)); 
	        // 生成连接池
	        shardedJedisPool = new ShardedJedisPool(config, shards);
	    } 
	    
	    //入redis队列
	    public static void toredis(){
	    	//System.out.println(jedis.flushDB());
	    	System.out.println(jedis.lpush("UFO", "wo"));
	    	System.out.println(jedis.lpush("UFO","shi"));
	    	System.out.println(jedis.lpush("UFO","wai"));
	    	System.out.println(jedis.lpush("UFO", "xing"));
	    	System.out.println(jedis.lpush("UFO", "ren"));
	    	System.out.println(jedis.lpush("UFF", "3"));
	    	System.out.println(jedis.lpush("UFF","33"));
	    	System.out.println(jedis.lpush("UFF","333"));
	    	
	    	System.out.println("UFO的所有元素：\n"+jedis.lrange("UFO", 0, -1));
	    	System.out.println("UFF的所有元素：\n"+jedis.lrange("UFF", 0, -1));
	    	
	    /*	String bytes = null;
	    	bytes = jedis.rpop("UFF");*/
	    	System.out.println("UFF出栈:"+jedis.lpop("UFF")); 
	    }
	    
	    public static void close(Jedis jedis){
	    	try{
//	    		jedisPool.returnResource(jedis);
				jedis.close();
	    	}catch(Exception e){
	    		if(jedis.isConnected()){
	    			jedis.quit();
	    			jedis.disconnect();
	    		}
	    	}
	    }
	    public static void main(String[] args){
	   
	    		intoredis obj = new intoredis();
	    	
	    		obj.toredis();
	    	//jedis = new Jedis("127.0.0.1",6379);
	    
	    	//jedis.set("name", "jj");
	    		
	    
	    	
	    }


	 
}
