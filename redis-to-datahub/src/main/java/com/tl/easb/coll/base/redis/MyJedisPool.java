package com.tl.easb.coll.base.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyJedisPool {
	private static JedisPool pool;
	// 静态代码初始化池配置
	static {
		int clientTimeout = 1000000;
		String idleTimeout = String.valueOf(clientTimeout);
		// 创建jedis池配置实例
		JedisPoolConfig config = new JedisPoolConfig();
		// 设置池配置项值
//		config.setMaxActive(RedisConfig.getInt("jedis.pool.maxActive"));
		config.setMaxIdle(RedisConfig.getInt("jedis.pool.maxIdle"));
//		config.setMaxWait(RedisConfig.getLong("jedis.pool.maxWait"));
		config.setTestOnBorrow(RedisConfig
				.getBoolean("jedis.pool.testOnBorrow"));
		config.setTestOnReturn(RedisConfig
				.getBoolean("jedis.pool.testOnReturn"));
		// 根据配置实例化jedis池
//		pool = new JedisPool(config, RedisConfig.getProperty("redis.ip"),
//				RedisConfig.getInt("redis.port"), clientTimeout);
	}

	public static Jedis getJedis() {
		return pool.getResource();
	}

	public static Jedis getJedis(int dbIdx) {
		Jedis jedis = pool.getResource();
		jedis.select(dbIdx);
		return jedis;
	}

	public static JedisPool getPool() {
		return pool;
	}

	public static void release(Jedis jedis) {
		if (null != jedis)
			jedis.close();
	}

}
