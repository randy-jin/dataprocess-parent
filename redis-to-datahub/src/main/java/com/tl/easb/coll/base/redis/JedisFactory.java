package com.tl.easb.coll.base.redis;

import redis.clients.jedis.Jedis;

public class JedisFactory {
	public static Jedis getInstance() {
		String ip = RedisConfig.getProperty("redis.ip");
		int port = RedisConfig.getInt("redis.port");
		return getInstance(ip, port);
	}
	public static Jedis getInstance(int dbIdx){
		Jedis jedis= getInstance();
		jedis.select(dbIdx);
		return jedis;
	}

	public static void disconnect(Jedis jedis) {
		jedis.disconnect();
	}

	public static Jedis getInstance(String ip, int port) {
		int clientTimeout = 1000000;
		String idleTimeout = String.valueOf(clientTimeout);
		Jedis jedis = new Jedis(ip, port, clientTimeout);
		jedis.connect();
		jedis.configSet("timeout", idleTimeout);
		return jedis;
	}
}
