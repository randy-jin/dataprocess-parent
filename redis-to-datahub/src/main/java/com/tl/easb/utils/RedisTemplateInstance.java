package com.tl.easb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.JedisSentinelPool;

public class RedisTemplateInstance {

    private static Logger log = LoggerFactory.getLogger(RedisTemplateInstance.class);

    public static RedisTemplate redisTemplate = null;

    public static JedisSentinelPool jedisSentinelPool = null;

    static {
//		if(redisTemplate == null){
//			redisTemplate = (RedisTemplate)SpringUtils.getBean("redisSentinelTemplate");
//			log.info("Initializeds redisTemplate");
//		}

//		if(jedisSentinelPool == null){
//			jedisSentinelPool = (JedisSentinelPool)SpringUtils.getBean("jedisSentinelPool");
//			log.info("Initialized jedisSentinelPool");
//		}
    }
}
