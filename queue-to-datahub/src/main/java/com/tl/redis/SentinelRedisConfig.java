package com.tl.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;


/**
 * 默认Redis实例
 *
 * @author jinzhiqiang
 */
@Configuration
@EnableCaching
public class SentinelRedisConfig extends RedisConfig {
    private static Logger logger = LoggerFactory.getLogger(SentinelRedisConfig.class);

//    @Autowired
//    StringRedisTemplate stringRedisTemplate;

    @Bean(name = "sentinelRedisTemplate")
    public RedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        logger.info("Initializing redis config ...");
        RedisTemplate redisTemplate = new RedisTemplate();
        setSerializer(redisTemplate, redisConnectionFactory);
        return redisTemplate;
    }


}
