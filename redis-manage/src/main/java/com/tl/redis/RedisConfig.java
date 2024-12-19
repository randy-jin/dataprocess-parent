package com.tl.redis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis配置类
 *
 * @author jinzhiqiang
 */
public class RedisConfig {

    @Value("${spring.redis.jedis.pool.max-active}")
    protected int redisPoolMaxActive;
    @Value("${spring.redis.jedis.pool.max-wait}")
    protected int redisPoolMaxWait;
    @Value("${spring.redis.jedis.pool.max-idle}")
    protected int redisPoolMaxIdle;
    @Value("${spring.redis.jedis.pool.min-idle}")
    protected int redisPoolMinIdle;

    protected void setSerializer(RedisTemplate redisTemplate, RedisConnectionFactory redisConnectionFactory) {
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate.afterPropertiesSet();
    }

    protected JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig(); // 进行连接池配置
        poolConfig.setMaxTotal(redisPoolMaxActive);
        poolConfig.setMaxIdle(redisPoolMaxIdle);
        poolConfig.setMinIdle(redisPoolMinIdle);
        poolConfig.setMaxWaitMillis(redisPoolMaxWait);
        return poolConfig;
    }

}