package com.ls.athena.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Dongwei-Chen
 * @Date 2020/6/10 16:58
 * @Description redisTemplate配置
 */
@Configuration
@EnableCaching
public class RedisConfig {

    @Autowired
    private JedisPoolConfig jedisPoolConfig;

    @Value("${spring.redis.host}")
    private String hostName;

    @Value("${spring.redis.port}")
    private int port;

    @Value("${spring.redis.password}")
    private String pass;

    @Value("${spring.redis.database}")
    private int database;

    // 以下两种redisTemplate自由根据场景选择
    @Bean(name = "defaultRedisTemplate")
    public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<Object, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);

        template.setValueSerializer(new StringRedisSerializer());
        template.setDefaultSerializer(new StringRedisSerializer());
        //使用StringRedisSerializer来序列化和反序列化redis的key值
        template.setKeySerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
        stringRedisTemplate.setConnectionFactory(redisConnectionFactory);
        return stringRedisTemplate;
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
//        RedisStandaloneConfiguration jsc = new RedisStandaloneConfiguration();
//        jsc.setHostName(hostName);
//        jsc.setPort(port);
//        jsc.setPassword(pass);
//        jsc.setDatabase(database);
        RedisConnectionFactory rf = new JedisConnectionFactory();
        ((JedisConnectionFactory) rf).setPoolConfig(jedisPoolConfig);
        ((JedisConnectionFactory) rf).setHostName(hostName);
        ((JedisConnectionFactory) rf).setPort(port);
        ((JedisConnectionFactory) rf).setPassword(pass);
        ((JedisConnectionFactory) rf).setDatabase(database);
        ((JedisConnectionFactory) rf).setTimeout(60000);
        return rf;
    }
}
