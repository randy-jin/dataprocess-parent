package com.tl.redis.standalone;

import com.tl.redis.RedisConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 档案Redis实例
 *
 * @author jinzhiqiang
 */
@Configuration
@EnableCaching
public class StandaloneRedisConfig extends RedisConfig {

    @Value("${spring.standalone.database}")
    private int dbIndex;
    @Value("${spring.standalone.host}")
    private String host;
    @Value("${spring.standalone.port}")
    private int port;
    @Value("${spring.standalone.password}")
    private String password;
    @Value("${spring.standalone.timeout}")
    private int timeout;

    /**
     * 配置redis连接工厂
     *
     * @return
     */
    @Bean
    public RedisConnectionFactory getstarandaloneRedisConnectionFactory() { // 是负责建立Factory的连接工厂类
        JedisPoolConfig jedisPoolConfig = getJedisPoolConfig();
        JedisConnectionFactory jedisFactory = new JedisConnectionFactory();
        jedisFactory.setHostName(host);
        jedisFactory.setPort(port);
        jedisFactory.setPassword(password);
        jedisFactory.setDatabase(dbIndex);
        jedisFactory.setPoolConfig(jedisPoolConfig);
        jedisFactory.afterPropertiesSet(); // 初始化连接池配置
        return jedisFactory;
    }

    /**
     * 配置redisTemplate 注入方式使用@Resource(name="") 方式注入
     *
     * @return
     */
    @Bean(name = "standaloneRedisTemplate")
    public RedisTemplate standaloneRedisTemplate() {
        RedisTemplate template = new RedisTemplate();
        setSerializer(template, getstarandaloneRedisConnectionFactory());
        return template;
    }


}
