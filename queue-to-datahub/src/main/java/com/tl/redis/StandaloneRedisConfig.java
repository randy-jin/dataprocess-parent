package com.tl.redis;

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

    @Value("${spring.redis-archives.database}")
    private int dbIndex;
    @Value("${spring.redis-archives.hosts}")
    private String hosts;
    @Value("${spring.redis-archives.port}")
    private int port;
    @Value("${spring.redis-archives.password}")
    private String password;
    @Value("${spring.redis-archives.timeout}")
    private int timeout;


    @Getter
    @Setter
    protected static List<JedisConnectionFactory> jedisConnectionFactories = new ArrayList<>();

    @Getter
    @Setter
    public static List<RedisTemplate> redisTemplateList = new ArrayList<>();

    @Bean
    public void redisPoolManager() {
        JedisPoolConfig jedisPoolConfig = getJedisPoolConfig();
        String[] hostList = hosts.split(",");
        for (String host : hostList) {
            JedisConnectionFactory jedisFactory = new JedisConnectionFactory();
            jedisFactory.setPoolConfig(jedisPoolConfig);
            jedisFactory.setDatabase(dbIndex);
            jedisFactory.setHostName(host);
            jedisFactory.setPort(port);
            jedisFactory.setPassword(password);
            jedisConnectionFactories.add(jedisFactory);
        }

        for (JedisConnectionFactory jedisConnectionFactory : jedisConnectionFactories) {
            RedisTemplate redisTemplate = new RedisTemplate();
            redisTemplate.setConnectionFactory(jedisConnectionFactory);
            redisTemplate.setDefaultSerializer(new StringRedisSerializer());
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setHashKeySerializer(new StringRedisSerializer());
            redisTemplate.setHashValueSerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            redisTemplate.afterPropertiesSet();
            redisTemplateList.add(redisTemplate);
        }
    }

    public static RedisTemplate getRandomTemplate() {
        Random random = new Random();
        return redisTemplateList.get(random.nextInt(redisTemplateList.size()));
    }

    /**
     * 配置redis连接工厂
     *
     * @return
     */
    public static JedisConnectionFactory getJedisConnectionFactory(Integer num) {
        Random random = new Random();
        int count = 0;
        JedisConnectionFactory jcf;
        while (count != jedisConnectionFactories.size()) {
            try {
                if (null == num || num > jedisConnectionFactories.size()-1 || num < 0) {
                    jcf = jedisConnectionFactories.get(random.nextInt(jedisConnectionFactories.size()));
                } else {
                    jcf = jedisConnectionFactories.get(num);
                }
                jcf.afterPropertiesSet();
                return jcf;
            } catch (Exception e) {
                e.printStackTrace();
            }
            count++;
        }
        return null;
    }

}
