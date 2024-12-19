package com.tl.redis.sentinel;

import com.tl.redis.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;


/**
 * 默认Redis实例
 *
 * @author jinzhiqiang
 */
@Configuration
@EnableCaching
public class SentinelRedisConfig extends RedisConfig {
    private static Logger logger = LoggerFactory.getLogger(SentinelRedisConfig.class);


    @Value("${spring.sentinel.nodes}")
    private String nodes;
    @Value("${spring.sentinel.master}")
    private String masterName;
    @Value("${spring.redis.password}")
    private String password;

    @Bean(name = "sentinelRedisTemplate")
    public RedisTemplate sentinelRedisTemplate() {
        RedisTemplate redisTemplate = new RedisTemplate();
        setSerializer(redisTemplate,getsentinelRedisConnectionFactory());
        return redisTemplate;
    }


    @Bean
    public RedisSentinelConfiguration sentinelConfiguration(){
        RedisSentinelConfiguration sentinelConfiguration=new RedisSentinelConfiguration();
        Set<RedisNode> redisNodes=new HashSet<>();
        String[] cNodes = nodes.split(",");
        for (String node : cNodes) {
            String[] hp = node.split(":");
            redisNodes.add(new RedisNode(hp[0], Integer.parseInt(hp[1])));
        }
        sentinelConfiguration.setMaster(masterName);
        sentinelConfiguration.setSentinels(redisNodes);
        sentinelConfiguration.setPassword(password);
//        sentinelConfiguration.setSentinelPassword(password);

        return sentinelConfiguration;
    }



    /**
     * 配置redis连接工厂
     *
     * @return
     */
    public RedisConnectionFactory getsentinelRedisConnectionFactory() { // 是负责建立Factory的连接工厂类
        JedisConnectionFactory jedisFactory=new JedisConnectionFactory(sentinelConfiguration(),getJedisPoolConfig());
        return jedisFactory;
    }
}
