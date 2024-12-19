package com.tl.redis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式Redis实例
 *
 * @author jinzhiqiang
 */
@Configuration
@EnableCaching
public class ClusterRedisConfig extends RedisConfig {

    @Value("${spring.redis-cluster.cluster.nodes}")
    private String nodes;
    @Value("${spring.redis-cluster.password}")
    private String password;
    @Value("${spring.redis-cluster.timeout}")
    private int timeout;
    @Value("${spring.redis-cluster.cluster.max-redirects}")
    private int maxRedirects;

    @Bean(name = "clusterRedisTemplate")
    public RedisTemplate clusterRedisTemplate() {
        RedisTemplate template = new RedisTemplate();
        setSerializer(template, getJedisConnectionFactory(getJedisCluster(), getJedisPoolConfig()));
        return template;
    }

    @Override
    protected void setSerializer(RedisTemplate redisTemplate, RedisConnectionFactory redisConnectionFactory) {
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());
//        redisTemplate.setKeySerializer(new StringRedisSerializer());
//        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
//        redisTemplate.setHashValueSerializer(new JdkSerializationRedisSerializer());
//        redisTemplate.setValueSerializer(new JdkSerializationRedisSerializer());
        redisTemplate.afterPropertiesSet();
    }

    @Bean
    public RedisClusterConfiguration getJedisCluster() {
        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
        redisClusterConfiguration.setMaxRedirects(maxRedirects);

        List<RedisNode> nodeList = new ArrayList<>();
        String[] cNodes = nodes.split(",");
        //分割出集群节点
        for (String node : cNodes) {
            String[] hp = node.split(":");
            nodeList.add(new RedisNode(hp[0], Integer.parseInt(hp[1])));
        }
        redisClusterConfiguration.setClusterNodes(nodeList);
        redisClusterConfiguration.setPassword(password);

        return redisClusterConfiguration;
    }

    public JedisConnectionFactory getJedisConnectionFactory(RedisClusterConfiguration redisClusterConfiguration, JedisPoolConfig jedisPoolConfig) {
        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(redisClusterConfiguration, jedisPoolConfig);
        jedisConnectionFactory.afterPropertiesSet();
        return jedisConnectionFactory;
    }

}
