package com.tl.redis.cluster;

import com.tl.redis.RedisHelper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Redis默认实例工具类
 *
 * @author jinzhiqiang
 * @date 2019-08-08
 */
@Component
public class ClusterRedisHelper extends RedisHelper {
//    @Resource(name = "clusterStringRedisTemplate")
//    public StringRedisTemplate stringRedisTemplate;

    @Resource(name = "clusterRedisTemplate")
    public RedisTemplate redisTemplate;


}
