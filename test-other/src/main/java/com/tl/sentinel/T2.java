package com.tl.sentinel;

import com.tl.redis.sentinel.SentinelRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by huangchunhuai on 2022/7/5.
 */
public class T2 {
    @Autowired
    SentinelRedisHelper sentinelRedisHelper;


    public void init(){
        sentinelRedisHelper.info();
    }

}
