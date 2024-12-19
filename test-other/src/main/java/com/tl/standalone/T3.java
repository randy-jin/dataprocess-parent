package com.tl.standalone;

import com.tl.redis.standalone.StandaloneRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by huangchunhuai on 2022/7/6.
 */
public class T3 {

    @Autowired
    StandaloneRedisHelper standaloneRedisHelper;


    public void init(){
        standaloneRedisHelper.info();
    }
}
