package com.tl.cluster;

import com.tl.redis.cluster.ClusterRedisHelper;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by huangchunhuai on 2022/7/5.
 */
public class T1 {

   @Autowired
   ClusterRedisHelper clusterRedisHelper;


   public void init(){
      clusterRedisHelper.info();
   }

}
