package com.tl.easb.utils.multi.pojo;

import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Dongwei-Chen
 * @Date 2019/8/27 19:14
 * @Description redis实体类
 */
public class JedisManager {

   
    /**
     * 管道读取数
     */
    private Integer pipelineCount;
    /**
     * redis连接重试次数
     */
    private Integer retry;
    /**
     * jedis连接池配置
     */
    private JedisPoolConfig jedisPoolConfig;

  

    public Integer getRetry() {
        return retry;
    }

    public void setRetry(Integer retry) {
        this.retry = retry;
    }
   

    public Integer getPipelineCount() {
		return pipelineCount;
	}

	public void setPipelineCount(Integer pipelineCount) {
		this.pipelineCount = pipelineCount;
	}

	public JedisPoolConfig getJedisPoolConfig() {
        return jedisPoolConfig;
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }
}
