package com.tl.easb.utils.multi.service;

import com.tl.easb.utils.multi.pojo.JedisManager;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Dongwei-Chen
 * @Date 2019/8/20 16:40
 * @Description 多数据源(分布式队列)监听
 * 通过多线程轮询分布式队列，获取数据汇集到指定队列进行处理
 */
public class MultipleData {

    private JedisManager jedisManager;


    private List<String> conList;
    /**
     * 管道数
     */
    protected static Integer pipelineCount;
    /**
     * 重试次数
     */
    protected static Integer retry;


    protected static List<JedisPool> jedisPools = new ArrayList<>();

    public JedisManager getJedisManager() {
        return jedisManager;
    }

    public void setJedisManager(JedisManager jedisManager) {
        this.jedisManager = jedisManager;
    }

    public List<String> getConList() {
        return conList;
    }

    public void setConList(List<String> conList) {
        this.conList = conList;
    }

    private void startListener() {
        pipelineCount = jedisManager.getPipelineCount();
        retry = jedisManager.getRetry();
        JedisPoolConfig jedisPoolConfig = jedisManager.getJedisPoolConfig();
        for (String con : conList) {
            String[] cn = con.split(":");
            //连接池
            JedisPool jedisPool;
            if (cn.length > 2 && null != cn[2] && !"".equals(cn[2])) {
                jedisPool = new JedisPool(jedisPoolConfig, cn[0], Integer.valueOf(cn[1]), 1000, cn[2]);
            } else {
                jedisPool = new JedisPool(jedisPoolConfig, cn[0], Integer.valueOf(cn[1]));
            }
            jedisPools.add(jedisPool);
        }
    }
}
