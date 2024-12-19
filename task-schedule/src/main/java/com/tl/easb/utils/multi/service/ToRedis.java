package com.tl.easb.utils.multi.service;

import com.alibaba.fastjson.JSON;
import com.tl.easb.utils.multi.pojo.PipeObject;
import com.tl.iot.front.base.frame.serial.nezha.queue.PrivTypeSerialer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author Dongwei-Chen
 * @Date 2020/1/6 14:24
 * @Description 写入分布式队列
 */
public class ToRedis extends MultipleData {

    private  static PrivTypeSerialer privTypeSerialer=new PrivTypeSerialer();
    /**
     * @author Dongwei-Chen
     * @Date 2020/1/6 14:58
     * @Description 发送集合
     */
    public static int sendToRedis(String queue, List<Object> objects) throws Exception {
        Jedis jedis = getJedis();
        if (jedis == null) {
            return 0;
        }
        try {
            Pipeline pipeline = jedis.pipelined();
            if (pipelineCount == null) {
                pipelineCount = 10;
            }
            List<List<Object>> sendObj = splitList(objects, pipelineCount);
            for (int i = 0, j = sendObj.size(); i < j; i++) {
                try {
                    List<Object> send = sendObj.get(i);
                    List<Response<Long>> responses = new ArrayList<>();
                    for (int k = 0, s = send.size(); k < s; k++) {
                        Response<Long> response = pipeline.rpush(queue, JSON.toJSONString(send.get(k)));
                        responses.add(response);
                    }
                    //发送命令
                    pipeline.sync();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return objects.size();
    }

    /**
     * @author Dongwei-Chen
     * @Date 2020/1/8 9:50
     * @Description 通过管道发送对象
     */
    public static int sendToRedis(List<PipeObject> pipeObjects) throws Exception {
        Jedis jedis = getJedis();
        if (jedis == null) {
            return 0;
        }
        try {
            Pipeline pipeline = jedis.pipelined();
            if (pipelineCount == null) {
                pipelineCount = 10;
            }
            List<List<PipeObject>> sendObj = splitList(pipeObjects, pipelineCount);
            for (int i = 0, j = sendObj.size(); i < j; i++) {
                try {
                    List<PipeObject> send = sendObj.get(i);
                    List<Response<Long>> responses = new ArrayList<>();
                    for (int k = 0, s = send.size(); k < s; k++) {
                        PipeObject po = send.get(k);
                        String queue = po.getQueue();
                        Object object = po.getObject();
                        Response<Long> response = pipeline.rpush(queue, JSON.toJSONString(object));
                        responses.add(response);
                    }
                    //发送命令
                    pipeline.sync();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return pipeObjects.size();
    }

    /**
     * @author Dongwei-Chen
     * @Date 2020/1/6 14:58Redis
     * @Description 单对象或多对象发送
     */
    public static int sendToRedis(String queue, Object... objects) throws Exception {
        Jedis jedis = getJedis();
        if (jedis == null) {
            return 0;
        }
        try {
            for (int i = 0; i < objects.length; i++) {
//                jedis.rpush(queue, JSON.toJSONString(objects[i]));
                jedis.rpush(queue.getBytes(),privTypeSerialer.serial(objects[i]) );
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return objects.length;
    }
    public static int sendToRedisByJson(String queue, Object... objects) throws Exception {
        Jedis jedis = getJedis();
        if (jedis == null) {
            return 0;
        }
        try {
            for (int i = 0; i < objects.length; i++) {
                jedis.rpush(queue, JSON.toJSONString(objects[i]));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return objects.length;
    }

    /**
     * @author Dongwei-Chen
     * @Date 2020/1/6 14:13
     * @Description 循环获取jedis连接
     */
    public static Jedis getJedis() throws Exception {
        Jedis jedis;
        Random random = new Random();
        int count = 0;
        if (retry == null) {
            retry = 5;
        }
        while (count != retry) {
            try {
                JedisPool jedisPool = jedisPools.get(random.nextInt(jedisPools.size()));
                jedis = jedisPool.getResource();
                jedis.ping();
                return jedis;
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1);
            }
            count++;
        }
        return null;
    }

    /**
     * 根据配置文件中的Redis实例序号,获取对应的Jedis实例
     *
     * @param sn
     * @return
     * @throws Exception
     */
    public static Jedis getJedis(int sn) throws Exception {
        Jedis jedis;
        int count = 0;
        if (retry == null) {
            retry = 5;
        }
        while (count != retry) {
            try {
                JedisPool jedisPool = jedisPools.get(sn);
                jedis = jedisPool.getResource();
                jedis.ping();
                return jedis;
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1);
            }
            count++;
        }
        return null;
    }

    public static <T> List<List<T>> splitList(List<T> list, int len) {
        if ((list == null) || (list.isEmpty()) || (len < 1)) {
            return Collections.emptyList();
        }
        List result = new ArrayList();

        int size = list.size();
        int count = (size + len - 1) / len;

        for (int i = 0; i < count; ++i) {
            List subList = list.subList(i * len, ((i + 1) * len > size) ? size : len * (i + 1));
            result.add(subList);
        }
        return result;
    }
}
