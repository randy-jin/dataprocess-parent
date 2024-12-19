package com;

import com.tl.QueueToDatahubApplication;
import com.tl.utils.helper.StandaloneRedisHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@Slf4j
@SpringBootTest(classes={QueueToDatahubApplication.class})
// 让 JUnit 运行 Spring 的测试环境， 获得 Spring 环境的上下文的支持
@RunWith(SpringRunner.class)
public class RedisTest {

    private static String defaultTime = "2021-11-22 11:08:20";

    @Test
    public void pushRedisTest(){
        RedisTemplate template = StandaloneRedisHelper.getRandomTemplate();
        String json = "{\"areaCode\":\"1203\",\"commAddr\":\"000000000000\",\"datas\":{\"000046972751\":[8,\"00100200\",\"1\",1637893874784,1637251200000,1090365,46533,536311,102378,405142]},\"tableName\":\"e_mp_day_read\",\"terminalAddr\":\"11\"}";
        template.opsForList().leftPush("Q_DATA_TEST", json);

    }

    @Test
    public void pushRedisTemplateTest(){
        RedisTemplate template = StandaloneRedisHelper.getRandomTemplate();
        template.executePipelined(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                int count = 0;
                for (int j = 0; j < 100; j++) {
//                    connection.openPipeline();
                    for (int i = 0; i < 2000; i++) {
                        count++;
                        connection.lPush("wjjtest".getBytes(), ("aa" + count).getBytes());
                    }
//                    connection.closePipeline();
                }
                return null;
            }
        });
        System.out.println("ssss");
    }

    @Test
    public void pushRedisTemplateRandomTest(){
        Long sTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            Long ssTime = System.currentTimeMillis();
            List<String> stringList = StandaloneRedisHelper.lpopPipline(10000, "wjjtest");
            System.out.println(stringList.size());
            System.out.println(stringList.get(0));
            Long eeTime = System.currentTimeMillis();
            System.out.println(i + "次时间：" + (eeTime-ssTime));
        }
        Long eTime = System.currentTimeMillis();
        Long time = eTime - sTime;
        System.out.println("总时间：" + time);
    }

}
