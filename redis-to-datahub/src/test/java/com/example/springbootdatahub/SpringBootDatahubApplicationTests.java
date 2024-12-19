package com.example.springbootdatahub;

import com.tl.easb.utils.SpringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.ApplicationContextEvent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

@SpringBootTest
class SpringBootDatahubApplicationTests {

    @Autowired
    SpringUtils su;
    @Test
    void contextLoads() {
        SetParams sp = SetParams.setParams().nx().ex(1000);
        System.out.println(su);
    }

}
