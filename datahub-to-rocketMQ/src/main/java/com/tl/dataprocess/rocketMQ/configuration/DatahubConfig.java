package com.tl.dataprocess.rocketMQ.configuration;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/5 17:28
 * @description ：
 * @version: 1.0.0.0
 */
@Configuration
public class DatahubConfig {

    @Value("${datahub.default.endpoint}")
    private String endpoint;

    @Value("${datahub.default.access.id}")
    private String id;

    @Value("${datahub.default.access.key}")
    private String key;

    @Bean
    public DatahubClient datahubClient(){
        AliyunAccount aliyunAccount = new AliyunAccount(id,key);
        DatahubConfiguration datahubConfiguration = new DatahubConfiguration(aliyunAccount,endpoint);

        DatahubClient client = new DatahubClient(datahubConfiguration);
        return client;
    }
}
