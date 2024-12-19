package com.tl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by huangchunhuai on 2021/11/22.
 */
@SpringBootApplication(scanBasePackages = "com.tl.redis.sentinel")
//@SpringBootApplication(scanBasePackages = "com.tl.redis.cluster")
//@SpringBootApplication(scanBasePackages = "com.tl.redis.standalone")
@ImportResource("classpath:config/*.xml")
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
