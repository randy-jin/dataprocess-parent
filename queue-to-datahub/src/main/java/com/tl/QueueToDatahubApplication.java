package com.tl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by huangchunhuai on 2021/11/22.
 */
@SpringBootApplication
@ImportResource("classpath:config/*.xml")
public class QueueToDatahubApplication extends SpringBootServletInitializer {
    public static void main(String[] args) {
        SpringApplication.run(QueueToDatahubApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(this.getClass());
    }
}
