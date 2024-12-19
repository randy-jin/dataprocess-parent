package com.tl.subjoin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ImportResource;

/**
 * @author Dongwei-Chen
 * @Date 2020/6/8 15:44
 * @Description 档案加载启动
 */
@SpringBootApplication(scanBasePackages = {"com.tl.subjoin.utils"})
@ImportResource(locations = {"classpath:spring/spring-task.xml"})
public class ArchivesSubjoinApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(ArchivesSubjoinApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(this.getClass());
    }
}
