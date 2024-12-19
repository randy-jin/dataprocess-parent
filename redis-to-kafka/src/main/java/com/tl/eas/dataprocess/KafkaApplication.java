package com.tl.eas.dataprocess;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource("classpath:config/springMvcConfig.xml")
public class KafkaApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
		System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow","|{}");
	}
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(KafkaApplication.class);
	}
}
