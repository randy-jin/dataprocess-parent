package com.example.demo;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class Test {
	public static void main(String[] args) {
		ApplicationContext context = new FileSystemXmlApplicationContext("classpath:redis.xml");
	}
	
	public void aa(){
		System.out.println("aaaaaaaa");
	}
}
