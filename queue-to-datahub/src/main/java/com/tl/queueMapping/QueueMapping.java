package com.tl.queueMapping;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class QueueMapping {

	private String handleType;//handle 类型
	private String queueName;//队列名称
	private String handleName;//线程名称
	private int maxThread;//最大线程数
	private int runThread;//运行线程数
	private String className;//运行类
	private int retryCount;//重试次数
	private int piplineCount;//一次获取数量

}
