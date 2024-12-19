package com.tl.promethues;

import java.awt.List;
import java.util.Timer;
import java.util.TimerTask;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Gauge;

public class PromethuesUtil {

	public static final Gauge sendTotal = Gauge.build().name("send_total").help("xx").
			labelNames("total").register();
	
	public static final Gauge sendTaskID = Gauge.build().name("send_taskId").help("xx").
			labelNames("taskId").register();

	//清零操作
	private double a = 0;
	
	public void run(){
		
		Timer timer = new Timer();
		
		timer.schedule(new TimerTask(){
			@Override
			public void run() {
				//做两个指标，用总的指标来做清零处理
				double counter = sendTotal.labels("total").get();
				
				

				if(counter ==a){

					sendTotal.clear();
					
					sendTaskID.clear();
					
					a = 0;

				}else if(counter>a){

					a=counter;

				}	
			}
		}, 3000, 30000);
	}
	
}
