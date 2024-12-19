package com.tl.promethues;

import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

public class PromethuesInit {

	public void init(){
		try {
			new HTTPServer(19090);
			
//			DefaultExports.initialize();
			
		}catch (IOException e){

		}
	}
}
