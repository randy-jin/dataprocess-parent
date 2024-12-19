package com.tl.promethues;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
public class PromethuesInit {

	public void init(){
		try {
			new HTTPServer(13000);
//			DefaultExports.initialize();
		}catch (IOException e){

		}
	}
}
