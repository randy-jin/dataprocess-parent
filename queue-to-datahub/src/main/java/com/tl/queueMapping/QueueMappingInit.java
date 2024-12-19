package com.tl.queueMapping;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.thoughtworks.xstream.XStream;
import com.tl.sqlMapping.SqlMapping;
import com.tl.sqlMapping.SqlMappingConfig;

import java.util.List;

public class QueueMappingInit {
	private static QueueMappingConfig queueMappingConfig;

	private static class SingletonHolder {
		private static final QueueMappingInit INSTANCE = new QueueMappingInit();
	}

	private QueueMappingInit() {
	}

	public static final QueueMappingInit getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public static void init() {
		XStream x = new XStream();
		x.alias("QueueMappingConfig", QueueMappingConfig.class);
		x.alias("QueueMapping", QueueMapping.class);
		ConfigFile configFile= ConfigService.getConfigFile("queueMapping", ConfigFileFormat.XML);
		queueMappingConfig = (QueueMappingConfig) x.fromXML(configFile.getContent());
		queueMappingConfig.fillMap();
		System.out.println("------------------------queue加载完成。。。。。");
	}

	public  List<QueueMapping> getAll(){
		return queueMappingConfig.getAll();
	}
}
