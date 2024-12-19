package com.tl.queueMapping;


import com.tl.sqlMapping.SqlMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class QueueMappingConfig {

	private  List<QueueMapping> queuelist;
	private Map<String, Object> listQueueMap;

	public QueueMappingConfig() {
	}



	protected void fillMap() {
		listQueueMap = new HashMap<String, Object>();
		addToMap(queuelist);
	}

	protected void addToMap(List<QueueMapping> listSql) {
		if (listSql == null || listSql.isEmpty()) {
			return;
		}
		listQueueMap = new HashMap<String, Object>();
		for (int i = 0; i < listSql.size(); i++) {
			QueueMapping mapping = (QueueMapping) listSql.get(i);
			listQueueMap.put(mapping.getQueueName(),mapping);
		}
	}

	protected List<QueueMapping> getAll(){
		return queuelist;
	}
}
