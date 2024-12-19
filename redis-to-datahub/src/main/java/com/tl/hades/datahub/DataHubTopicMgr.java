package com.tl.hades.datahub;

import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.GetTopicResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataHub topic管理
 * @author jinzhiqiang
 *
 */
public class DataHubTopicMgr {
	private final static Logger logger = LoggerFactory.getLogger(DataHubTopicMgr.class);

	private static final Map<String,GetTopicResult> topicPool = new ConcurrentHashMap<String,GetTopicResult>();

	private DataHubTopicMgr(){}

	public static GetTopicResult getTopic(String topic) {
		return topicPool.get(topic);
	}

	public static void putTopic(String topic, GetTopicResult topicResult){
		topicPool.put(topic, topicResult);
	}

	public static GetTopicResult createAndGetTopic(String project, DataHubTopic dataHubTopic) {
		String topicName = dataHubTopic.getTopicName();
		GetTopicResult topicResult = null;
		synchronized (DataHubTopicMgr.class) {
			topicResult = getTopic(topicName);
			if(null == topicResult){
				try {
					topicResult = DataHubProps.client.getTopic(project, topicName);
					putTopic(topicName, topicResult);
				} catch (ResourceNotFoundException e) {
					logger.error("Topic " + topicName + " is not exists,will be creating.",e);
					DataHubProps.client.createTopic(project, topicName, dataHubTopic.getShardCount(), dataHubTopic.getLifeCycle(), dataHubTopic.getRecordType(), dataHubTopic.getRecordSchema(), dataHubTopic.getDesc());
					topicResult = DataHubProps.client.getTopic(project, topicName);
					putTopic(topicName, topicResult);
				}
				if(null == topicResult){
					logger.error("Topic " + topicName + " is not exists,will be creating.");
					DataHubProps.client.createTopic(project, topicName, dataHubTopic.getShardCount(), dataHubTopic.getLifeCycle(), dataHubTopic.getRecordType(), dataHubTopic.getRecordSchema(), dataHubTopic.getDesc());
					topicResult = DataHubProps.client.getTopic(project, topicName);
					putTopic(topicName, topicResult);
				}
			}
		}
		return topicResult;
	}
}
