package com.tl.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.exception.ResourceNotFoundException;
import com.aliyun.datahub.model.GetTopicResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataHub topic管理
 *
 * @author jinzhiqiang
 */
@Component
public class DataHubTopicMgr {
    private static Logger logger = LoggerFactory.getLogger(DataHubTopicMgr.class);

    @Autowired
    private DatahubClient datahubClient;

    private final Map<String, GetTopicResult> topicPool = new ConcurrentHashMap<String, GetTopicResult>();

    private DataHubTopicMgr() {
    }

    public GetTopicResult getTopic(String topic) {
        return topicPool.get(topic);
    }

    public void putTopic(String topic, GetTopicResult topicResult) {
        topicPool.put(topic, topicResult);
    }

    public GetTopicResult createAndGetTopic(String project, DataHubTopic dataHubTopic) {
        String topicName = dataHubTopic.getTopicName();
        GetTopicResult topicResult = null;
        synchronized (DataHubTopicMgr.class) {
            topicResult = getTopic(topicName);
            if (null == topicResult) {
                try {
                    topicResult = datahubClient.getTopic(project, topicName);
                    putTopic(topicName, topicResult);
                } catch (ResourceNotFoundException e) {
                    logger.error("Topic " + topicName + " is not exists,will be creating.", e);
                    datahubClient.createTopic(project, topicName, dataHubTopic.getShardCount(), dataHubTopic.getLifeCycle(), dataHubTopic.getRecordType(), dataHubTopic.getRecordSchema(), dataHubTopic.getDesc());
                    topicResult = datahubClient.getTopic(project, topicName);
                    putTopic(topicName, topicResult);
                }
                if (null == topicResult) {
                    logger.error("Topic " + topicName + " is not exists,will be creating.");
                    datahubClient.createTopic(project, topicName, dataHubTopic.getShardCount(), dataHubTopic.getLifeCycle(), dataHubTopic.getRecordType(), dataHubTopic.getRecordSchema(), dataHubTopic.getDesc());
                    topicResult = datahubClient.getTopic(project, topicName);
                    putTopic(topicName, topicResult);
                }
            }
        }
        return topicResult;
    }
}
