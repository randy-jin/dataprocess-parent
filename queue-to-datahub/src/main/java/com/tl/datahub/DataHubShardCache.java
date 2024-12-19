package com.tl.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.ShardEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 获取datahub中状态为ACTIVE的shard并缓存
 *
 * @author jinzhiqiang
 * @date 2017-12-28
 */
@Component
public class DataHubShardCache {
    private static Logger logger = LoggerFactory.getLogger(DataHubShardCache.class);
    @Autowired
    private DatahubClient datahubClient;

    private final HashMap<String, List<String>> mapActiveShard = new HashMap<>();

    private DataHubShardCache() {
    }

    public synchronized List<String> getActiveShard(String projectName, String topicName) {
        String key = projectName + "_" + topicName;
        List<String> activeShardList = mapActiveShard.get(key);
        if (activeShardList == null || activeShardList.size() == 0) {
            activeShardList = setListShardResult(projectName, topicName);
        }
        return activeShardList;
    }

    /**
     * 删除shard状态已过时的缓存
     *
     * @param projectName
     * @param topicName
     * @return
     */
    public List<String> delListShardResultByTopic(String projectName, String topicName) {
        String key = projectName + "_" + topicName;
        return mapActiveShard.remove(key);
    }

    private List<String> setListShardResult(String projectName, String topicName) {
        String key = projectName + "_" + topicName;
        ListShardResult listShard = datahubClient.listShard(projectName, topicName);
        List<ShardEntry> shardEntryList = listShard.getShards();
        List<String> activeShardList = new ArrayList<>(shardEntryList.size());
        for (int i = 0; i < shardEntryList.size(); i++) {
            ShardEntry shardEntry = shardEntryList.get(i);
            if (shardEntry.getState().name().equals("ACTIVE")) {
                activeShardList.add(shardEntry.getShardId());
            }
        }
        mapActiveShard.put(key, activeShardList);
        return activeShardList;
    }

}