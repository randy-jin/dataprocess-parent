package com.tl.utils.helper;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.GetTopicResult;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.common.seda.task.AutoMatch;
import com.tl.datahub.DataHubShardCache;
import com.tl.datahub.DataHubTopic;
import com.tl.datahub.DataHubTopicMgr;
import com.tl.dataprocess.utils.DataObject;
import com.tl.utils.PropertiesUtils;
import com.tl.utils.refreshkey.ClearHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DataHub入库工具类
 *
 * @author jinzhiqiang
 * @date 2019-09-03
 */
@Component
public class DatahubHelper {
    private static Logger logger = LoggerFactory.getLogger(DatahubHelper.class);

    @Value("${spring.datahub.project}")
    private String project;

    @Autowired
    private DatahubClient datahubClient;
    @Autowired
    private DataHubTopicMgr dataHubTopicMgr;
    @Autowired
    private DataHubShardCache dataHubShardCache;

    @AutoMatch
    private ClearHelper clearHelper;

    /**
     * 判断字段类型并填入entry对象
     * @param entry
     * @param data
     */
    private void fillRecEntry(RecordEntry entry, DataObject data) {
        List<Object> objList = data.getObjList();
        if(objList != null && objList.size() > 0) {
            int count = 0;
            for(Object obj : objList) {
                if(obj instanceof Date) {
                    entry.setTimeStampInDate(count, (Date) obj);
                } else if(obj instanceof String) {
                    entry.setString(count, String.valueOf(obj));
                } else if(obj instanceof Double) {
                    entry.setDouble(count, Double.valueOf(String.valueOf(obj)));
                } else if(obj instanceof Integer) {
                    entry.setBigint(count, Long.valueOf(String.valueOf(obj)));
                } else if(obj instanceof BigDecimal) {
                    entry.setBigint(count, Long.valueOf(String.valueOf(obj)));
                }
                count++;
            }
        }
    }


    /**
     * 写入datahub
     * @param doList
     * @return
     * @throws Exception
     */
    public PutRecordsResult insert(List<DataObject> doList) throws Exception {
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>(doList.size());
        List<Object[]> refreshKeyArrList = new ArrayList<>(doList.size());
        String batchTopicFlag = null;
        for(DataObject dataObject : doList){
            DataHubTopic dataHubTopic = dataObject.getDataHubTopic();
            String topicName = dataHubTopic.getTopicName();
            if(batchTopicFlag == null){
                batchTopicFlag = topicName;
            }
            //此处先检查topic是否存在，不存在的话会在project中创建
            GetTopicResult topicResult = dataHubTopicMgr.getTopic(topicName);
            if(null == topicResult){
                topicResult = dataHubTopicMgr.createAndGetTopic(project, dataHubTopic);
            }
            //recordEntry录入数据
            //todo可根据实际性能情况改为批量写入
            RecordSchema topicSchema = topicResult.getRecordSchema();//表Schema

            RecordEntry entry = new RecordEntry(topicSchema);
            fillRecEntry(entry, dataObject);//字段=数据
            entry.setShardId(dataObject.getShardId()); //通道id

            if(logger.isInfoEnabled()){
                logger.info("You have a new message write to project " + project + " topic " + topicName + " and shard id " + dataObject.getShardId());
            }
            if(batchTopicFlag != topicName){
                PutRecordsResult putRecordsResult = datahubClient.putRecords(project, batchTopicFlag, recordEntries);
                putRecordsResult = refresh(project, batchTopicFlag, refreshKeyArrList, putRecordsResult);
                if(putRecordsResult.getFailedRecordCount() > 0){
                    logger.error("Warning!!!!!!Write to datahub " + batchTopicFlag + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId());
                }
                batchTopicFlag = topicName;
                recordEntries.clear();
                refreshKeyArrList.clear();
            } else {
                if(null != dataObject.getKey()){
                    refreshKeyArrList.add(dataObject.getKey());
                }
                recordEntries.add(entry);
                continue;
            }
            if(null != dataObject.getKey()){
                refreshKeyArrList.add(dataObject.getKey());
            }
            recordEntries.add(entry);
        }
        PutRecordsResult putRecordsResult = datahubClient.putRecords(project, batchTopicFlag, recordEntries);
        putRecordsResult = refresh(project, batchTopicFlag, refreshKeyArrList, putRecordsResult);
        if(putRecordsResult.getFailedRecordCount() > 0){
            logger.error("Warning!!!!!!Write to datahub " + batchTopicFlag + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId());
        }
        recordEntries.clear();
        refreshKeyArrList.clear();

        return putRecordsResult;
    }
    private PutRecordsResult refresh(String project, String topicName, List<Object[]> refreshKeyArrList, PutRecordsResult putRecordsResult) {
        if(putRecordsResult.getFailedRecordCount() == 0){
            doPut(refreshKeyArrList);
        } else {
            for(RecordEntry failedRecordEntry : putRecordsResult.getFailedRecords()){
                logger.error("WriteFailed record topic is ["+topicName+"], who's jsonNode is:"+failedRecordEntry.toJsonNode());
            }
            logger.error("Warning!!!!!!Write to datahub " + topicName + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId() + ",to update DataHub-shard-cache.");

            dataHubShardCache.delListShardResultByTopic(project, topicName);
            List<String> activeShardList = dataHubShardCache.getActiveShard( project, topicName);
            for(RecordEntry recordEntry : putRecordsResult.getFailedRecords()){
                int index = Math.abs(recordEntry.hashCode()) % activeShardList.size();
                recordEntry.setShardId(activeShardList.get(index));
            }
            PutRecordsResult failedRecordsResult = datahubClient.putRecords(project, topicName, putRecordsResult.getFailedRecords());
            if(failedRecordsResult.getFailedRecordCount() == 0){
                doPut(refreshKeyArrList);
            }
        }
        return putRecordsResult;
    }

    /**
     * 写入datahub的记录put队列
     * @param refreshKeyArrList
     */
    private static void doPut(List<Object[]> refreshKeyArrList) {
        if(refreshKeyArrList.size() == 0){
            return;
        }
        for(Object[] refreshKey : refreshKeyArrList){
            if(null != refreshKey){
                try {
                    PropertiesUtils.clearQueue.put(refreshKey);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
