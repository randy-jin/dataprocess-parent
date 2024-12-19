package com.tl.dataprocess.kafka;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.datahub.DataHubShardCache;
import com.tl.dataprocess.datahub.SingleSubscriptionAsyncExecutor;
import com.tl.util.TaskThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Created by jinzhiqiang on 2021/9/9.
 * 从DataHub订阅数据,并写入kafka
 */
@EnableScheduling
public class KafkaProducer extends SingleSubscriptionAsyncExecutor {
    private Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    private String kafkaTopicName;//kafka topic名称

    private String filterField;
    private String filterIsId;

    public void setKafkaTopicName(String kafkaTopicName) {
        this.kafkaTopicName = kafkaTopicName;
    }

    private void sendMessage(String jsonStr) {
        kafkaTemplate.send(kafkaTopicName, jsonStr);
    }

    public void setFilterField(String filterField) {
        this.filterField = filterField;
    }

    public void setFilterIsId(String filterIsId) {
        this.filterIsId = filterIsId;
    }

    @Override
    protected void init() {
        if (isRun == 0) {
            return;
        }
        List<String> activeShardList = DataHubShardCache.getActiveShard(datahubClient, datahubPojectName, datahubTopicName);
            executor = Executors.newFixedThreadPool(threadPoolSize == 0 ? activeShardList.size() : threadPoolSize, new TaskThreadPool(datahubTopicName));
        for (final String activeShard : activeShardList) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    start(activeShard);
                }
            });
        }
    }

    @Override
    protected long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
        try {
            for (RecordEntry record : records) {
                offsetCtx.setOffset(record.getOffset());
                Map<String, Object> map = new HashMap<>(20);
                //循环接收到的数据，过滤datahub中的数据
                boolean isTrue=false;
                String dataHubVar =null;
                if (filterIsId == null || "".equals(filterIsId)) {
                    dataHubVar = record.get(filterField.toLowerCase()).toString();
                }else{
                    dataHubVar = record.get(filterIsId.toLowerCase()).toString();
                }


                switch (filterField.toUpperCase()) {
                    case "TERMINAL_ID":
                        isTrue = TimeToReadDRDS.TMNL_MAP.get(dataHubVar)==null?false:true;
                        break;
                    case "MPED_ID":
                        isTrue = TimeToReadDRDS.MPED_MAP.get(dataHubVar)==null?false:true;
                        break;
                    case "METER_ID":
                        isTrue = TimeToReadDRDS.METER_MAP.get(dataHubVar)==null?false:true;
                        break;
                }
//                if("4248558334".equals(dataHubVar)){
//                    System.out.println("succ");
//                }
                if(!isTrue){
                 continue;
                }
                for (Field field : record.getFields()) {
                    String fieldName = field.getName();
                    map.put(fieldName, record.get(fieldName));
                }
                JSONObject jsonObject = new JSONObject(map);
                logger.info("have a record to write kafkf "+jsonObject.toJSONString());
                sendMessage(jsonObject.toJSONString());

            }
        } catch (Exception e) {
            logger.error("Kafka producer error:", e);
        } finally {
            try {
                datahubClient.commitOffset(offsetCtx);
            } catch (Exception e) {
                throw new OffsetResetedException("提交偏移量时发生异常！");
            }
        }
        return recordNum;
    }
}
