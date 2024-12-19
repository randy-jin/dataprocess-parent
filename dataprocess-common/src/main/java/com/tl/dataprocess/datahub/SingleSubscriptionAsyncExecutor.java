package com.tl.dataprocess.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 从DataHub的topic中订阅数据(在子类中进行数据下游处理)
 *
 * @author jinzhiqiang
 */
public abstract class SingleSubscriptionAsyncExecutor {
    private Logger logger = LoggerFactory.getLogger(SingleSubscriptionAsyncExecutor.class);

    protected static ExecutorService executor = null;
    protected int threadPoolSize;

    protected DatahubClient datahubClient;
    protected String datahubPojectName;
    protected String datahubTopicName;
    protected String datahubSubId;
    private int limit = 1000;
    protected int isRun = 1;
    protected String tableField;
    protected String isID;

    public void setIsID(String isID) {
        this.isID = isID;
    }

    public void setTableField(String tableField) {
        this.tableField = tableField;
    }

    public void setIsRun(int isRun) {
        this.isRun = isRun;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public void setDatahubPojectName(String datahubPojectName) {
        this.datahubPojectName = datahubPojectName;
    }

    public void setDatahubTopicName(String datahubTopicName) {
        this.datahubTopicName = datahubTopicName;
    }

    public void setDatahubSubId(String datahubSubId) {
        this.datahubSubId = datahubSubId;
    }

    public void setDatahubClient(DatahubClient datahubClient) {
        this.datahubClient = datahubClient;
    }

    protected abstract void init();

    /**
     * 根据DataHub的活跃shard进行数据订阅
     *
     * @param activeShard
     */
    protected void start(String activeShard) {
        try {
            boolean bExit = false;
            GetTopicResult topicResult = datahubClient.getTopic(datahubPojectName, datahubTopicName);
            // 首先初始化offset上下文
            OffsetContext offsetCtx = datahubClient.initOffsetContext(datahubPojectName, datahubTopicName, datahubSubId, activeShard);
            String cursor = null; // 开始消费的cursor
            if (!offsetCtx.hasOffset()) {
                // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
                GetCursorResult cursorResult = datahubClient.getCursor(datahubPojectName, datahubTopicName, activeShard, CursorType.OLDEST);
                cursor = cursorResult.getCursor();
            } else {
                try {
                    // 否则，获取当前已消费点位的下一个cursor
                    cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
                } catch (Exception ex) {
                    logger.error("获取当前已消费点位的下一个cursor时发生异常:", ex);
                    // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
                    GetCursorResult cursorResult = datahubClient.getCursor(datahubPojectName, datahubTopicName, activeShard, CursorType.OLDEST);
                    cursor = cursorResult.getCursor();
                }
            }
            logger.info("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
            long recordNum = 0L;
            while (!bExit) {
                try {
                    GetRecordsResult recordResult = datahubClient.getRecords(datahubPojectName, datahubTopicName, activeShard, cursor, limit, topicResult.getRecordSchema());
                    List<RecordEntry> records = recordResult.getRecords();
                    if (null == records || records.size() == 0) {
                        // 将最后一次消费点位上报
                        datahubClient.commitOffset(offsetCtx);
                        logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                        // 可以先休眠一会，再继续消费新记录
                        Thread.sleep(1000);
                        logger.info("sleep 1s and continue consume records! shard id:" + activeShard);
                    } else {
                        recordNum = dataProcess(offsetCtx, recordNum, records);
                        cursor = recordResult.getNextCursor();
//                        logger.info("Write to rdb success, the current shard_id is:" + activeShard + ",the recordNum is:" + recordNum);
                    }
                } catch (SubscriptionOfflineException e) {
                    // 订阅下线，退出
                    bExit = true;
                    logger.error("SubscriptionOfflineException:", e);
                } catch (OffsetResetedException e) {
                    try {
                        // 点位被重置，更新offset上下文
                        datahubClient.updateOffsetContext(offsetCtx);
                        // 否则，获取当前已消费点位的下一个cursor
                        cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
                    } catch (Exception ex) {
                        logger.error("获取当前已消费点位的下一个cursor时发生异常:", ex);
                        // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
                        GetCursorResult cursorResult = datahubClient.getCursor(datahubPojectName, datahubTopicName, activeShard, CursorType.OLDEST);
                        cursor = cursorResult.getCursor();
                    }
                    logger.error("Restart consume shard:" + activeShard + ", reset offset:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
                } catch (OffsetSessionChangedException e) {
                    // 其他consumer同时消费了该订阅下的相同shard，退出
                    bExit = true;
                    logger.error("OffsetSessionChangedException:", e);
                } catch (Exception e) {
                    try {
                        datahubClient.commitOffset(offsetCtx);
                    } catch (Exception ie) {
                        logger.error("提交偏移量时发生异常:", e);
                    }
                    // bExit = true;
                    logger.error("Other Exception:", e);
                }
            }
        } catch (Exception e) {
            logger.error("Exception ", e);
        }
    }

    /**
     * 将订阅获取的数据实时写入下游存储介质(具体业务在子类中进行实现)
     *
     * @param offsetCtx
     * @param recordNum
     * @param records
     * @return
     */
    protected abstract long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records);

}