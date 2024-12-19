package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse.RowResult;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import com.aliyun.datahub.model.*;

import com.tl.dataprocess.datahub.DataHubShardCache;
import com.tl.util.TaskThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * 从DataHub的topic中订阅数据并以“同步”方式批量写入tablestore
 *
 * @author jinzhiqiang
 */
public class SingleSubscriptionExecutor extends SubscriptionExecutor {
    private static Logger logger = LoggerFactory.getLogger(SingleSubscriptionExecutor.class);

    private SyncClientInterface syncClient;

    public void setSyncClient(SyncClientInterface syncClient) {
        this.syncClient = syncClient;
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
    protected void start(String activeShard) {
        try {
            boolean bExit = false;
            GetTopicResult topicResult = datahubClient.getTopic(datahubPojectName, datahubTopicName);
            // 首先初始化offset上下文
            OffsetContext offsetCtx = datahubClient.initOffsetContext(datahubPojectName, datahubTopicName, datahubSubId, activeShard);
            String cursor = null; // 开始消费的cursor
            if (!offsetCtx.hasOffset()) {
                // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
                GetCursorResult cursorResult = datahubClient.getCursor(datahubPojectName, datahubTopicName, activeShard, GetCursorRequest.CursorType.OLDEST);
                cursor = cursorResult.getCursor();
            } else {
                // 否则，获取当前已消费点位的下一个cursor
                cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
            }
            logger.info("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
            long recordNum = 0L;
            while (!bExit) {
                try {
                    GetRecordsResult recordResult = datahubClient.getRecords(datahubPojectName, datahubTopicName, activeShard, cursor, datahubSearchSize, topicResult.getRecordSchema());
                    int recordCount = recordResult.getRecordCount();
                    if (recordCount != 0) {
                        logger.info("Get record result count:" + recordResult.getRecordCount());
                    }
                    List<RecordEntry> records = recordResult.getRecords();
                    if (null == records || records.size() == 0) {
                        // 将最后一次消费点位上报
                        datahubClient.commitOffset(offsetCtx);
                        //						logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                        // 可以先休眠一会，再继续消费新记录
                        Thread.sleep(5000);
                        //						logger.info("sleep 1s and continue consume records! shard id:" + datahub_shardId);
                    } else {
                        List<BatchWriteRowResponse> batchWriteRowResponseList = new ArrayList<BatchWriteRowResponse>();
                        recordNum = batchWriteRow(offsetCtx, recordNum, records, batchWriteRowResponseList);
                        cursor = recordResult.getNextCursor();

                        // 统计返回结果
                        int totalSucceedRows = 0;
                        int totalFailedRows = 0;
                        for (BatchWriteRowResponse result : batchWriteRowResponseList) {
                            if (null == result) {
                                continue;
                            }
                            totalSucceedRows += result.getSucceedRows().size();
                            totalFailedRows += result.getFailedRows().size();
                        }

                        logger.info("Write to tablesore success, the current shard_id is:" + activeShard + ",the recordNum is:" + recordNum);
                        logger.info("Total succeed rows: " + totalSucceedRows);
                        logger.info("Total failed rows: " + totalFailedRows);
                    }
                } catch (SubscriptionOfflineException e) {
                    // 订阅下线，退出
                    bExit = true;
                    logger.error("SubscriptionOfflineException:", e);
                } catch (OffsetResetedException e) {
                    // 点位被重置，更新offset上下文
                    datahubClient.updateOffsetContext(offsetCtx);
                    cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
                    logger.error("Restart consume shard:" + activeShard + ", reset offset:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
                } catch (OffsetSessionChangedException e) {
                    // 其他consumer同时消费了该订阅下的相同shard，退出
                    bExit = true;
                    logger.error("OffsetSessionChangedException:", e);
                } catch (Exception e) {
//					bExit = true;
                    logger.error("",e);
                }
            }
        } catch (Exception e) {
            logger.error("",e);
        }
    }

    public long batchWriteRow(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records, List<BatchWriteRowResponse> batchWriteRowResponseList) {
        try {
            int recordNumCycle = 0;
            int batchRecordsCount = records.size();
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
            for (RecordEntry record : records) {
                offsetCtx.setOffset(record.getOffset());
                if (tableStoreConstructor(batchWriteRowRequest, record)) continue;
                recordNumCycle++;
                recordNum++;
                if (batchRecordsCount >= tablestoreWriteSize && recordNumCycle % tablestoreWriteSize == 0) {
                    batchWriteRowResponseList.add(doBatchWritRow(batchWriteRowRequest, recordNumCycle));
                    batchWriteRowRequest = new BatchWriteRowRequest();
                } else if (batchRecordsCount < tablestoreWriteSize && recordNumCycle == batchRecordsCount) {
                    batchWriteRowResponseList.add(doBatchWritRow(batchWriteRowRequest, recordNumCycle));
                }
            }
            if (batchRecordsCount > tablestoreWriteSize && batchRecordsCount % tablestoreWriteSize != 0) {
                batchWriteRowResponseList.add(doBatchWritRow(batchWriteRowRequest, recordNumCycle));
            }
        } catch (Exception e) {
            logger.error("BatchUpdate tablestore error:", e);
        } finally {
            //无论如何，必须提交点位
            datahubClient.commitOffset(offsetCtx);
        }
        return recordNum;
    }

    private BatchWriteRowResponse doBatchWritRow(BatchWriteRowRequest batchWriteRowRequest, int recordNumCycle) {
        if (recordNumCycle == 0) {// batchWriteRowRequest中没有数据，不提交
            return null;
        }
        BatchWriteRowResponse response = syncClient.batchWriteRow(batchWriteRowRequest);

        //		logger.info("是否全部成功:" + response.isAllSucceed());
        if (!response.isAllSucceed()) {
            for (RowResult rowResult : response.getFailedRows()) {
                logger.error("Failed row:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                logger.error("Failed reason:" + rowResult.getError());
            }
            /*
             * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
    		 * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
    		 */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
            BatchWriteRowResponse failedResponse = syncClient.batchWriteRow(retryRequest);
            if (!failedResponse.isAllSucceed()) {
                List<RowResult> listFailed = failedResponse.getFailedRows();
                for (RowResult rowResult : listFailed) {
                    logger.error(rowResult.getTableName() + ":" + rowResult.getRow().getPrimaryKey() + ":" + rowResult.getRow().getColumnsMap());
                }
            }
        }
        return response;
    }
}