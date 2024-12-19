package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
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
import java.util.concurrent.Future;

/**
 * 从DataHub的topic中订阅数据并以“异步”方式批量写入tablestore
 * <p>
 * 优点:同步获取DataHub数据,并采用异步方式提交至TableStore,相比"同步"提交方式,性能有很大提升
 * 缺点:如果业务层面无法规避单个shard内的重复数据,那么该"异步"提交方式会报提交冲突异常,甚至造成数据丢失
 * <p>
 * 已过期,请采用同目录下的com.tl.dataprocess.tablestore.TableStoreBatchWriter
 *
 * @author jinzhiqiang
 */
@Deprecated
public class SingleSubscriptionAsyncExecutor extends SubscriptionExecutor {
    private static Logger logger = LoggerFactory.getLogger(SingleSubscriptionAsyncExecutor.class);

    private AsyncClientInterface asyncClient;

    public void setAsyncClient(AsyncClientInterface asyncClient) {
        this.asyncClient = asyncClient;
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
                    GetRecordsResult recordResult = datahubClient.getRecords(datahubPojectName, datahubTopicName, activeShard, cursor, 1000, topicResult.getRecordSchema());
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
                        List<Future<BatchWriteRowResponse>> futures = new ArrayList<Future<BatchWriteRowResponse>>();
                        recordNum = batchWriteRow(offsetCtx, recordNum, records, futures);
                        cursor = recordResult.getNextCursor();

                        // 等待结果返回
                        List<BatchWriteRowResponse> results = new ArrayList<BatchWriteRowResponse>();
                        for (Future<BatchWriteRowResponse> future : futures) {
                            try {
                                BatchWriteRowResponse result = future.get(); // 同步等待结果返回
                                results.add(result);
                            } catch (TableStoreException e) {
                                e.printStackTrace();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // 统计返回结果
                        int totalSucceedRows = 0;
                        int totalFailedRows = 0;
                        for (BatchWriteRowResponse result : results) {
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
                    try {
                        datahubClient.commitOffset(offsetCtx);
                    } catch (Exception ie) {
                        logger.error("提交偏移量时发生异常:", e);
                    }
//					bExit = true;
                    logger.error("Other Exception:", e);
                }
            }
        } catch (Exception e) {
            logger.error("",e);
        }
    }

    private long batchWriteRow(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records, List<Future<BatchWriteRowResponse>> futures) {
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
                    Future<BatchWriteRowResponse> result = asyncClient.batchWriteRow(batchWriteRowRequest, null);
                    futures.add(result);
//					datahubClient.commitOffset(offsetCtx);
                    batchWriteRowRequest = new BatchWriteRowRequest();
                } else if (batchRecordsCount < tablestoreWriteSize && recordNumCycle == batchRecordsCount) {
                    Future<BatchWriteRowResponse> result = asyncClient.batchWriteRow(batchWriteRowRequest, null);
                    futures.add(result);
//					datahubClient.commitOffset(offsetCtx);
                }
                //			logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
            }
            if (batchRecordsCount > tablestoreWriteSize && batchRecordsCount % tablestoreWriteSize != 0) {
                Future<BatchWriteRowResponse> result = asyncClient.batchWriteRow(batchWriteRowRequest, null);
                futures.add(result);
//				datahubClient.commitOffset(offsetCtx);
            }
        } catch (Exception e) {
            logger.error("BatchUpdate tablestore error:", e);
        } finally {
            //无论如何，必须提交点位
            datahubClient.commitOffset(offsetCtx);
        }
        return recordNum;
    }

}
