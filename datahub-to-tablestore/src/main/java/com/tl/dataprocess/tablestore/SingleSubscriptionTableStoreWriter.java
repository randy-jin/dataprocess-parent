package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.datahub.DataHubShardCache;
import com.tl.util.TaskThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 从DataHub的topic中订阅数据并以“异步”方式批量写入tablestore
 *
 * @author jinzhiqiang
 */
public class SingleSubscriptionTableStoreWriter extends SubscriptionExecutor {
    private static Logger logger = LoggerFactory.getLogger(SingleSubscriptionTableStoreWriter.class);

    private AsyncClientInterface asyncClient;
    private WriterConfig config;

    private AtomicLong succeedCount = new AtomicLong();
    private AtomicLong failedCount = new AtomicLong();

    private TableStoreCallback<RowChange, ConsumedCapacity> callback;
    private TableStoreWriter tablestoreWriter;

    public void setAsyncClient(AsyncClientInterface asyncClient) {
        this.asyncClient = asyncClient;
    }

    @Override
    protected void init() {
        // 初始化
        config = new WriterConfig();
        config.setMaxBatchSize(16 * 1024 * 1024); // 配置一次批量导入请求的大小限制，默认是4MB
        config.setMaxColumnsCount(128); // 配置一行的列数的上限，默认128列
        config.setBufferSize(1024 * 64); // 配置内存中最多缓冲的数据行数，默认1024行，必须是2的指数倍
        config.setMaxBatchRowsCount(200); // 配置一次批量导入的行数上限，默认100
        config.setConcurrency(100); // 配置最大并发数，默认10
        config.setMaxAttrColumnSize(4 * 1024 * 1024); // 配置属性列的值大小上限，默认是2MB
        config.setMaxPKColumnSize(1024); // 配置主键列的值大小上限，默认1KB
        config.setFlushInterval(10000); // 配置缓冲区flush的时间间隔，默认10s

        writeExecutor = Executors.newFixedThreadPool(5, new TaskThreadPool("callback_thread"));
        callback = new SampleCallback(succeedCount, failedCount);
        tablestoreWriter = new DefaultTableStoreWriter(asyncClient, tablestoreTableName, config, callback, writeExecutor);

        if (isRun == 0) {
            return;
        }
        List<String> activeShardList = DataHubShardCache.getActiveShard(datahubClient, datahubPojectName, datahubTopicName);
        executor = Executors.newFixedThreadPool(threadPoolSize == 0 ? activeShardList.size() : threadPoolSize, new TaskThreadPool(datahubTopicName));
        for (final String activeShard : activeShardList) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        start(activeShard);
                    } catch (Exception e) {
//						logger.info("readExecutor: " +e);
                        executor.execute(this);
                    }

                }
            });
        }
    }

    @Override
    protected long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
        try {
            for (RecordEntry record : records) {
                offsetCtx.setOffset(record.getOffset());
                if (tableStoreConstructor(tablestoreWriter, record)) continue;
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
