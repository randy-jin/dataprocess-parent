package com.tl.dataprocess.tablestore;

import java.util.concurrent.atomic.AtomicLong;


import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jinzhiqiang on 2020/6/23.
 */
public class SampleCallback implements TableStoreCallback<RowChange, ConsumedCapacity> {
	private static Logger logger = LoggerFactory.getLogger(SampleCallback.class);
	
    private AtomicLong succeedCount;
    private AtomicLong failedCount;

    public SampleCallback(AtomicLong succeedCount, AtomicLong failedCount) {
        this.succeedCount = succeedCount;
        this.failedCount = failedCount;
    }

    @Override
    public void onCompleted(RowChange req, ConsumedCapacity res) {
        succeedCount.incrementAndGet();
    }

    @Override
    public void onFailed(RowChange req, Exception ex) {
    	logger.error("Callback failed: ", ex);
        failedCount.incrementAndGet();
    }
}
