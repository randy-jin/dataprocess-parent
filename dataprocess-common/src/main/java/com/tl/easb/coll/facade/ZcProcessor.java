package com.tl.easb.coll.facade;

import com.tl.easb.coll.api.ZcProcedureManager;
import com.tl.easb.coll.base.concurrent.ConcurrentDataExecutor;
import com.tl.easb.coll.base.concurrent.IDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZcProcessor {
    private Logger logger = LoggerFactory.getLogger(ZcProcessor.class);

    public static long processConcurrently(final JedisPool jedisPool, String task,
                                           final IDataFetchCallback<List<String>> callback, int threadNum, long sleepInterval) {
        Map<String, Object> callbackArgs = new HashMap<String, Object>(1);
        callbackArgs.put("task", task);
        callback.init(callbackArgs);

        ConcurrentDataExecutor<List<String>> executor = new ConcurrentDataExecutor<List<String>>();

        return executor.execute(threadNum, sleepInterval, callback, new IDataHandler<List<String>>() {
            public long handleData(List<String> data) {
                long recordNum = 0;
                recordNum = ZcProcedureManager.process(data, 3000, System.currentTimeMillis());
                // System.out.println("recordnum:"+recordNum);
                return recordNum;
            }

        });
    }
}
