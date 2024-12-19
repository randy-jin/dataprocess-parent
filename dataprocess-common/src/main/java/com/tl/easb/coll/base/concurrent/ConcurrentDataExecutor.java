package com.tl.easb.coll.base.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tl.easb.coll.base.test.TimeCountUtils;
import com.tl.easb.coll.base.utils.StackTraceUtils;
import com.tl.easb.coll.facade.IDataFetchCallback;

/**
 * 并发数据处理执行器
 *
 * @author JerryHuang
 * <p>
 * 2014-3-7 下午2:29:55
 */
public class ConcurrentDataExecutor<T> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 封装数据并发处理逻辑。使用固定并发线程池控制并发线程数
     *
     * @param fixedThreadNum    线程池大小
     * @param sleepMs           每个线程sleep时间（毫秒）
     * @param dataFetchCallback 数据获取程序的回调接口
     * @param dataHandler       数据处理程序
     * @return 被处理的数据总量。
     */
    public long execute(int fixedThreadNum, final long sleepMs,
                        final IDataFetchCallback<T> dataFetchCallback,
                        final IDataHandler<T> dataHandler) {
        final AtomicLong sum = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(fixedThreadNum);
        final CountDownLatch executeLatch = new CountDownLatch(
                dataFetchCallback.getCountdownLatchNum());

        Long btime = System.currentTimeMillis();
        final Semaphore prepareSem = new Semaphore(fixedThreadNum);
        long lastFromIdx = 0;

        while (!dataFetchCallback.isEnd()) {
            try {
                prepareSem.acquire();
            } catch (InterruptedException e) {
                logger.error(StackTraceUtils.getStackTrace(e));
            }
            final long fromSubtask = lastFromIdx;
            executor.execute(new Runnable() {
                public void run() {
                    T data = dataFetchCallback.fetchData(fromSubtask);
                    long dataNum = dataHandler.handleData(data);
                    sum.addAndGet(dataNum);
                    if (sleepMs > 0) {
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e) {
                            logger.error(StackTraceUtils.getStackTrace(e));
                        }
                    }
                    executeLatch.countDown();
                    prepareSem.release();
                }
            });
            lastFromIdx += dataFetchCallback.getFetchSize();
        }
        try {
            executeLatch.await();
        } catch (InterruptedException e) {
            logger.error(StackTraceUtils.getStackTrace(e));
        }

        long handledSum = sum.get();
        Long etime = System.currentTimeMillis();
        long usedTime = etime - btime;
        long countedSum = dataFetchCallback.getCountedNum();
        if (logger.isInfoEnabled()) {
            logger.info("counted sum:" + countedSum + ",handled sum:"
                    + handledSum);
        }
        System.out.println(TimeCountUtils.log(4, usedTime, handledSum));
        executor.shutdown();
        return handledSum;
    }

    /**
     * 封装数据并发处理逻辑。使用固定并发线程池控制并发线程数
     *
     * @param fixedThreadNum    线程池大小
     * @param dataFetchCallback 数据获取程序的回调接口
     * @param dataHandler       数据处理程序
     * @return 被处理的数据总量。
     */
    public long execute(int fixedThreadNum,
                        final IDataFetchCallback<T> dataFetchCallback,
                        final IDataHandler<T> dataHandler) {
        return execute(fixedThreadNum, -1, dataFetchCallback, dataHandler);
    }
}
