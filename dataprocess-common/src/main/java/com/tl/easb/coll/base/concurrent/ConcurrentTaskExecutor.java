package com.tl.easb.coll.base.concurrent;

import com.tl.easb.coll.base.test.TimeCountUtils;
import com.tl.easb.coll.base.utils.StackTraceUtils;
import com.tl.easb.coll.facade.ITaskHandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 并发任务处理执行器
 *
 * @author JerryHuang
 * <p>
 * 2014-3-7 下午2:29:55
 */
public class ConcurrentTaskExecutor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 封装任务并发处理逻辑。使用固定并发线程池控制并发线程数
     *
     * @param fixedThreadNum     线程池大小
     * @param sleepMs            每个线程sleep时间（毫秒）
     * @param taskHandleCallback 任务执行程序的回调接口
     * @param taskHandler        任务执行程序
     * @return 被处理的数据总量。
     */
    public long execute(int fixedThreadNum, final long sleepMs,
                        final ITaskHandleCallback taskHandleCallback, final ITaskHandler taskHandler) {
        final AtomicLong sum = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(fixedThreadNum);
        final CountDownLatch executeLatch = new CountDownLatch(
                taskHandleCallback.getCountdownLatchNum());

        Long btime = System.currentTimeMillis();
        final Semaphore prepareSem = new Semaphore(fixedThreadNum);

        while (!taskHandleCallback.isEnd()) {
            try {
                prepareSem.acquire();
            } catch (InterruptedException e) {
                logger.error(StackTraceUtils.getStackTrace(e));
            }
            executor.execute(new Runnable() {
                public void run() {
                    long handledNum = taskHandler.handle();
                    sum.addAndGet(handledNum);
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
        }
        try {
            executeLatch.await();
        } catch (InterruptedException e) {
            logger.error(StackTraceUtils.getStackTrace(e));
        }

        long handledSum = sum.get();
        Long etime = System.currentTimeMillis();
        long usedTime = etime - btime;
        long countedSum = taskHandleCallback.getCountedNum();
        if (logger.isInfoEnabled()) {
            logger.info("counted sum:" + countedSum + ",handled sum:"
                    + handledSum);
        }
        System.out.println(TimeCountUtils.log(4, usedTime, handledSum));
        executor.shutdown();
        return handledSum;
    }

    /**
     * 封装任务并发处理逻辑。使用固定并发线程池控制并发线程数
     *
     * @param fixedThreadNum     线程池大小
     * @param taskHandleCallback 任务执行程序的回调接口
     * @param taskHandler        任务执行程序
     * @return 被处理的数据总量。
     */
    public long execute(int fixedThreadNum,
                        final ITaskHandleCallback taskHandleCallback, final ITaskHandler taskHandler, final Map<String, Object> taskArgs) {
        return execute(fixedThreadNum, -1, taskHandleCallback, taskHandler);
    }
}
