package com.tl.easb.task.handle.subtask;

import com.alibaba.fastjson.JSON;
import com.ls.athena.callmessage.multi.batch.TmnlMessageSet;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.task.thread.TaskThreadPool;
import com.tl.easb.utils.SplitPackage;
import com.tl.easb.utils.SpringUtils;
import com.tl.easb.utils.multi.pojo.PipeObject;
import com.tl.easb.utils.multi.service.ToRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by jinzhiqiang on 2021/7/14.
 */
public class TransferToPrepositiveMachine extends ToRedis {
    private static Logger log = LoggerFactory.getLogger(TransferToPrepositiveMachine.class);

//    @Autowired
    private static RedisTemplate redisClusterTemplate;


    private static int TMNL_BATCH_QUEUE_COUNT = ParamConstants.TMNL_BATCH_QUEUE_COUNT;
    private static int TMNL_BATCH_SLEEP = ParamConstants.TMNL_BATCH_SLEEP;
    private static int TMNL_BATCH_THREAD_COUNT = ParamConstants.TMNL_BATCH_THREAD_COUNT;
    public static int QZ_TC_NUM = ParamConstants.QZ_TC_NUM;
    public static int QZ_YC_NUM = ParamConstants.QZ_YC_NUM;
    private static BlockingQueue<TmnlMessageSet> TERMINAL_PACKAGE_LOCAL_QUEUE = new LinkedBlockingQueue<TmnlMessageSet>(TMNL_BATCH_QUEUE_COUNT);
    private static final String TERMINAL_PACKAGE_REMOTE_QUEUE = "TERMINAL_PACKAGE_REMOTE_QUEUE";

    private static final String Q_PRE_TMNL_QUEUE = "Q_PRE_TMNL_QUEUE";
    private static final String Q_THR_TMNL_QUEUE = "Q_TC_TMNL_";
    private static final String TMNL_LOCK_PRE = "TMNL:LOCK:";// 终端锁前缀

    static {
        redisClusterTemplate = (RedisTemplate) SpringUtils.getBean("redisClusterTemplate");
    }

    /**
     * 发送数据给前置机队列
     *
     * @param sendData
     * @param taskConfig
     * @throws Exception
     */
    public static void send(Map<String, TmnlMessageSet> sendData, AutoTaskConfig taskConfig)
            throws Exception {
        if (sendData == null || sendData.size() == 0) {
            return;
        }
        // 透抄对象
        List<PipeObject> thrList = new ArrayList<PipeObject>();
        PipeObject thrPo = null;

        Iterator<Map.Entry<String, TmnlMessageSet>> iter = sendData.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, TmnlMessageSet> entry = (Map.Entry<String, TmnlMessageSet>) iter.next();
            TmnlMessageSet termData = entry.getValue();
            termData.setPassword("000000");
            termData.setPriority(100);//统一指令优先级

            if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {//透抄任务
                long index = Math.abs(termData.getAreaCode().hashCode() + Long.valueOf(termData.getTmnlAddr())) % QZ_TC_NUM;
                String queueName = Q_THR_TMNL_QUEUE + index;
                thrPo = new PipeObject();
                thrPo.setQueue(queueName);
                thrPo.setObject(termData);
                thrList.add(thrPo);
            } else {//预抄任务
                List<TmnlMessageSet> tmnlMessageSetPackages = SplitPackage.splitList(termData);
                for (TmnlMessageSet tmnlMessagePackage : tmnlMessageSetPackages) {
                    try {
                        String terminalId = tmnlMessagePackage.getTmnlId();
                        // 获取Redis终端锁,true 进行下发;false 放回队列
                        if (redisClusterTemplate.boundValueOps(TMNL_LOCK_PRE + terminalId).setIfAbsent(String.valueOf(System.currentTimeMillis()), TMNL_BATCH_SLEEP, TimeUnit.MILLISECONDS)) {
                            String queueName=Q_PRE_TMNL_QUEUE;
                            if(QZ_YC_NUM>1){
                                long ycIndex = Math.abs(tmnlMessagePackage.getAreaCode().hashCode() + Long.valueOf(tmnlMessagePackage.getTmnlAddr())) % QZ_YC_NUM;
                                queueName=queueName+"_"+ycIndex;
                            }
                            sendToRedis(queueName, tmnlMessagePackage);// 发送给接口服务
                        } else {
                            if (!TERMINAL_PACKAGE_LOCAL_QUEUE.offer(tmnlMessagePackage)) {
                                sendToRedisByJson(TERMINAL_PACKAGE_REMOTE_QUEUE, tmnlMessagePackage);// 暂存到分包暂存Redis队列
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
//			PromethuesAllDayUtil.sendTaskIDAllDay.labels(taskConfig.getAutoTaskId()).inc();
            }
        }
        if (!thrList.isEmpty()) {//透抄任务
            sendToRedis(thrList);
        }
    }

    static {
        // 处理因为间隔时间不足2秒而无法下发的包
        ExecutorService executorService = Executors.newFixedThreadPool(TMNL_BATCH_THREAD_COUNT, new TaskThreadPool("TMNL_BATCH_THREAD"));
        for (int i = 0; i < TMNL_BATCH_THREAD_COUNT; i++) {
            executorService.execute(
                    new Runnable() {
                        public void run() {
                            while (true) {
                                try {
                                    TmnlMessageSet tmnlMessagePackage = TERMINAL_PACKAGE_LOCAL_QUEUE.take();
                                    String terminalId = tmnlMessagePackage.getTmnlId();
                                    if (redisClusterTemplate.boundValueOps(TMNL_LOCK_PRE + terminalId).setIfAbsent(String.valueOf(System.currentTimeMillis()), TMNL_BATCH_SLEEP, TimeUnit.MILLISECONDS)) {
                                        String queueNameThread=Q_PRE_TMNL_QUEUE;
                                        if(QZ_YC_NUM>1){
                                            long ycIndexThread = Math.abs(tmnlMessagePackage.getAreaCode().hashCode() + Long.valueOf(tmnlMessagePackage.getTmnlAddr())) % QZ_YC_NUM;
                                            queueNameThread=queueNameThread+"_"+ycIndexThread;
                                        }
                                        sendToRedis(queueNameThread, tmnlMessagePackage);// 发送给接口服务
                                    } else {
                                        if (!TERMINAL_PACKAGE_LOCAL_QUEUE.offer(tmnlMessagePackage)) {
                                            sendToRedisByJson(TERMINAL_PACKAGE_REMOTE_QUEUE, tmnlMessagePackage);// 暂存到分包暂存Redis队列
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("循环处理下行队列包发生异常:", e);
                                }
                            }
                        }
                    });
        }
        executorService.shutdown();

        // 从Redis队列中获取包,放入本地队列
        ExecutorService executorPopService = Executors.newFixedThreadPool(jedisPools.size(), new TaskThreadPool("POP_FROM_QUEUE"));
        for (int i = 0; i < jedisPools.size(); i++) {
            final int j = i;
            executorPopService.execute(
                    new Runnable() {
                        public void run() {
                            while (true) {
                                Jedis jedis = null;
                                try {
                                    jedis = getJedis(j);
                                    //TODO 更换序列化原因注释
                                    List<String> packages = jedis.blpop(1000, TERMINAL_PACKAGE_REMOTE_QUEUE);
                                    if (packages == null) {
                                        continue;
                                    }
                                    for (String packageJSON : packages) {
                                        if(TERMINAL_PACKAGE_REMOTE_QUEUE.equals(packageJSON)){
                                            continue;
                                        }
                                        TmnlMessageSet tmnlMessageSet = JSON.parseObject(packageJSON, TmnlMessageSet.class);
                                        TERMINAL_PACKAGE_LOCAL_QUEUE.put(tmnlMessageSet);

                                    }
                                } catch (Exception e) {
                                    log.error("循环处理下行队列包发生异常:", e);
                                } finally {
                                    if (jedis != null) {
                                        jedis.close();
                                    }
                                }
                            }

                        }
                    });
        }
        executorPopService.shutdown();

        Timer timer = new Timer();
        timer.schedule(new QueuePrintTimer(timer), new Date(), 5000);
    }

    static class QueuePrintTimer extends TimerTask {
        Timer timer = null;

        public QueuePrintTimer(Timer timer) {
            this.timer = timer;
        }

        public void run() {
            try {
                int size = TERMINAL_PACKAGE_LOCAL_QUEUE.size();
                if (size > 0) {
                    log.info("TMNL_BATCH_QUEUE_COUNT's size is [" + TERMINAL_PACKAGE_LOCAL_QUEUE.size() + "']");
                }
            } catch (Exception e) {
                log.error("TMNL_BATCH_QUEUE_COUNT is null");
            }

        }
    }

}