package com.tl.easb.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tl.easb.coll.api.ZcDataManager;
import com.tl.easb.coll.api.ZcKeyDefine;
import com.tl.easb.coll.api.ZcMonitorManager;
import com.tl.easb.coll.api.ZcProcedureManager;
import com.tl.easb.coll.api.ZcTaskManager;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.handle.rautotaskhistory.RautotaskHistoryHandle;
import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.utils.template.RedisOperation;
import com.tl.util.StringUtil;
import com.tl.util.TaskThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 缓存存取工具类
 *
 * @author JinZhiQiang
 * @date 2014年3月10日
 */
public class CacheUtil {
    private static Logger log = LoggerFactory.getLogger(CacheUtil.class);

    private static final String KEY_HEADER = "T$";
    private static final String SEPARATOR = "#";

//	private static RedisTemplate redisTemplate = RedisTemplateInstance.redisTemplate;
//	private static JedisSentinelPool jedisSentinelPool = RedisTemplateInstance.jedisSentinelPool;

    private CacheUtil() {
    }

    /**
     * 清除指定缓存中的所有信息
     */
    @Deprecated
    public static void clearAll() {
        ZcDataManager.clearAll();
    }

    /**
     * 初始化一级任务-二级任务关系数据集
     *
     * @param autoTaskId
     * @param subtasks
     * @return
     */
    public static long initTask2Subtask(String autoTaskId, Set<String> subtasks, String collectDate) {
        long ret = 0;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId, collectDate);
        ret = ZcDataManager.initTask2Subtask(wrapAutoTaskId, subtasks);
        return ret;
    }

    /**
     * 1、删除子任务与测点信息数据集 2、删除任务与子任务数据集 被方法clearCpData代替
     *
     * @param autotaskId
     */
    @Deprecated
    public static void cleanTaskCache(String autotaskId, String dataDate) {
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dataDate);
        ZcDataManager.clearSubtaskCp(wrapAutoTaskId, ParamConstants.REDIS_DELETE_SUBTASK_STEP_LENGTH);
        ZcDataManager.clearTask2subtask(wrapAutoTaskId);
    }

    /**
     * 检索缓存任务队列是否有未处理信息
     *
     * @param autotaskId
     * @return
     */
    public static boolean isTaskAllDistributed(String autotaskId, String colDataDate) {
        boolean finished = false;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, colDataDate);
        finished = ZcMonitorManager.isTaskAllDistributed(wrapAutoTaskId);
        return finished;
    }

    /**
     * 判断任务是否执行完成
     *
     * @param autotaskId
     * @param type       01 预 02 透
     * @return
     */
    public static boolean checkTaskComplete(String autotaskId, String colDataDate, int itemTotal, String type) {
        int timeLatch = 0;
        boolean finished = false;
        if (itemTotal > ParamConstants.TASK_FINISH_TOTAL) {//应采总数大于1000
            if ("01".equals(type)) {
                timeLatch = ParamConstants.TASK_FINISH_TIMEOUT;//预抄任务超时时间
            } else if ("02".equals(type)) {
                timeLatch = ParamConstants.TASK_TC_FINISH_TIMEOUT;//透传任务超时时间
            }
        } else {
            if ("01".equals(type)) {
                timeLatch = ParamConstants.TASK_FINISH_TIMEOUT + 10000;//预抄任务超时时间
            } else if ("02".equals(type)) {
                timeLatch = ParamConstants.TASK_TC_FINISH_TIMEOUT + 10000;//透传任务超时时间
            }
        }

        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, colDataDate);
        // 判断缓存是否读取完成
        finished = ZcTaskManager.checkTaskComplete(wrapAutoTaskId, timeLatch);
        return finished;
    }

    /**
     * 过程数据清理 入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN] 供前入库服务调用
     * 根据测点信息，找到子任务信息，再根据子任务信息删除其对应的【采集信息数据集】
     *
     * @param cps
     */
    public static void process(final List<Object[]> cps) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                Set<String> newCps = new HashSet<String>(cps.size());
                try {
                    translate(cps, newCps);
                    if (newCps.size() > 0) {
                        List<String> newCpsList = new ArrayList<String>(newCps);
                        ZcProcedureManager.process(newCpsList, ParamConstants.REDIS_DELETE_MP_STEP_LENGTH,
                                System.currentTimeMillis());
                        newCps.clear();
                        newCpsList.clear();
                    }
                } catch (Exception e) {
                    log.error("过程数据清理发生异常：", e);
                }
            }
        });
    }

    /**
     * 采集点信息转换 入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN]
     * 出参格式：行政区码_终端地址_测量点序号_采集日期（yyyymmddhhmmss）_数据项标示
     *
     * @param cps
     * @return
     */
    private static void translate(List<Object[]> cps, Set<String> newCps) {
        for (int i = 0; i < cps.size(); i++) {
            Object[] cp = cps.get(i);
            String _tempCp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp[0], cp[1], cp[2], cp[3], cp[4]);
            cp = null;
            newCps.add(_tempCp);
        }
        cps.clear();
    }

    /**
     * 初始化任务相关缓存
     *
     * @param autoTaskId
     * @param taskStatus
     * @param dataDate
     */
    public static void initTaskAbout(final String autoTaskId, final String taskStatus, final String dataDate) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId, dataDate);
                Transaction tx = jedis.multi();
                tx.hset(ZcKeyDefine.KEY_TASK2STATUS, autoTaskId, taskStatus);
                tx.set(ZcKeyDefine.KPREF_TASKUPDTIME + wrapAutoTaskId, String.valueOf(System.currentTimeMillis()));
                tx.hset(ZcKeyDefine.KEY_TASK2COUNTER, wrapAutoTaskId, String.valueOf(0));
                tx.set(ZcKeyDefine.KPREF_TASKOFFSET + wrapAutoTaskId, String.valueOf(0));
                tx.exec();
            }
        });
    }

    /**
     * 增加任务计数器
     *
     * @param autoTaskId
     * @param size
     */
    public static void incrTaskCounter(String autoTaskId, long size) {
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId, null);
        ZcDataManager.incrTaskCounter(wrapAutoTaskId, size);
    }

    /**
     * 根据任务清理相关缓存信息 删除信息如下： 1、删除任务计数器 2、删除任务更新时间 3、清除任务状态缓存
     *
     * @param autotaskId
     */
    public static void clearTaskAbout(String autotaskId, String dataDate) {
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dataDate);
        // 删除任务计数器
        ZcDataManager.removeTaskCounter(wrapAutoTaskId);
        log.info("clear TASK2COUNTER ok!");
        // 删除任务更新时间
        ZcDataManager.removeTaskUpdTime(wrapAutoTaskId);
        log.info("clear TKUPDTIME ok!");
        // 清除任务状态缓存
        ZcDataManager.removeTaskStatus(autotaskId);
        log.info("clear TaskStatus ok!");
        // 删除子任务偏移量
        ZcDataManager.removeTaskOffset(wrapAutoTaskId);
        log.info("clear TaskOffset ok!");
    }

    /**
     * 该方法清除以下信息： 1、清除测点-二级任务对应关系 2、二级任务-测点对应关系 3、主任务与二级任务对应关系
     *
     * @param autotaskId
     */
    public static void clearTaskCpData(String autotaskId, String dataDate) {
        log.info("TASK[" + autotaskId + "_" + dataDate + "]CLEAR TASK AND CP INFO START.......");
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dataDate);
        ZcDataManager.clearCpData(wrapAutoTaskId, ParamConstants.REDIS_DELETE_SUBTASK_STEP_LENGTH, -1);
        log.info("TASK[" + autotaskId + "_" + dataDate + "]CLEAR TASK AND CP INFO END.......");
    }

    /**
     * 存在性能问题
     * 根据自动任务编号检索redis队列中所有的主任务对子任务键值，然后删除该任务下所有日期的测点信息 该方法清除以下信息：
     * 1、清除测点-二级任务对应关系 2、二级任务-测点对应关系 3、主任务与二级任务对应关系
     *
     * @param autotaskId
     */
    public static void clearTaskAllCpData(final String autotaskId) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                log.info(ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*" + " CLEAR TASK AND CP ALL START.......");

                // 游标初始值为0
                String cursor = ScanParams.SCAN_POINTER_START;
                String key = ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*";
                ScanParams scanParams = new ScanParams();
                scanParams.match(key);// 匹配以 ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*" 为前缀的 key
                scanParams.count(1000);
                while (true) {
                    //使用scan命令获取500条数据，使用cursor游标记录位置，下次循环使用
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getCursor();// 返回0 说明遍历完成
                    List<String> list = scanResult.getResult();
//                    long t1 = System.currentTimeMillis();
                    for (int m = 0; m < list.size(); m++) {
                        String task2Subtask = list.get(m);
                        String dataDate = task2Subtask.split(MainTaskDefine.SEPARATOR)[1];
                        clearTaskCpData(autotaskId, dataDate);
                    }
//                    long t2 = System.currentTimeMillis();
//                    log.info("删除" + list.size() + "条数据，耗时: " + (t2-t1) + "毫秒,cursor:" + cursor);
                    if ("0".equals(cursor)) {
                        break;
                    }
                }

//                Set<String> set = jedis.keys(ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*");
//                Iterator<String> iterator = set.iterator();
//                while (iterator.hasNext()) {
//                    String task2Subtask = iterator.next();
//                    String dataDate = task2Subtask.split(MainTaskDefine.SEPARATOR)[1];
//                    clearTaskCpData(autotaskId, dataDate);
//                }
                log.info(ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*" + " CLEAR TASK AND CP ALL END.......");
            }
        });

    }

    /**
     * 功能：传入任务ID，从缓存中获取任务状态，并将其解析成数组，数组内容如下 [0]:开始时间 [1]:采集日期 [2]:优先级 [3]:补采次数
     * [4]:采集信息生成方式 [5]:执行标识 [6]:应采总数
     *
     * @param autotaskId
     * @return
     */
    public static String[] getTaskStatus(String autotaskId) {
        String taskStatus = ZcDataManager.getTaskStatus(autotaskId);
        String[] strs = null;
        if (null == taskStatus) {
            return null;
        }
        strs = StringUtil.explodeString(taskStatus, "_");
        return strs;
    }

    /**
     * 功能：清除任务状态缓存
     *
     * @param autotaskId
     * @return
     */
    @Deprecated
    public static long removeTaskStatus(String autotaskId) {
        return ZcDataManager.removeTaskStatus(autotaskId);
    }

    /**
     * 重置任务状态为“停止”
     *
     * @param autotaskId
     */
    public synchronized static void setTaskPause(String autotaskId) {
        String taskStatus = ZcDataManager.getTaskStatus(autotaskId);
        if (null == taskStatus) {
            return;
        }
        String[] strs = StringUtil.explodeString(taskStatus, "_");

//        //应采数
//        int allCount=Integer.valueOf(strs[SubTaskDefine.STATUS_PARAMS_COLL_TOTAL_NUM]);
//        String key=autotaskId+"_"+strs[SubTaskDefine.STATUS_PARAMS_COLL_DATA_DATE];
//        //计数器
//        int counterCount=getCounterCount(key,allCount);
//        //计数器小于应采数时return 不设置停止标示
//        if(counterCount<allCount){
//            return;
//        }
        if (strs[5].equals(SubTaskDefine.STATUS_EXEC_FLAG_STOP) || SubTaskDefine.STATUS_EXEC_FLAG_STOP == strs[5]) {
            return;
        }
        strs[5] = SubTaskDefine.STATUS_EXEC_FLAG_STOP;// 0表示任务停止、1表示任务执行中
        StringBuffer status = new StringBuffer();
        status.append(strs[0]).append("_").append(strs[1]).append("_").append(strs[2]).append("_").append(strs[3])
                .append("_").append(strs[4]).append("_").append(strs[5]).append("_").append(strs[6]);
        ZcDataManager.setTaskStatus(autotaskId, status.toString());
        log.info("TASK[" + autotaskId + "]STATUS HAS BEEN PAUSED:" + status.toString());
    }

    /**
     * 获取偏移量，如果没有偏移量把应采数设置成偏移量
     * @param key
     * @param allCount
     * @return
     */
    public static int getCounterCount(final String key,final int allCount) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {
            public Integer doWithJedis(Jedis jedis) {

                String counterCount = jedis.hget(ZcKeyDefine.KEY_TASK2COUNTER,key);
                int count=0;
                if (null == counterCount) {
                    jedis.hset(ZcKeyDefine.KEY_TASK2COUNTER,key,String.valueOf(allCount));
                } else {
                    count = Integer.valueOf(counterCount);
                }
                return count;
            }
        });
    }

    /**
     * 重置任务状态为“执行中”
     *
     * @param autotaskId
     */
    public synchronized static void setTaskExec(String autotaskId) {
        String taskStatus = ZcDataManager.getTaskStatus(autotaskId);
        if (null == taskStatus) {
            return;
        }
        String[] strs = StringUtil.explodeString(taskStatus, "_");
        if (strs[5].equals(SubTaskDefine.STATUS_EXEC_FLAG_EXECING) || SubTaskDefine.STATUS_EXEC_FLAG_EXECING == strs[5]) {
            return;
        }
        strs[5] = SubTaskDefine.STATUS_EXEC_FLAG_EXECING;// 0表示任务停止、1表示任务执行中
        StringBuffer status = new StringBuffer();
        status.append(strs[0]).append("_").append(strs[1]).append("_").append(strs[2]).append("_").append(strs[3])
                .append("_").append(strs[4]).append("_").append(strs[5]).append("_").append(strs[6]);
        ZcDataManager.setTaskStatus(autotaskId, status.toString());
        log.info("TASK[" + autotaskId + "]STATUS HAS BEEN Execute:" + status.toString());
    }

    /**
     * 获取任务计数器值
     *
     * @param autotaskId 任务号
     * @return
     */
    public static BigDecimal getTaskCounter(String autotaskId) {
        long taskCounter = 0;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, null);
        taskCounter = ZcDataManager.getTaskCounter(wrapAutoTaskId);
        return new BigDecimal(taskCounter);
    }

    /**
     * 设置任务偏移量
     *
     * @param autotaskId
     * @param offset
     */
    public static void setCurrentTaskOffset(String autotaskId, String dataDate, long offset) {
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dataDate);
        if (null == wrapAutoTaskId || wrapAutoTaskId.contains("null")) {
            log.error("Get Task's [" + autotaskId + "] dataDate is null,please check");
        }
        ZcDataManager.setCurrentTaskOffset(wrapAutoTaskId, offset);
    }

    /**
     * 获取剩余测量点数步长参数，默认为5000
     * 注：可在redis中获取到以TASK_LEFT_MP_COUNTER_STEP:*开头的任务自行调整参数大小
     *
     * @param autotaskId
     * @return
     */
    public static int getLeftMpCounterStep(final String autotaskId) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {
            public Integer doWithJedis(Jedis jedis) {
                int step = 5000;
                String stepStr = jedis.get(ZcKeyDefine.KEY_LEFTMPCOUNTER_STEP + autotaskId);
                if (null == stepStr) {
                    jedis.set(ZcKeyDefine.KEY_LEFTMPCOUNTER_STEP + autotaskId, String.valueOf(step));
                } else {
                    step = Integer.valueOf(stepStr);
                }
                return step;
            }
        });
    }

    /**
     * 获取任务剩余测点数(存在严重性能问题)
     *
     * @param autotaskId 任务号
     * @return
     */
    public static BigDecimal getLeftMpCounter(final String autotaskId, final String dateStr) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<BigDecimal>() {
            public BigDecimal doWithJedis(Jedis jedis) {
                long startTime = System.currentTimeMillis();
                long size = 0;
                if (dateStr != null && dateStr.length() != 14) {
                    log.error("When Getting Left MpCounter,the param dateStr is(autotaskId=" + autotaskId + "):"
                            + dateStr);
                    return new BigDecimal(size);
                }
                String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dateStr);
                String task2subtaskKey = ZcKeyDefine.KPREF_TASK2SUBTASK + wrapAutoTaskId;
                long subtaskCount = jedis.llen(task2subtaskKey);
                long start = 0;
                int step = getLeftMpCounterStep(autotaskId);
                while (start < subtaskCount) {
                    long end = start + step;
                    List<String> subtaskList = jedis.lrange(task2subtaskKey, start, end);
                    Pipeline pipeline = jedis.pipelined();
                    for (String str : subtaskList) {
                        pipeline.scard(ZcKeyDefine.KPREF_SUBTASK2CP + str);
                    }
                    List<Object> list = pipeline.syncAndReturnAll();
                    for (Object o : list) {
                        size += Long.valueOf(o.toString());
                    }
                    list.clear();
                    start = end + 1;
                }
                log.info("The Getting left mp counter[" + size + "] time escaped:"
                        + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                return new BigDecimal(size);
            }
        });
    }

    private static ExecutorService writeToHistoryThreadPool = null;

    static {
        writeToHistoryThreadPool = Executors.newSingleThreadExecutor(new TaskThreadPool("WriteToHistoryThreadPool"));
    }

    // 对外接口启动线程
    public static void startThread(AutoTaskConfig taskConfig, String dataDate) {
        final String _dataDate = dataDate;
        final int _itemsScope = taskConfig.getItemsScope();
        final String _autotaskId = taskConfig.getAutoTaskId();
        final String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(_autotaskId, dataDate);
        log.info("删除任务【" + _autotaskId + "】数据日期为【" + dataDate + "】的表【R_AUTOTASK_HISTORY】中的历史记录");
        dataDate = dataDate.substring(0, 4) + "/" + dataDate.substring(4, 6) + "/" + dataDate.substring(6, 8);
        RautotaskHistoryHandle.del(_autotaskId, dataDate);
        writeToHistoryThreadPool.execute(new Runnable() {
            public void run() {
                ZcDataManager.setCurrentTaskOffset(wrapAutoTaskId, 0);
                CacheUtil.writeToHistory(_autotaskId, _dataDate, wrapAutoTaskId);
                if (DateUtil.beforeYesterday(_dataDate)
                        || _itemsScope == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ) {
                    log.info("任务【" + _autotaskId + "】采集历史数据或当日曲线完成，清除测点信息缓存");
                    CacheUtil.clearTaskCpData(_autotaskId, _dataDate);
                }
                // 删除子任务偏移量
                clearTaskAbout(_autotaskId, _dataDate);
            }
        });
    }

    /**
     * 将缓存中采集失败的测点信息入库
     *
     * @param autotaskId
     */
    private static void writeToHistory(String autotaskId, String dataDate, String wrapAutoTaskId) {
        log.info("任务【" + autotaskId + "】将失败信息写入历史表开始");
        int step = ParamConstants.TASK_FAIL_PERSISTENCE_STEP;
        while (true) {
            List<String> mpList = new ArrayList<String>(step);
            List<String> subTaskIds = ZcTaskManager.drawSubtasks(wrapAutoTaskId,
                    ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
            if (null == subTaskIds || subTaskIds.size() == 0) {
                break;
            }
            Set<String> cpSet = ZcTaskManager.getCps(subTaskIds);
            Iterator<String> iterator = cpSet.iterator();
            while (iterator.hasNext()) {
                mpList.add(iterator.next());
                if (mpList.size() >= step) {
                    RautotaskHistoryHandle.batchInsert(autotaskId, mpList);
                    mpList.clear();
                }
            }
            if (mpList.size() > 0) {
                RautotaskHistoryHandle.batchInsert(autotaskId, mpList);
                mpList.clear();
            }
        }
        log.info("任务【" + autotaskId + "】将失败信息写入历史表结束");
    }

    /**
     * 包装任务编号，作为redis键值使用
     *
     * @param autotaskId  任务编号
     * @param dataDateStr 采集数据日期字符串，格式如：20140407000000
     * @return
     */
    public static String wrapAutoTaskId(String autotaskId, String dataDateStr) {
        if (null == autotaskId) {
            return null;
        }
        if (null == dataDateStr) {
            String[] taskStatus = getTaskStatus(autotaskId);
            if (null != taskStatus) {
                dataDateStr = taskStatus[1];
            } else {
                log.error("Autotask [" + autotaskId + "]'s dataDateStr is null");
            }
        }
        return autotaskId + "_" + dataDateStr;
    }

    /**
     * 存在性能问题
     * 初始化任务执行测量点缓存
     *
     * @param list
     * @param autotaskId
     */
    public static void initTaskScope(final List<String[]> list, final String autotaskId) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                Pipeline pipeline = jedis.pipelined();
                Map<String, String> map = new HashMap<String, String>();
                String areaCode = null;
                String terminalAddr = null;
                for (String[] arr : list) {
                    String mpedIndex = arr[2];
                    String mpedId = arr[3];
                    String pn = "P" + mpedIndex;
                    if (areaCode == null || terminalAddr == null) {
                        areaCode = arr[0];
                        terminalAddr = arr[1];
                        map.put(pn, mpedId);
                    } else if (areaCode != arr[0] || terminalAddr != arr[1]) {
                        map.clear();
                        areaCode = arr[0];
                        terminalAddr = arr[1];
                    }
                    map.put(pn, mpedId);
                    String key = KEY_HEADER + autotaskId + SEPARATOR + areaCode + SEPARATOR + terminalAddr;
                    pipeline.hmset(key, map);
                }
                pipeline.sync();
            }
        });
    }

    /**
     * 根据任务编号、行政区划码、终端地址获取对应的终端下要采集的测量点序号
     *
     * @param autotaskId
     * @param areaCode
     * @param terminalAddr
     * @return
     */
    public static Map<String, String> getTaskScope(final String autotaskId, final String areaCode,
                                                   final String terminalAddr) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Map<String, String>>() {
            public Map<String, String> doWithJedis(Jedis jedis) {
                String key = KEY_HEADER + autotaskId + SEPARATOR + areaCode + SEPARATOR + terminalAddr;
                return jedis.hgetAll(key);
            }
        });
    }

    /**
     * 根据任务编号、行政区划码、终端地址删除任务临时缓存(存在效率问题)
     *
     * @param autotaskId
     * @return
     */
    public static long delTaskScope(final String autotaskId) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
            public Long doWithJedis(Jedis jedis) {
                String key = KEY_HEADER + autotaskId + SEPARATOR + "*";

                // 游标初始值为0
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams();
                scanParams.match(key);// 匹配以 ZcKeyDefine.KPREF_TASK2SUBTASK + autotaskId + "*" 为前缀的 key
                scanParams.count(1000);
                long count = 0;
                while (true) {
                    //使用scan命令获取500条数据，使用cursor游标记录位置，下次循环使用
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getCursor();// 返回0 说明遍历完成
                    List<String> list = scanResult.getResult();
                    long t1 = System.currentTimeMillis();
                    if (list.size() > 0) {
                        for (String delKey : list) {
                            count = count + jedis.del(delKey);
                        }
                    }
                    long t2 = System.currentTimeMillis();
                    log.info("删除" + list.size() + "条数据，耗时: " + (t2 - t1) + "毫秒,cursor:" + cursor);
                    if ("0".equals(cursor)) {
                        break;
                    }
                }

//                Set<String> refDelSet;
//                refDelSet = jedis.keys(key);
//                long count = 0;
//                if (refDelSet.size() > 0) {
//                    for (String delKey : refDelSet) {
//                        count = count + jedis.del(delKey);
//                    }
//                }

                log.info("+++++++++++delete temp cache count：" + count);
                return count;
            }
        });
    }

    static final int retryCount = 5;// 最多重复获取5次

    /**
     * 获取jedis对象
     *
     * @return
     */
    public static Jedis getJedis() {
        Jedis jedis = null;
        JedisSentinelPool jedisSentinelPool = null;
        int curCount = 0;
        while (curCount < retryCount) {
            try {
                jedisSentinelPool = (JedisSentinelPool) SpringUtils.getBean("jedisSentinelPool");
                jedis = jedisSentinelPool.getResource();
                return jedis;
            } catch (Exception e) {
                log.info("JedisPool activeNum:" + jedisSentinelPool.getNumActive() + ",idleNum:"
                        + jedisSentinelPool.getNumIdle() + ",waiterNum:" + jedisSentinelPool.getNumWaiters()
                        + ",MeanBorrowWaitTime:" + jedisSentinelPool.getMeanBorrowWaitTimeMillis()
                        + ",MaxBorrowWaitTime:" + jedisSentinelPool.getMaxBorrowWaitTimeMillis() + ",MaxBorrowWaitTime:"
                        + jedisSentinelPool.getMaxBorrowWaitTimeMillis());
                log.error("Get Jedis Object fail,Current Run Count:[" + curCount + "]", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.error("",e1);
                }
            }
            curCount++;
        }
        return jedis;
    }

    /**
     * 回收jedis到pool中
     *
     * @param jedis
     */
    public static void returnJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                log.error("Close Jedis Exception:", e);
            }

        }
    }

    /**
     * 清除redis实例范围内所有数据。谨慎使用！
     * <p>
     * redis客户端实例
     */
    public static void clearDB() {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                jedis.flushDB();
            }
        });
    }

}
