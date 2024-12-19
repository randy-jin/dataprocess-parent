package com.tl.dataprocess.rocketMQ.writer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;
import com.tl.dataprocess.datahub.DataHubShardCache;
import com.tl.dataprocess.datahub.SingleSubscriptionAsyncExecutor;
import com.tl.dataprocess.rocketMQ.manager.RedisCacheManager;
import com.tl.dataprocess.rocketMQ.scheduled.MpedScheduled;
import com.tl.dataprocess.rocketMQ.scheduled.OrgNoScheduled;
import com.tl.dataprocess.rocketMQ.service.RocketMQService;
import com.tl.util.TaskThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.tl.dataprocess.rocketMQ.constant.WriterConstant.*;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/5 10:39
 * @description ：
 * @version: 1.0.0.0
 */
public class RocketMQWriter extends SingleSubscriptionAsyncExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RocketMQWriter.class);

    private RocketMQService rocketMQService;

    private OrgNoScheduled orgNoScheduled;

    private MpedScheduled mpedScheduled;

    private RedisCacheManager redisCacheManager;

    private String mqTopic;

    private String mqTag;

    private boolean filterFlag;

    public boolean isFilterFlag() {
        return filterFlag;
    }

    public void setFilterFlag(boolean filterFlag) {
        this.filterFlag = filterFlag;
    }

    public MpedScheduled getMpedScheduled() {
        return mpedScheduled;
    }

    public void setMpedScheduled(MpedScheduled mpedScheduled) {
        this.mpedScheduled = mpedScheduled;
    }

    public String getMqTopic() {
        return mqTopic;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public String getMqTag() {
        return mqTag;
    }

    public void setMqTag(String mqTag) {
        this.mqTag = mqTag;
    }

    public OrgNoScheduled getOrgNoScheduled() {
        return orgNoScheduled;
    }

    public void setOrgNoScheduled(OrgNoScheduled orgNoScheduled) {
        this.orgNoScheduled = orgNoScheduled;
    }

    public RocketMQService getRocketMQService() {
        return rocketMQService;
    }

    public void setRocketMQService(RocketMQService rocketMQService) {
        this.rocketMQService = rocketMQService;
    }


    public RedisCacheManager getRedisCacheManager() {
        return redisCacheManager;
    }

    public void setRedisCacheManager(RedisCacheManager redisCacheManager) {
        this.redisCacheManager = redisCacheManager;
    }

    @Override
    protected void init() {
        if (isRun == 0) {
            return;
        }
        logger.info("==========start RocketMQWriter init==========topic is ::"+datahubTopicName);
        List<String> activeShardList = DataHubShardCache.getActiveShard(datahubClient, datahubPojectName, datahubTopicName);
        executor = Executors.newFixedThreadPool(threadPoolSize == 0 ? activeShardList.size() : threadPoolSize, new TaskThreadPool(datahubTopicName));
        for (final String activeShard : activeShardList) {
            executor.execute(() -> start(activeShard));
        }
    }

    /**
     * 将订阅获取的数据实时写入下游存储介质(具体业务在子类中进行实现)
     *
     * @param offsetCtx 点位
     * @param recordNum 写入数量
     * @param records 数据
     * @return
     */
    @Override
    protected long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
        try {
            long id = IdUtil.getSnowflakeNextId();
            logger.info("==== batchNo: "+id + " records size " + records.size()+"=========");

            AtomicLong count = new AtomicLong(0);

            for (RecordEntry record : records){
                offsetCtx.setOffset(record.getOffset());
                Field[] fields = record.getFields();
                Map<String, Object> fieldValue = new HashMap<>();

                for (Field field : fields) {
                    String fieldName = field.getName();
                    fieldValue.put(fieldName.toUpperCase(), record.get(fieldName));
                }
                try {
                    if(DATA_EVENT_TOPIC.equals(datahubTopicName)){
                        fieldValue = getEventMap(fieldValue);
                    }
                    else if(DATA_VOL_CURVE_TOPIC.equals(datahubTopicName)){
                        fieldValue = getCurveMap(fieldValue,1);
                    }
                    else if(DATA_CUR_CURVE_TOPIC.equals(datahubTopicName)){
                        fieldValue = getCurveMap(fieldValue,2);
                    }
                    else if(DATA_POWER_CURVE_TOPIC.equals(datahubTopicName)){
                        fieldValue = getCurveMap(fieldValue,3);
                    }
                    else if(DATA_FACTOR_CURVE_TOPIC.equals(datahubTopicName)){
                        fieldValue = getCurveMap(fieldValue,4);
                    }
                    else if(DATA_READ_CURVE_TOPIC.equals(datahubTopicName)){
                        fieldValue = getCurveMap(fieldValue,5);
                    }
                    else if(DATA_USER_NO_POWER_TOPIC.equals(datahubTopicName)){
                        fieldValue = getUserEventMap(fieldValue);
                    }
                }catch (Exception e){
                    logger.error("======conversion MSG error::::", e);
                    continue;
                }
                String message = JSONUtil.toJsonStr(fieldValue,new JSONConfig().setIgnoreNullValue(false));
                logger.info("------have a record to write MQ -----"+message);
                //同步方法 快
//                Boolean flag = rocketMQService.sendCommonMessage(message);
//                if(flag){
//                    count.incrementAndGet();
//                }
                //异步方法 快 因为是异步，所以计数不一定准确
                rocketMQService.sendAsync(message,mqTopic,mqTag, new SendCallback() {
                    @Override
                    public void onSuccess(final SendResult sendResult) {
                        // 消息发送成功。
                        count.incrementAndGet();
                    }
                    @Override
                    public void onException(OnExceptionContext context) {
                        // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                        System.out.println("send message failed. topic=" + context.getTopic() + ", msgId=" + context.getMessageId());
                    }
                });

            }
            logger.info("====batchNo: "+ id + " batch updated count ::" + count);
            recordNum = count.addAndGet(recordNum);
        }catch (Exception e) {
            logger.error("===write to MQ error:", e);
        } finally {
            try {
                datahubClient.commitOffset(offsetCtx);
            } catch (Exception e) {
                throw new OffsetResetedException("提交偏移量时发生异常！");
            }
        }

        return recordNum;
    }

    /**
     * 用tmnlId去查询orgNo等
     * 停电事件转换
     * @param fieldValue 注意：key都是大写的
     */
    private Map<String, Object> getEventMap(Map<String, Object> fieldValue){
        Map<String, Object> build = new HashMap<>();

        build.put(TERMINAL_ID,fieldValue.get(TERMINAL_ID).toString());
        Map<String, Object> mapTB =  redisCacheManager.hmget(TB_KEY + fieldValue.get(TERMINAL_ID));
        if(MapUtil.isEmpty(mapTB)){
            throw new RuntimeException("====TERMINAL has error===="+fieldValue.get(TERMINAL_ID)+":::Tmnl map is Empty");
        }
        if(ObjectUtil.isNull(mapTB.get(ORG))){
            throw new RuntimeException("====OrgNo has error===="+fieldValue.get(TERMINAL_ID)+":::orgNo is null");
        }
        //FIXME 新加业务：判断白名单
        if(filterFlag){
            if(!orgNoScheduled.isContains(mapTB.get(ORG).toString())){
                throw new RuntimeException("====OrgNo has error===="+mapTB.get(ORG)+":::Not in the whitelist");
            }
        }
        build.put(ORG_NO,mapTB.get(ORG));

        String[] addr = mapTB.get(ADDR).toString().split("\\|");
        Map<String, Object> mapM = redisCacheManager.hmget(StrUtil.format(M_KEY,addr[0],addr[1]));
        if(MapUtil.isEmpty(mapM)){
            throw new RuntimeException("====TERMINAL has error===="+fieldValue.get(TERMINAL_ID)+":::Meter map is Empty");
        }

        String point = "";
        for (String key : mapM.keySet()) {
            //key
            if(StrUtil.startWith(key, "P")){
                point = StrUtil.removePrefix(key, "P");
                break;
            }
        }
        if(StrUtil.isBlank(point)){
            throw new RuntimeException("====TERMINAL has error===="+fieldValue.get(TERMINAL_ID)+":::Measurement point is Empty");
        }

        Object tgId = redisCacheManager.hget(StrUtil.format(P_KEY,addr[0],addr[1],point),TGID);
        if(ObjectUtil.isNull(tgId)){
            throw new RuntimeException("====TERMINAL has error===="+fieldValue.get(TERMINAL_ID)+":::tgId is null");
        }

        build.put(TG_ID,tgId);
        build.put(OUTAGE_BEGIN_TIME, DateUtil.format(new Date(getNarrow((long) fieldValue.get(EVENT_TIME))),"yyyy-MM-dd HH:mm:ss"));
        build.put(OUTAGE_FLAG,"0");
        build.put(OUTAGE_END_TIME,null);
        if(ObjectUtil.isNotNull(fieldValue.get(RESTART_TIME))){
            build.put(OUTAGE_END_TIME,DateUtil.format(new Date(getNarrow((long) fieldValue.get(RESTART_TIME))),"yyyy-MM-dd HH:mm:ss"));
            build.put(OUTAGE_FLAG,"1");
        }
        build.put(EVENT_TIME,DateUtil.format(new Date(getNarrow((long) fieldValue.get(EVENT_TIME))),"yyyy-MM-dd HH:mm:ss"));
        return build;
    }

    /**
     * 电压曲线转换
     * @param fieldValue
     * @param type 1:电压 2:电流 3:功率 4因数 5示值
     * @return
     */
    private Map<String, Object> getCurveMap(Map<String, Object> fieldValue,Integer type){
        if(filterFlag){
            if(!mpedScheduled.isContains((long)fieldValue.get(ID))){
                throw new RuntimeException("====Mped has error===="+fieldValue.get(ID)+":::Not in the whitelist");
            }
            if(!orgNoScheduled.isContains(fieldValue.get(ORG_NO).toString())){
                throw new RuntimeException("====OrgNo has error===="+fieldValue.get(ORG_NO)+":::Not in the whitelist");
            }
        }
        Map<String, Object> build = new HashMap<>();
        build.put(METER_ID,fieldValue.get(ID).toString());
        build.put(ORG_NO,fieldValue.get(ORG_NO));
        build.put(DATA_DATE,fieldValue.get(DATA_DATE));
        build.put(COL_TIME,DateUtil.format(new Date(getNarrow((long) fieldValue.get(INSERT_TIME))),"yyyyMMddHHmmss"));
        if(type == 1){
            build.put("U",fieldValue.get("U").toString());
            build.put(PHASE_FLAG,fieldValue.get(PHASE_FLAG).toString());
        }
        else if(type == 2){
            build.put("I",fieldValue.get("I").toString());
            build.put(PHASE_FLAG,fieldValue.get(PHASE_FLAG).toString());
        }
        else if(type == 3){
            build.put("P",fieldValue.get("P").toString());
            build.put(DATA_TYPE,fieldValue.get(DATA_TYPE).toString());
        }
        else if(type == 4){
            build.put("C",fieldValue.get("C").toString());
            build.put(PHASE_FLAG,fieldValue.get(PHASE_FLAG).toString());
        }
        else if(type == 5){
            build.put("R",fieldValue.get("R").toString());
            build.put(DATA_TYPE,fieldValue.get(DATA_TYPE).toString());
        }
        return build;
    }

    /**
     * 用户停电事件转换
     * @param fieldValue
     * @return
     */
    private Map<String, Object> getUserEventMap(Map<String, Object> fieldValue) {
        Map<String, Object> build = new HashMap<>();
        if(filterFlag){
            if(!orgNoScheduled.isContains(fieldValue.get(ORG_NO).toString())){
                throw new RuntimeException("====OrgNo has error===="+fieldValue.get(ORG_NO)+":::Not in the whitelist");
            }
        }
        build.put(ORG_NO,fieldValue.get(ORG_NO));
        build.put(METER_ID,fieldValue.get(METER_ID));
        build.put(OUTAGE_BEGIN_TIME, DateUtil.format(new Date(getNarrow((long) fieldValue.get(NO_POWER_SD))),"yyyy-MM-dd HH:mm:ss"));
        build.put(TG_ID,null);
        build.put(CONS_NO,null);
        build.put(OUTAGE_END_TIME,null);
        if(ObjectUtil.isNotNull(fieldValue.get(NO_POWER_ED))){
            build.put(OUTAGE_END_TIME,DateUtil.format(new Date(getNarrow((long) fieldValue.get(NO_POWER_ED))),"yyyy-MM-dd HH:mm:ss"));
        }
        build.put(OUTAGE_FLAG,fieldValue.get(EVENT_TYPE));
        build.put(EVENT_TIME,DateUtil.format(new Date(getNarrow((long) fieldValue.get(INPUT_TIME))),"yyyy-MM-dd HH:mm:ss"));
        return build;
    }

    /**
     * 将数据缩小1000
     * @param value
     * @return
     */
    private long getNarrow(long value){
        return value / 1000L;
    }
}
