package com.tl.dataprocess.rocketMQ.configuration;// 创建代理

import com.aliyun.datahub.DatahubClient;
import com.tl.dataprocess.rocketMQ.manager.RedisCacheManager;
import com.tl.dataprocess.rocketMQ.scheduled.MpedScheduled;
import com.tl.dataprocess.rocketMQ.scheduled.OrgNoScheduled;
import com.tl.dataprocess.rocketMQ.service.RocketMQService;
import com.tl.dataprocess.rocketMQ.writer.RocketMQWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import static com.tl.dataprocess.rocketMQ.constant.WriterConstant.*;

@Service
@Configuration
public class RocketMQWriterConfig {

    @Resource
    private RedisCacheManager redisCacheManager;

    @Resource
    private DatahubClient datahubClient;

    @Resource(name = "aliRocketMQService")
    RocketMQService rocketMQService;

    @Resource
    private OrgNoScheduled orgNoScheduled;

    @Resource
    private MpedScheduled mpedScheduled;

    @Value("${datahub.default.project}")
    private String project;

    /**停电事件datahub的topic*/
    @Value("${datahub.default.topic.blackoutEvent}")
    private String blackoutEventTopic;

    @Value("${datahub.default.subId.blackoutEvent}")
    private String blackoutEventSubId;

    @Value("${datahub.default.topic.volCurve}")
    private String volCurveTopic;

    @Value("${datahub.default.subId.volCurve}")
    private String volCurveSubId;

    @Value("${datahub.default.topic.curCurve}")
    private String curCurveTopic;

    @Value("${datahub.default.subId.curCurve}")
    private String curCurveSubId;

    @Value("${datahub.default.topic.powerCurve}")
    private String powerCurveTopic;

    @Value("${datahub.default.subId.powerCurve}")
    private String powerCurveSubId;

    @Value("${datahub.default.topic.factorCurve}")
    private String factorCurveTopic;

    @Value("${datahub.default.subId.factorCurve}")
    private String factorCurveSubId;

    @Value("${datahub.default.topic.readCurve}")
    private String readCurveTopic;

    @Value("${datahub.default.subId.readCurve}")
    private String readCurveSubId;

    @Value("${datahub.default.topic.userPowerCut}")
    private String userPowerCutTopic;

    @Value("${datahub.default.subId.userPowerCut}")
    private String userPowerCutSubId;

    @Value("${filterFlag}")
    private boolean filterFlag;

    @Bean
    public RocketMQWriter blackoutEventMQWriter() {

        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(blackoutEventTopic);
        writer.setDatahubSubId(blackoutEventSubId);

        writer.setMqTopic(EVENT_TOPIC);
        writer.setMqTag(EVENT_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter volCurveMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(volCurveTopic);
        writer.setDatahubSubId(volCurveSubId);

        writer.setMqTopic(VOL_CURVE_TOPIC);
        writer.setMqTag(VOL_CURVE_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter curCurveMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(curCurveTopic);
        writer.setDatahubSubId(curCurveSubId);

        writer.setMqTopic(CUR_CURVE_TOPIC);
        writer.setMqTag(CUR_CURVE_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter powerCurveMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(powerCurveTopic);
        writer.setDatahubSubId(powerCurveSubId);

        writer.setMqTopic(POWER_CURVE_TOPIC);
        writer.setMqTag(POWER_CURVE_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter factorCurveMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(factorCurveTopic);
        writer.setDatahubSubId(factorCurveSubId);

        writer.setMqTopic(FACTOR_CURVE_TOPIC);
        writer.setMqTag(FACTOR_CURVE_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter readCurveMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(readCurveTopic);
        writer.setDatahubSubId(readCurveSubId);

        writer.setMqTopic(READ_CURVE_TOPIC);
        writer.setMqTag(READ_CURVE_TAG);
        return writer;
    }

    @Bean
    public RocketMQWriter userPowerMQWriter() {
        RocketMQWriter writer = buildCommonWriter();
        writer.setDatahubTopicName(userPowerCutTopic);
        writer.setDatahubSubId(userPowerCutSubId);

        writer.setMqTopic(USER_POWER_CUT_TOPIC);
        writer.setMqTag(USER_POWER_CUT_TAG);
        return writer;
    }

    public RocketMQWriter buildCommonWriter(){
        RocketMQWriter writer = new RocketMQWriter();
        //如果设置为0，则根据active shard数作为线程数，否则根据设定的threadPoolSize为线程数
        writer.setThreadPoolSize(THREAD_POOL_SIZE);
        writer.setDatahubClient(datahubClient);
        writer.setDatahubPojectName(project);
        writer.setIsRun(RUN_FLAG);

        writer.setFilterFlag(filterFlag);
        writer.setMpedScheduled(mpedScheduled);
        writer.setRocketMQService(rocketMQService);
        writer.setOrgNoScheduled(orgNoScheduled);
        return writer;
    }
}

