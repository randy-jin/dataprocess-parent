package com.tl.dataprocess.rocketMQ.writer;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/5 14:17
 * @description ：
 * @version: 1.0.0.0
 */
@Configuration
public class StartWriterServer implements ApplicationRunner {

    @Resource(name = "blackoutEventMQWriter")
    private RocketMQWriter blackoutEventMQWriter;

    @Resource(name = "volCurveMQWriter")
    private RocketMQWriter volCurveMQWriter;

    @Resource(name = "curCurveMQWriter")
    private RocketMQWriter curCurveMQWriter;

    @Resource(name = "powerCurveMQWriter")
    private RocketMQWriter powerCurveMQWriter;

    @Resource(name = "factorCurveMQWriter")
    private RocketMQWriter factorCurveMQWriter;

    @Resource(name = "readCurveMQWriter")
    private RocketMQWriter readCurveMQWriter;

    @Resource(name = "userPowerMQWriter")
    private RocketMQWriter userPowerMQWriter;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        blackoutEventMQWriter.init();
        volCurveMQWriter.init();
        curCurveMQWriter.init();
        powerCurveMQWriter.init();
        factorCurveMQWriter.init();
        readCurveMQWriter.init();
        userPowerMQWriter.init();
    }
}
