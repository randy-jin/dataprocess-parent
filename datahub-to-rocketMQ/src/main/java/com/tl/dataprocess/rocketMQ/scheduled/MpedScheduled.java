package com.tl.dataprocess.rocketMQ.scheduled;

import cn.hutool.core.util.ObjectUtil;
import com.tl.dataprocess.rocketMQ.mapper.mped.MpedMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/8/23 16:23
 * @description ：
 * @version: 1.0.0.0
 */
@Component
public class MpedScheduled {

    private static final Logger logger = LoggerFactory.getLogger(MpedScheduled.class);

    @Autowired
    private MpedMapper mpedMapper;

    private List<Long> mpedList = null;

    @Scheduled(cron = "0 0 0/1 * * ?")
    @PostConstruct
    public void execute() {
        init();
    }

    public void init(){
        mpedList = mpedMapper.selectMpedList();
    }

    /**
     * 获取list
     * @return
     */
    public List<Long> getMpedList() {
        if(ObjectUtil.isNull(mpedList)){
            logger.info("=======the Mped List is Null=======");
            init();
        }
        return mpedList;
    }

    /**
     * 判断是否在白名单
     * @param mpedId
     * @return
     */
    public Boolean isContains(Long mpedId) {
        return getMpedList().contains(mpedId);
    }
}
