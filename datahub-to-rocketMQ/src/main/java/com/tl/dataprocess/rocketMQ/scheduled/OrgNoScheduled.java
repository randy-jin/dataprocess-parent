package com.tl.dataprocess.rocketMQ.scheduled;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import com.tl.dataprocess.rocketMQ.mapper.tmnl.TmnlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/8/22 16:33
 * @description ：加载orgNo的集合
 * @version: 1.0.0.0
 */
@Component
public class OrgNoScheduled {

    private static final Logger logger = LoggerFactory.getLogger(OrgNoScheduled.class);

    @Autowired
    private TmnlMapper tmnlMapper;

    private List<String> orgNoList = null;

//    @Scheduled(cron = "0 0 1,13 * * ?")
    @Scheduled(cron = "0 0 0/1 * * ?")
    @PostConstruct
    public void execute() {
        init();
    }

    /**
     * 初始化
     */
    public void init(){
        orgNoList = tmnlMapper.selectOrgNo();
    }

    /**
     * 获取list
     * @return
     */
    public List<String> getOrgNoList() {
        if(ObjectUtil.isNull(orgNoList)){
            logger.info("=======the List is Null=======");
            init();
        }
        return orgNoList;
    }

    /**
     * 判断是否在白名单
     * @param orgNo
     * @return
     */
    public Boolean isContains(String orgNo) {
        return getOrgNoList().contains(orgNo);
    }
}
