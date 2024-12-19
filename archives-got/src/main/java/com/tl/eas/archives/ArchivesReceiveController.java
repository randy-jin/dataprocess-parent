package com.tl.eas.archives;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import com.tl.eas.archives.service.LoadArchivesService;
import org.future.hand.HandClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 档案变更请求接收服务，提供给接口或者前台应用
 *
 * @author jinzhiqiang
 */
@Configuration
@EnableApolloConfig
@RestController
@Component
@SuppressWarnings("all")
public class ArchivesReceiveController {
    private static Logger logger = LoggerFactory.getLogger(ArchivesReceiveController.class);

    @Autowired
    public RedisTemplate redisSingleTemplate;

    @Value("${queueNameOne:apollo}")
    private String QUEUE_NAME = "Q_CHG_ARCHIVES";

    @Autowired
    private LoadArchivesService loadArchivesService;

    private static ThreadLocal<Long> threadLocal = new ThreadLocal<>();

    /**
     * 传入的JSON格式如下
     * <p>
     * {
     * \"TERMINAL_ID\":\"" + chgObjId + "\",
     * \"SHARD_NO\":\"" + shardNo + "\",
     * \"APP_NO\":\"\"
     * }
     *
     * @param archivesObject
     * @return
     */
    @RequestMapping("/archivesReciever/{terminalId}/{shardNo}/{appNo}")
    public String recieve(@PathVariable String terminalId, @PathVariable String shardNo, @PathVariable String appNo) {
        logger.info("Recieve change request:{} shardNo:{} appNo:{}", terminalId, shardNo, appNo);
        try {
            ArchivesObject archivesObject = new ArchivesObject();
            archivesObject.setTERMINAL_ID(terminalId);
            archivesObject.setSHARD_NO(shardNo);
            archivesObject.setAPP_NO(appNo);
            String send = JSON.toJSONString(archivesObject);
            HandClient.ts("AR").sendData(send);
        } catch (Exception e) {
            logger.error("接收异常：terminalId:{},shardNo:{},appNo:{}", terminalId, shardNo, appNo, e);
            return e.toString();
        }

        return "1";
    }


    @RequestMapping(value = "/archivesReciever", method = RequestMethod.POST)
    public String recieveBatch(@RequestBody String archiveJson) {
        logger.info("Recieve Batch request:{}", archiveJson);
        try {
            BatchTmnlData batchTmnlData = JSON.parseObject(archiveJson, BatchTmnlData.class);
            String appNo = batchTmnlData.getAppNo();
            String shardNo = batchTmnlData.getShardNo();
            List<String> batchTmnlInfo = batchTmnlData.getBatchTmnlInfo();
            List<String> archivesObjects = new ArrayList<>();
            for (String terminalId : batchTmnlInfo) {
                ArchivesObject archivesObject = new ArchivesObject();
                archivesObject.setTERMINAL_ID(terminalId);
                archivesObject.setSHARD_NO(shardNo);
                archivesObject.setAPP_NO(appNo);
                String jsonString = JSON.toJSONString(archivesObject);
                archivesObjects.add(jsonString);
            }
            if (!archivesObjects.isEmpty()) {
                HandClient.ts("AR").sendDataBatch(archivesObjects);
            }
        } catch (Exception e) {
            logger.error("批量接收异常：archiveJson:{}", archiveJson, e);
            return e.toString();
        }

        return "1";
    }

    @RequestMapping("/loadArchives")
    public String loadArchives() {
        logger.info("Recieve change request: reloadAll");
        try {
            Long lastCall = threadLocal.get();
            if (lastCall == null || System.currentTimeMillis() - lastCall > 10 * 60 * 1000) {
                loadArchivesService.loadAllArchives();
                threadLocal.set(System.currentTimeMillis());
            }
        } catch (Exception e) {
            return e.toString();
        }

        return "1";
    }

}