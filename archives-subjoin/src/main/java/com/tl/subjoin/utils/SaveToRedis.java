package com.tl.subjoin.utils;

import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaveToRedis {

    private final static Logger logger = LoggerFactory.getLogger(SaveToRedis.class);

    public static void save(JdbcTemplate jdbcTemplate, IOperateBzData operateBzData, ArchivesObject archives) {
        String terminalId = archives.getTERMINAL_ID();
        String shardNo = archives.getSHARD_NO();

        List<String> saveList = new ArrayList<>();
        saveList.add(terminalId);
        saveList.add(shardNo);
        try {
            List<Map<String, Map>> dataList=SqlUtils.getList(jdbcTemplate,saveList);
            if(dataList==null ){
                return;
            }
            for (Map<String, Map> m:dataList) {
                operateBzData.saveHash(m);
            }
            logger.info("save end!"+saveList.toString());
        } catch (Exception e) {
            logger.error("error:", e);

        }
    }


}
