package com.tl.eas.archives.drds;

import com.tl.eas.archives.RdbArchivesChgParent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * 根据r_tmnl_profile_all表中的变更记录,全量加载档案变更信息至Redis缓存
 * <p>
 * 注:此类针对DRDS数据库,请注意SQL语句的语法规则
 */
public class ScanArchivesChgDrdsAll extends RdbArchivesChgParent {

    private static Logger log = LoggerFactory.getLogger(ScanArchivesChgDrdsAll.class);

    private String ALL_INTERVAL_TIME_KEY = "ALL_INTERVAL_TIME";
    private int defaultIntervalTime = 5;// 秒，默认5秒钟扫描一次

    public void setALL_INTERVAL_TIME_KEY(String ALL_INTERVAL_TIME_KEY) {
        this.ALL_INTERVAL_TIME_KEY = ALL_INTERVAL_TIME_KEY;
    }

    public void setDefaultIntervalTime(int defaultIntervalTime) {
        this.defaultIntervalTime = defaultIntervalTime;
    }

    private ScanArchivesChgDrdsAll(JdbcTemplate jdbcTemplate, RedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Override
    protected String getTmnlProfileSQL(int beforeSeconds) {
        String FIND_TMNL_PROFILE_SQL = " SELECT PROFILE_ID, CHG_OBJ_ID, SHARD_NO FROM " + tableName
                + " WHERE D_STATUS = '0' limit 1000 ";
        return FIND_TMNL_PROFILE_SQL;
    }

    @Override
    protected void getChgArchives() {
        String UPDATE_TMNL_PROFILE_STATUS_SQL = " UPDATE " + tableName + " SET D_STATUS = '1' WHERE PROFILE_ID in ";
        String FIND_TMNL_PROFILE_SQL = getTmnlProfileSQL(0);
        while (true) {
            try {
                List<Map<String, Object>> list = jdbcTemplate.queryForList(FIND_TMNL_PROFILE_SQL);
                if (null == list || list.size() == 0) {
                    Object sleepSecondsStr = redisTemplate.boundValueOps(ALL_INTERVAL_TIME_KEY).get();
                    int sleepSeconds = 0;
                    if (null == sleepSecondsStr) {
                        sleepSeconds = defaultIntervalTime;
                        redisTemplate.boundValueOps(ALL_INTERVAL_TIME_KEY).set(String.valueOf(defaultIntervalTime));
                    } else {
                        sleepSeconds = Integer.parseInt(String.valueOf(sleepSecondsStr));
                    }
                    sleep(sleepSeconds);
                    continue;
                }
                log.info("loading archives: " + list.size());
                StringBuilder profileIds = new StringBuilder();
                redisTemplate.setEnableTransactionSupport(true);
                redisTemplate.multi();
                for (int index = 0; index < list.size(); index++) {
                    Map<String, Object> map = list.get(index);
                    long profileId = (Long) map.get("PROFILE_ID");
                    String chgObjId = (String) map.get("CHG_OBJ_ID");
                    String shardNo = (String) map.get("SHARD_NO");

                    if (index == 0) {
                        profileIds.append("(");
                        profileIds.append(profileId);
                    } else {
                        profileIds.append(",");
                        profileIds.append(profileId);
                    }

                    String json = getJson(String.valueOf(chgObjId), shardNo);
                    redisTemplate.boundListOps(queueName + "_" + shardNo).leftPush(json);
                }
                List<Object> retList = redisTemplate.exec();
                log.info("批次量:[" + list.size() + "] 写入量:[" + retList.size() + "]");
                profileIds.append(")");
                String updtSQL = UPDATE_TMNL_PROFILE_STATUS_SQL + profileIds.toString();
                if (log.isInfoEnabled()) {
                    log.info("Update r_tmnl_profile,SQL is:" + updtSQL);
                }
                jdbcTemplate.execute(updtSQL);
            } catch (Exception e) {
                log.error("Loading ALL archives exception: ", e);
            }
        }
    }

}