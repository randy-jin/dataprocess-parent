package com.tl.eas.archives;

import com.tl.eas.archives.drds.ScanArchivesChgDrdsAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by jinzhiqiang on 2021/8/18.
 * <p>
 * 根据r_tmnl_profile表中的变更记录,加载档案变更信息至Redis缓存
 */
public abstract class RdbArchivesChgParent {
    private static Logger log = LoggerFactory.getLogger(ScanArchivesChgDrdsAll.class);

    private String INTERVAL_TIME_KEY = "INTERVAL_TIME";
    private String INTERVAL_BEFORE_TIME_KEY = "INTERVAL_BEFORE_TIME";
    private int defaultIntervalTime = 300;// 秒，默认5分钟扫描一次
    private int defaultBeforeTime = 600;// 秒，默认获取10分钟之前的未变更记录

    protected JdbcTemplate jdbcTemplate;
    protected RedisTemplate redisTemplate;
    protected String queueName;
    protected String tableName;

    public void setDefaultBeforeTime(int defaultBeforeTime) {
        this.defaultBeforeTime = defaultBeforeTime;
    }

    public void setINTERVAL_TIME_KEY(String INTERVAL_TIME_KEY) {
        this.INTERVAL_TIME_KEY = INTERVAL_TIME_KEY;
    }

    public void setINTERVAL_BEFORE_TIME_KEY(String INTERVAL_BEFORE_TIME_KEY) {
        this.INTERVAL_BEFORE_TIME_KEY = INTERVAL_BEFORE_TIME_KEY;
    }

    public void setDefaultIntervalTime(int defaultIntervalTime) {
        this.defaultIntervalTime = defaultIntervalTime;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void init() {
        FutureTask<Void> task = new FutureTask<Void>(new Callable<Void>() {
            @Override
            public Void call() {
                try {
                    getChgArchives(); // 使用另一个线程来执行该方法，会避免占用Tomcat的启动时间
                } catch (Exception e) {
                    log.error("Loading archives failed:", e);
                }
                return null;
            }
        });
        new Thread(task).start();
    }

    protected void getChgArchives() {
        while (true) {
            try {
                Object beforeSecondsStr = redisTemplate.boundValueOps(INTERVAL_BEFORE_TIME_KEY).get();
                int beforeSeconds;
                if (null == beforeSecondsStr) {
                    beforeSeconds = defaultBeforeTime;
                    redisTemplate.boundValueOps(INTERVAL_BEFORE_TIME_KEY).set(String.valueOf(defaultBeforeTime));
                } else {
                    beforeSeconds = Integer.parseInt(String.valueOf(beforeSecondsStr));
                }
                String FIND_TMNL_PROFILE_SQL = getTmnlProfileSQL(beforeSeconds);
                List<Map<String, Object>> list = jdbcTemplate.queryForList(FIND_TMNL_PROFILE_SQL);
                if (null == list || list.size() == 0) {
                    intervalSleep();
                    continue;
                }
                log.info("Loading archives: " + list.size());
                cyclePushToQueue(list);
                intervalSleep();
            } catch (Exception e) {
                log.error("Loading archives exception: ", e);
            }
        }
    }

    protected abstract String getTmnlProfileSQL(int beforeSeconds);

    protected String getJson(String chgObjId, String shardNo) {
        StringBuffer sb = new StringBuffer();
        sb.append(" {                                   ");
        sb.append(" 	\"TERMINAL_ID\":\"" + chgObjId + "\",");
        sb.append(" 	\"SHARD_NO\":\"" + shardNo + "\",    ");
        sb.append(" 	\"APP_NO\":\"\"                  ");
        sb.append(" }                                   ");
        return sb.toString();
    }

    protected void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            log.error("Sleep error:", e);
        }
    }

    private void cyclePushToQueue(List<Map<String, Object>> list) {
        redisTemplate.setEnableTransactionSupport(true);
        redisTemplate.multi();
        for (int index = 0; index < list.size(); index++) {
            Map<String, Object> map = list.get(index);
            long profileId = (Long) map.get("PROFILE_ID");
            String chgObjId = (String) map.get("CHG_OBJ_ID");
            String shardNo = (String) map.get("SHARD_NO");
            if (chgObjId == null || "null".equals(chgObjId) || shardNo == null || "null".equals(shardNo)) {
                continue;
            }

            String json = getJson(chgObjId, shardNo);
            redisTemplate.boundListOps(queueName).leftPush(json);
        }
        List<Object> retList = redisTemplate.exec();
        log.info("批次量:[" + list.size() + "] 写入量:[" + retList.size() + "]");
    }

    private void intervalSleep() {
        Object sleepSecondsStr = redisTemplate.boundValueOps(INTERVAL_TIME_KEY).get();
        int sleepSeconds;
        if (null == sleepSecondsStr) {
            sleepSeconds = defaultIntervalTime;
            redisTemplate.boundValueOps(INTERVAL_TIME_KEY).set(String.valueOf(defaultIntervalTime));
        } else {
            sleepSeconds = Integer.parseInt(String.valueOf(sleepSecondsStr));
        }
        sleep(sleepSeconds);
    }
}