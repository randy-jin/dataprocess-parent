package com.tl.eas.archives.drds;

import com.tl.eas.archives.RdbArchivesChgParent;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 档案加载DRDS实现类
 * <p>
 * 注:此类针对DRDS数据库,请注意SQL语句的语法规则
 */
public class MsgArchivesChgDrds extends RdbArchivesChgParent {

    public MsgArchivesChgDrds(JdbcTemplate jdbcTemplate, RedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Override
    protected String getTmnlProfileSQL(int beforeSeconds) {
        String FIND_TMNL_PROFILE_SQL = " SELECT PROFILE_ID, CHG_OBJ_ID, SHARD_NO FROM " + tableName
                + " WHERE D_STATUS = '0' AND OPT_TIME < DATE_SUB(SYSDATE(),INTERVAL " + beforeSeconds
                + " SECOND) ";
        return FIND_TMNL_PROFILE_SQL;
    }
}
