package com.tl.eas.archives.pgsql;

import com.tl.eas.archives.RdbArchivesChgParent;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by huangchunhuai on 2020/3/4.
 * <p>
 * 档案加载PostGreSQL实现类
 * <p>
 * 注:此类针对PostGreSQL数据库,请注意SQL语句的语法规则
 */
public class MsgArchivesChgPg extends RdbArchivesChgParent {

    private MsgArchivesChgPg(JdbcTemplate jdbcTemplate, RedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Override
    protected String getTmnlProfileSQL(int beforeSeconds) {
        String FIND_TMNL_PROFILE_SQL = " SELECT PROFILE_ID, CHG_OBJ_ID, SHARD_NO FROM " + tableName
                + " WHERE D_STATUS = '0' AND OPT_TIME < CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL  '" + beforeSeconds + " second '";
        return FIND_TMNL_PROFILE_SQL;
    }
}
