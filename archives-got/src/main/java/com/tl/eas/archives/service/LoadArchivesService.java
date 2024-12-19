package com.tl.eas.archives.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * @Author chendongwei
 * Date 2022/6/26 14:39
 * @Descrintion
 */
@Service
public class LoadArchivesService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String sql = "INSERT INTO r_tmnl_profile_all( `CHG_OBJ_ID`, `D_STATUS`, `OPT_TIME`, `CHG_MODE`, `SHARD_NO`)\n" +
            "SELECT terminal_id,'0',now(),'02',shard_no from R_TMNL_INFO LIMIT ?,?";

    public void loadAllArchives() {
        int start = 0;
        int step = 1000;
        do {
            int update = jdbcTemplate.update(sql, start, step);
            if (update == 0) {
                break;
            }
            start += step;
        } while (true);
    }

    public void loadArchivesByShardNo(String shardNo) {
    }
}
