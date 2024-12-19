package com.tl.dataprocess.kafka;

import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.datahub.SingleSubscriptionAsyncExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@EnableScheduling
public class TimeToReadDRDS  {

    private JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    /* 定义三个map用来接收过滤字段的值 */
    public static  Map<String, Object> TMNL_MAP =new HashMap<>();
    public static  Map<String, Object> MPED_MAP =new HashMap<>();
    public static  Map<String, Object> METER_MAP =new HashMap<>();

//    public static Map<String, Object> getTerminalId() {
//        return terminalId;
//    }
//
//    public static Map<String, Object> getMpedId() {
//        return mpedId;
//    }
//
//    public static Map<String, Object> getMeterId() {
//        return meterId;
//    }

    /**
     * 读取数据库
     */
    protected void readValue() {

        final String fieldSQL = "SELECT a.TERMINAL_ID as terminal_id, b.MPED_ID as mped_id, b.METER_ID as meter_id  FROM r_tmnl_info a, r_mped b WHERE a.SHARD_NO = b.SHARD_NO  AND a.SHARD_NO = '41404'  AND a.TERMINAL_ID = b.TERMINAL_ID  AND a.ASSET_NO IN ( '4130009000000012260745','4130009500000000358312', '4130009500000000358329', '4130009500000000358336', '4130009500000000358343', '4130009500000000358350', '4130009500000000358367', '4130009500000000358374', '4130009500000000358381', '4130009500000000358398', '4130009500000000358404', '4130009500000000358411', '4130009500000000358428', '4130009500000000358435', '4130009500000000358442', '4130009500000000358459', '4130009500000000358466', '4130009500000000358473', '4130009500000000358480', '4130009500000000358497', '4130009500000000358503', '4130009500000000358510', '4130009500000000358527', '4130009500000000358534', '4130009500000000358541', '4130009500000000358558', '4130009500000000358565', '4130009500000000358572', '4130009500000000358589', '4130009500000000358596', '4130009500000000358602', '4130009500000000358619', '4130009500000000358626', '4130009500000000358633', '4130009500000000358640', '4130009500000000358657', '4130009500000000358664', '4130009500000000358671', '4130009500000000358688', '4130009500000000358695', '4130009500000000358701', '4130009500000000358718', '4130009500000000358725', '4130009500000000358732', '4130009500000000358749', '4130009500000000358756', '4130009500000000358763', '4130009500000000358770', '4130009500000000358787', '4130009500000000358794', '4130009500000000358800', '4130009500000000358817', '4130009500000000358824', '4130009500000000358831', '4130009500000000358848', '4130009500000000358855', '4130009500000000358862', '4130009500000000358879', '4130009500000000358886', '4130009500000000358893', '4130009500000000358909', '4130009500000000358916', '4130009500000000358923', '4130009500000000358930', '4130009500000000358947', '4130009500000000358954', '4130009500000000358961', '4130009500000000358978', '4130009500000000358985', '4130009500000000358992', '4130009500000000359005', '4130009500000000359012', '4130009500000000359029', '4130009500000000359036', '4130009500000000359043', '4130009500000000359050', '4130009500000000359067', '4130009500000000359074', '4130009500000000359081', '4130009500000000359098', '4130009500000000359104', '4130009500000000359111', '4130009500000000359128', '4130009500000000359135', '4130009500000000359142', '4130009500000000359159', '4130009500000000359166', '4130009500000000359173', '4130009500000000359180', '4130009500000000359197', '4130009500000000359203', '4130009500000000359210', '4130009500000000359227', '4130009500000000359234', '4130009500000000359241', '4130009500000000359258', '4130009500000000359265', '4130009500000000359272', '4130009500000000359289', '4130009500000000359296', '4130009500000000359302'  )";
        List<Map<String,Object>> result =new CopyOnWriteArrayList<>(jdbcTemplate.queryForList(fieldSQL));
        //测试环境中的SQL
//        final String fieldSQL = "select TERMINAL_ID,MPED_ID,METER_ID from r_mped limit 10";
//        List<Map<String,Object>> result=jdbcTemplate.queryForList(fieldSQL);
        if(result==null||result.size()<1){
            return;
        }
        for (Map<String,Object> resultMap:result ) {
            for (String key:resultMap.keySet()) {
                Object mapValue=resultMap.get(key);
                if (mapValue==null)continue;;
                switch (key.toUpperCase()){
                    case "TERMINAL_ID":
                        TMNL_MAP.put(mapValue.toString(),mapValue);
                        break;
                    case "MPED_ID":
                        MPED_MAP.put(mapValue.toString(),mapValue);
                        break;
                    case "METER_ID":
                        METER_MAP.put(mapValue.toString(),mapValue);
                        break;
                }
            }
        }

    }

    /**
     * 定时执行SQL语句，并赋值给map
     */
    @Scheduled(cron = "00 55 23 ? * *")
    public void readResult(){
        readValue();
    }
}
