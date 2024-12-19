package com.tl.subjoin.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlUtils {
    private final static Logger logger = LoggerFactory.getLogger(SqlUtils.class);
    private static final String TG_ABOUT_PARAM = "TG:ABOUT:PARAM:";

    private static final String GET_TG_ABOUT_PARAM_SQL = getTgAboutParamSQL();


    public static List<Map<String, Map>> getList(JdbcTemplate jdbcTemplate, List<String> list) throws Exception {
        SqlRowSet rs = jdbcTemplate.queryForRowSet(GET_TG_ABOUT_PARAM_SQL, new String[]{list.get(0), list.get(1)});
        logger.info("sql:"+GET_TG_ABOUT_PARAM_SQL+" param :"+list.toString());
        List<Map<String, Map>> hashList=new ArrayList<>();
        while (rs.next()) {
            Map<String, Map> m=new HashMap<>();
            String mapKey= TG_ABOUT_PARAM+rs.getString("MPED_ID");
            Map<String,String> valueMap=new HashMap<>();
            valueMap.put("TMNL_ID",rs.getString("TERMINAL_ID"));
            valueMap.put("ORG_NO",rs.getString("ORG_NO"));
            valueMap.put("TG_CAP",rs.getString("TG_CAP"));
            valueMap.put("TG_NO",rs.getString("TG_NO"));
            valueMap.put("TG_NAME",rs.getString("TG_NAME"));
            valueMap.put("PUB_PRIV_FLAG",rs.getString("PUB_PRIV_FLAG"));
            valueMap.put("CT_RATIO",rs.getString("CT_RATIO"));
            valueMap.put("PT_RATIO",rs.getString("PT_RATIO"));
            valueMap.put("T_FACTOR",rs.getString("T_FACTOR"));

            m.put(mapKey,valueMap);
            hashList.add(m);
        }
        return hashList;
    }

    private static String getTgAboutParamSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append("select                   ");
        sb.append(" r.TERMINAL_ID as TERMINAL_ID,           ");//终端标识
        sb.append(" r.ORG_NO as ORG_NO,                     ");//单位编号
        sb.append(" r.MPED_ID as MPED_ID,                   ");//测量点标识:
        sb.append(" TG_CAP,                                 ");//台区容量,为可并列运行的变压器容量之和
        sb.append(" TG_NO,                                  ");//台区编码'
        sb.append(" TG_NAME,                                ");//台区名称
        sb.append(" PUB_PRIV_FLAG,                          ");//公变专变标志 01-公变 02-专变
        sb.append(" CT_RATIO,                               ");//'电流互感器的变比',
        sb.append(" PT_RATIO,                               ");//'电压互感器的变比'
        sb.append(" T_FACTOR                                ");//综合倍率=CT变比*PT变比

        sb.append(" from                                    ");
        sb.append(" r_mped r JOIN c_mp c                    ");
        sb.append(" ON r.cons_id = c.cons_id AND r.shard_no = c.shard_no                    ");
        sb.append(" join g_tg t on c.tg_id=t.TG_ID and c.shard_no = t.SHARD_NO              ");
        sb.append(" WHERE  r.TERMINAL_ID=? and r.SHARD_NO=? and c.usage_type_code='02'      ");
        // c_mp ,r_mped,g_tg
        // usage_type_code = '02'
        return sb.toString();
    }


}
