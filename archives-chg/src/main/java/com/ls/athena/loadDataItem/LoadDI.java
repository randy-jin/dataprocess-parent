package com.ls.athena.loadDataItem;

import com.ls.athena.utils.SpringUtils;
import com.ls.pf.common.dataCatch.bz.bo.ObjectBz;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 加载dataitem
 */
@Component
public class LoadDI implements ApplicationRunner {

    private static Logger log = LoggerFactory.getLogger(LoadDI.class);

    private static final String load_dataitem = getDataitem();
    private static final String load_pcode = getPCode();
    private static final String get_pcode = " SELECT EXTEND_CONTENT03, CODE_NAME FROM	p_standard_code where CODE_TYPE_CODE = ? ORDER BY EXTEND_CONTENT03";


    private static IOperateBzData operateBzData;

    private static JdbcTemplate jdbcTemplate;

    private RedisTemplate defaultRedisTemplate;

    @PostConstruct
    private void load() {
        operateBzData = SpringUtils.getBean("operateBzData");
        jdbcTemplate = SpringUtils.getBean("jdbcTemplate");
        defaultRedisTemplate = SpringUtils.getBean("defaultRedisTemplate");
    }

    @SuppressWarnings("unchecked")
    private static void loadDataItem() throws Exception {
        List<ObjectBz> list = getDataItemInfo();
        if (list == null || list.size() == 0) { //没有添加与更新的档案
            return;
        }
        log.info("初始化dataitem");
        operateBzData.loadDataItem(list);
        log.info("成功初始化");
    }

    @SuppressWarnings("unchecked")
    private static void loadPCode() throws Exception {
        List<ObjectBz> list = getPStCodeInfo();
        if (list == null || list.size() == 0) { //没有添加与更新的档案
            return;
        }
        log.info("初始化低电压p_standard_code");
        operateBzData.loadPCode(list);
        log.info("成功初始化低电压p_standard_code");
    }

    private static List<ObjectBz> getPStCodeInfo() throws Exception {
        List<ObjectBz> list = new ArrayList<>();
        SqlRowSet rs = jdbcTemplate.queryForRowSet(load_pcode);
        ObjectBz objBz;
        while (rs.next()) {
            List<Map<String, String>> pCodeList = new ArrayList<Map<String, String>>();
            objBz = new ObjectBz();
            String ctc = rs.getString("CODE_TYPE_CODE");
            objBz.setDataItemId(ctc);
            SqlRowSet rs1 = jdbcTemplate.queryForRowSet(get_pcode, new String[]{ctc});
            while (rs1.next()) {
                Map<String, String> dataItem = new HashMap<String, String>();
                if (rs1.getString("EXTEND_CONTENT03") != null && rs1.getString("EXTEND_CONTENT03") != "") {
                    dataItem.put(rs1.getString("EXTEND_CONTENT03"), rs1.getString("CODE_NAME"));
                    pCodeList.add(dataItem);
                }
            }
            if (pCodeList.size() <= 0) {
                continue;
            }
            objBz.setDataItemList(pCodeList);
            list.add(objBz);
        }
        return list;
    }

    private static List<ObjectBz> getDataItemInfo() throws Exception {
        List<ObjectBz> list = new ArrayList<ObjectBz>();
        SqlRowSet rs = jdbcTemplate.queryForRowSet(load_dataitem);
        ObjectBz objBz;
        System.out.println("start set pt$");
        while (rs.next()) {
            if ("".equals(rs.getString("CMD"))) {
                continue;
            }
            String cmds[] = rs.getString("CMD").split("\\|");
            for (int i = 0; i < cmds.length; i++) {
                String cmd = cmds[i];//得到cmd
                String[] cmdSplit = cmd.split("-");
                String pid = "";
                if (cmdSplit.length < 2) {
                    pid = "9";
                } else {
                    pid = cmdSplit[0];
                }
                //得到规约id
                List<Map<String, String>> dataItemList = new ArrayList<>();
                objBz = new ObjectBz();
                objBz.setDataItemId(rs.getString("ID"));
                objBz.setProtocolId(pid);
                Map<String, String> dataItem = new HashMap<String, String>();
                dataItem.put("TYPE", rs.getString("DEVICE_TYPE"));
                dataItem.put("OBJF", rs.getString("OBJ_FLAG"));
                dataItem.put("DFLAG", rs.getString("DATA_FLAG"));
                dataItem.put("DUNIT", rs.getString("CMD"));//对于376.1为：AFN|FN	对于645为：AFN|FN|控制域|数据项标识
                dataItem.put("SPLITCMD", cmd);//针对需要split cmd增加的
                dataItem.put("NAME", rs.getString("NAME"));
                dataItemList.add(dataItem);
                if (dataItemList.size() <= 0) {
                    continue;
                }
                objBz.setDataItemList(dataItemList);
                list.add(objBz);
            }
        }
        System.out.println("end set pt$");
        return list;
    }

    /**
     * 对外接口启动线程
     */
    public static void startThread() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

        singleThreadExecutor.execute(() -> {
            try {
                // 延时加载
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            //第一次档案加载业务数据项档案信息
            try {
                loadDataItem();
                loadPCode();
            } catch (Exception e) {
                log.error("e:", e);
            }

        });
        singleThreadExecutor.shutdown();
    }


    private static String getDataitem() {
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT                               ");
        sb.append(" 	B.ID,                            ");
        sb.append(" 	B.PROTOCOL_ID,                   ");
        sb.append(" 	P.DEVICE_TYPE,                   ");
        sb.append(" 	D.OBJ_FLAG,                      ");
        sb.append(" 	D.DATA_FLAG,                     ");
        sb.append(" 	D.CMD,                           ");
        sb.append(" 	B.NAME                           ");
        sb.append(" FROM                                 ");
        sb.append(" 	R_PROTOCOL P,                    ");
        sb.append(" 	R_BUSINESS_DATAITEM B,           ");
        sb.append(" 	R_DATAITEM D                     ");
        sb.append(" WHERE                                ");
        sb.append(" 	P.PROTOCOL_ID = B.PROTOCOL_ID    ");
        sb.append(" AND B.DATAITEM_ID = D.DATAITEM_ID    ");
        return sb.toString();
    }

    private static String getPCode() {
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT                 ");
        sb.append(" 	t.CODE_TYPE_CODE   ");
        sb.append(" FROM                   ");
        sb.append(" 	p_standard_code t  ");
        sb.append(" group BY               ");
        sb.append(" 	t.CODE_TYPE_CODE   ");
        return sb.toString();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Boolean chg_item_load = defaultRedisTemplate
                .opsForValue()
                .setIfAbsent("CHG_ITEM_LOAD", "load", 3, TimeUnit.MINUTES);
        if (chg_item_load) {
            startThread();
        }
    }
}
