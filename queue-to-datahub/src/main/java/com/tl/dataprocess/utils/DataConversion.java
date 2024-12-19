//package com.tl.dataprocess.utils;
//
//import com.tl.archives.TerminalArchivesObject;
//import com.tl.utils.DateUtil;
//import org.apache.commons.lang3.StringUtils;
//
//import java.math.BigDecimal;
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
///**
// * Created by huangchunhuai on 2021/11/26.
// */
//public class DataConversion {
//
//    public static List<Object> e_mp_day_read(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        dataList.remove(0);//删除数据list中的主键，因为前置为null需要自己放
//        String dataType=dataList.remove(0).toString();//删除并取出list当前index为0的值作为dataType
//        String dataDate=dataList.remove(0).toString();//删除并取出list当前index为0的值作为数据日期
//        Object callObj=dataList.remove(0);//删除并取出list当前index为0的值作为抄表时间
//        Date callDate=new Date();
//        if(callObj!=null){
//            callDate=new Date(Long.parseLong(callObj.toString()));
//        }
//        //用于缓存删除和datahub自动分配shardid, 每个表不同，要么是 测量点id 要么是 终端id
//        String shardIndex=terminalArchivesObject.getMpedId();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//
//
//        finallyList.add(terminalArchivesObject.getMpedId()+"_"+dataDate);
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataType);
//        finallyList.add(callDate);
//        for (int i=0;i<15;i++) {
//            Object obj=dataList.get(i);
//            if(obj==null){
//                finallyList.add(null);
//                continue;
//            }
//            finallyList.add(Double.parseDouble(dataList.get(i).toString()));
//        }
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add("00");
//        finallyList.add(dataSrc);
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));
//        finallyList.add(dataDate);
//
//        return finallyList;
//    }
//
//    /**
//     * 状态字1
//     * @author wjj
//     * @date 2021/11/29 9:23
//     * @return
//     */
//    public static List<Object> e_meter_run_status_num1(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
//        return status(dataList,terminalArchivesObject,dataSrc);
//    }
//    /**
//     * 状态字3
//     * @author wjj
//     * @date 2021/11/29 9:23
//     * @return
//     */
//    public static List<Object> e_meter_run_status_num3(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
//       return status(dataList,terminalArchivesObject,dataSrc);
//    }
//    private static List<Object> status(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //mped_id:BIGINT
//        //data_date:STRING
//        //org_no:STRING
//        //status:STRING
//        //shard_no:STRING
//        //insert_time:TIMESTAMP
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(1));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(dataList.get(3));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));
//        finallyList.add(new Date());
//        return finallyList;
//    }
//
//    /**
//     * 透支金额
//     * @author wjj
//     * @date 2021/11/29 14:24
//     * @return
//     */
//    public static List<Object> e_mped_real_overdraft(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        return template1(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 内部时钟电池电压
//     * @author wjj
//     * @date 2021/11/29 14:41
//     * @return
//     */
//    public static List<Object> e_meter_inside_battery_voltage(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        return template1(dataList, terminalArchivesObject, dataSrc);
//    }
//    private static List<Object> template1(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
////        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,BATTERY_VOLTAGE:DOUBLE,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(1));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(new Date());
//        return finallyList;
//    }
//
//    /**
//     * 内部电池工作时间
//     * @author wjj
//     * @date 2021/11/29 14:41
//     * @return
//     */
//    public static List<Object> e_meter_inside_work_time(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getTerminalId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
////        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,WORK_TIME:BIGINT,
//// SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(1));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(new Date());
//        return finallyList;
//    }
//
//    /**
//     * 电流
//     * @author wjj
//     * @date 2021/11/29 9:24
//     * @return
//     */
//    public static List<Object> e_edc_cons_cur(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
////        ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,I_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,
//// DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        return template2(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电压
//     * @author wjj
//     * @date 2021/11/29 11:30
//     * @return
//     */
//    public static List<Object> e_edc_cons_volt(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
//        // ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,U_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,
//        // DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        return template2(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 瞬时有功功率
//     * @author wjj
//     * @date 2021/11/29 14:50
//     * @return
//     */
//    public static List<Object> e_edc_cons_power(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        // ID:BIGINT,DATA_DATE:STRING,DATA_TYPE:STRING,ORG_NO:STRING,P_VALUE:DOUBLE,
//        // POINT_TIME:TIMESTAMP,STATUS:STRING,DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        return template2(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 总功率因数
//     * @author wjj
//     * @date 2021/11/29 17:20
//     * @return
//     */
//    public static List<Object> e_edc_cons_factor(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //        ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,C_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,
//        // STATUS:STRING,DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        return template2(dataList, terminalArchivesObject, dataSrc);
//    }
//    private static List<Object> template2(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(1));
//        finallyList.add(dataList.get(2));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Double.class));
//        finallyList.add(new Date());
//        finallyList.add("00");
//        finallyList.add(dataSrc);
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 终端实时时钟信息
//     * @author wjj
//     * @date 2021/11/30 10:10
//     * @return
//     */
//    public static List<Object> e_tmnl_real_time(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getTerminalId();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//
//        //TERMINAL_ID:BIGINT,AREA_CODE:BIGINT,TERMINAL_ADDR:STRING,T_TIME:TIMESTAMP,M_TIME:TIMESTAMP,OVER_TIME:DOUBLE,
//        // CON_TIME_NUM:BIGINT,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getTerminalId()));
//        finallyList.add(new BigDecimal(terminalArchivesObject.getAreaCode()));
//        finallyList.add(terminalArchivesObject.getTerminalAddr());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));
//        finallyList.add(getDataByObj(dataList.get(6), BigDecimal.class));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电表时钟
//     * @param dataList
//     * @param terminalArchivesObject
//     * @param dataSrc
//     * @return
//     */
//    public static List<Object> e_meter_time(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getTerminalId();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //DATA_DATE:STRING,MPED_ID:BIGINT,SET_TIME:STRING,COLL_TIME:TIMESTAMP,TIME_TYPE:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(getDataByObj(dataList.get(0), Date.class));
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Double.class));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 购电状态
//     * @author wjj
//     * @date 2021/11/30 15:31
//     * @return
//     */
//    public static List<Object> e_mped_real_buy_record(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //ID:BIGINT,DATA_DATE:STRING,COL_TIME:TIMESTAMP,ORG_NO:STRING,REMAIN_ENEGY:DOUBLE,REMAIN_MONEY:DOUBLE,
//        // ALARM_ENEGY:DOUBLE,FAIL_ENEGY:DOUBLE,SUM_ENEGY:DOUBLE,SUM_MONEY:DOUBLE,BUY_NUM:BIGINT,
//        // OVERDR_LIMIT:DOUBLE,OVERDR_ENEGY:DOUBLE,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(1));
//        Object callObj=dataList.get(2);//抄表时间处理
//        Date callDate=new Date();
//        if(callObj!=null){
//            callDate=new Date(Long.parseLong(callObj.toString()));
//        }
//        finallyList.add(callDate);
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Double.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * (上1结算日)正向有功总最大需量及发生时间
//     * @author wjj
//     * @date 2021/11/30 16:16
//     * @return
//     */
//    public static List<Object> e_mp_day_demand(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        return demand(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 月需量
//     * @param dataList
//     * @param terminalArchivesObject
//     * @param dataSrc
//     * @return
//     */
//    public static List<Object> e_mp_mon_demand(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        return demand(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 需量公共处理
//     * @param dataList
//     * @param terminalArchivesObject
//     * @param dataSrc
//     * @return
//     */
//    private static List<Object> demand(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //ID:BIGINT,DATA_TYPE:STRING,COL_TIME:TIMESTAMP,DEMAND:DOUBLE,DEMAND_TIME:TIMESTAMP,DEMAND1:DOUBLE,
//        // DEMAND_TIME1:TIMESTAMP,DEMAND2:DOUBLE,DEMAND_TIME2:TIMESTAMP,DEMAND3:DOUBLE,DEMAND_TIME3:TIMESTAMP,
//        // DEMAND4:DOUBLE,DEMAND_TIME4:TIMESTAMP,ORG_NO:STRING,STATUS:STRING,DATA_SRC:STRING,
//        // INSERT_TIME:TIMESTAMP,SHARD_NO:STRING,DATA_DATE:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(dataList.get(2));
//
//        Object callObj=dataList.get(3);//抄表时间处理
//        Date callDate=new Date();
//        if(callObj!=null){
//            callDate=new Date(Long.parseLong(callObj.toString()));
//        }
//        finallyList.add(callDate);
//        finallyList.add(getDataByObj(dataList.get(4), Double.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));//DEMAND1
//        finallyList.add(getDataByObj(dataList.get(7), Date.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Date.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Date.class));//DEMAND_TIME3
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add("00");
//        finallyList.add(dataSrc);
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(dataList.get(1));
//        return finallyList;
//    }
//
//    /**
//     * 电能表开表盖事件
//     * @author wjj
//     * @date 2021/12/6 10:09
//     * @return
//     */
//    public static List<Object> e_meter_event_open_lid(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,BEFORE_PAP_R:DOUBLE,
//        // BEFORE_RAP_R:DOUBLE,BEFORE_QUAD_1_R:DOUBLE,BEFORE_QUAD_2_R:DOUBLE,BEFORE_QUAD_3_R:DOUBLE,
//        // BEFORE_QUAD_4_R:DOUBLE,AFTER_PAP_R:DOUBLE,AFTER_RAP_R:DOUBLE,AFTER_QUAD_1_R:DOUBLE,AFTER_QUAD_2_R:DOUBLE,
//        // AFTER_QUAD_3_R:DOUBLE,AFTER_QUAD_4_R:DOUBLE,SORT_NO:BIGINT,EXTEN_INFO:STRING,SHARD_NO:STRING,EVENT_DATE:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));//BEFORE_PAP_R
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));//BEFORE_QUAD_3_R
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));//AFTER_QUAD_2_R
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));
//        finallyList.add(getDataByObj(dataList.get(18), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(19), String.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(getDataByObj(dataList.get(1), String.class));
//        return finallyList;
//    }
//
//    /**
//     * 电能表恒定磁场干扰事件记录
//     * @author wjj
//     * @date 2021/12/6 11:21
//     * @return
//     */
//    public static List<Object> e_meter_event_mag_interf(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,ORG_NO:STRING,INPUT_TIME:TIMESTAMP,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,ST_PAP_R:DOUBLE,
//        // ST_RAP_R:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,SHARD_NO:STRING,EVENT_DATE:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(new Date());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));//ST_PAP_R
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(dataDate);
//        return finallyList;
//    }
//
//    /**
//     * 电能表停电事件
//     * @author wjj
//     * @date 2021/12/6 15:33
//     * @return
//     */
//    public static List<Object> e_meter_event_no_power(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        //用于缓存删除
//        String clearDate="00000000000000";
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_TYPE:STRING,NO_POWER_SD:TIMESTAMP,
//        // NO_POWER_ED:TIMESTAMP,SHARD_NO:STRING,EVENT_DATE:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), String.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), String.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));//NO_POWER_SD
//        finallyList.add(getDataByObj(dataList.get(6), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(getDataByObj(dataList.get(1), String.class));
//        return finallyList;
//    }
//
//    /**
//     * 开盖总次数
//     * @author wjj
//     * @date 2021/12/6 16:23
//     * @return
//     */
//    public static List<Object> e_event_meter_total_num(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMpedId();
////        String dataDate = dataList.get(1).toString();
////        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //MPED_ID:BIGINT,DATAITEM_ID:BIGINT,TERMINAL_ID:BIGINT,TOTAL_NUM:BIGINT,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(getDataByObj(dataList.get(1), BigDecimal.class));
//        finallyList.add(new BigDecimal(terminalArchivesObject.getTerminalId()));
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        finallyList.add(new Date());
//        return finallyList;
//    }
//
//    /**
//     * 电能表全失压事件
//     * @author wjj
//     * @date 2021/12/6 17:04
//     * @return
//     */
//    public static List<Object> e_meter_event_all_vol_lose(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,CURR_VAL:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表清零事件记录记录
//     * @author wjj
//     * @date 2021/12/6 17:18
//     * @return
//     */
//    public static List<Object> e_meter_event_clear(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,BEFORE_PAP_R:DOUBLE,
//        // BEFORE_RAP_R:DOUBLE,BEFORE_QUAD_1_R:DOUBLE,BEFORE_QUAD_2_R:DOUBLE,BEFORE_QUAD_3_R:DOUBLE,
//        // BEFORE_QUAD_4_R:DOUBLE,BEFORE_PAP_R_A:DOUBLE,BEFORE_RAP_R_A:DOUBLE,BEFORE_QUAD_1_R_A:DOUBLE,
//        // BEFORE_QUAD_2_R_A:DOUBLE,BEFORE_QUAD_3_R_A:DOUBLE,BEFORE_QUAD_4_R_A:DOUBLE,BEFORE_PAP_R_B:DOUBLE,
//        // BEFORE_RAP_R_B:DOUBLE,BEFORE_QUAD_1_R_B:DOUBLE,BEFORE_QUAD_2_R_B:DOUBLE,BEFORE_QUAD_3_R_B:DOUBLE,
//        // BEFORE_QUAD_4_R_B:DOUBLE,BEFORE_PAP_R_C:DOUBLE,BEFORE_RAP_R_C:DOUBLE,BEFORE_QUAD_1_R_C:DOUBLE,
//        // BEFORE_QUAD_2_R_C:DOUBLE,BEFORE_QUAD_3_R_C:DOUBLE,BEFORE_QUAD_4_R_C:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));//BEFORE_PAP_R
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));//BEFORE_QUAD_3_R
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));//BEFORE_QUAD_1_R_A
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));//BEFORE_PAP_R_B
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(getDataByObj(dataList.get(20), Double.class));
//        finallyList.add(getDataByObj(dataList.get(21), Double.class));//BEFORE_QUAD_3_R_B
//        finallyList.add(getDataByObj(dataList.get(22), Double.class));
//        finallyList.add(getDataByObj(dataList.get(23), Double.class));
//        finallyList.add(getDataByObj(dataList.get(24), Double.class));
//        finallyList.add(getDataByObj(dataList.get(25), Double.class));//BEFORE_QUAD_1_R_C
//        finallyList.add(getDataByObj(dataList.get(26), Double.class));
//        finallyList.add(getDataByObj(dataList.get(27), Double.class));
//        finallyList.add(getDataByObj(dataList.get(28), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表事件清零
//     * @author wjj
//     * @date 2021/12/7 10:32
//     * @return
//     */
//    public static List<Object> e_meter_event_clear_event(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,REPORT_OR_COLL:BIGINT,COLL_TIME:TIMESTAMP,
//        // EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,EVENT_FLAG:STRING,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(7), String.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表校时事件记录
//     * @author wjj
//     * @date 2021/12/7 10:40
//     * @return
//     */
//    public static List<Object> e_meter_event_chek_time(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,BEFORE_CHECK_TIME:TIMESTAMP,AFTER_CHECK_TIME:TIMESTAMP,OP_CODE:BIGINT,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), BigDecimal.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表有功组合方式编程事件记录
//     * @author wjj
//     * @date 2021/12/7 11:07
//     * @return
//     */
//    public static List<Object> e_meter_event_ap_group_pro (List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template3(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表无功组合方式1/2编程事件记录
//     * @author wjj
//     * @date 2021/12/7 11:16
//     * @return
//     */
//    public static List<Object> e_meter_event_rq_group_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template3(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表结算日编程事件记录
//     * @author wjj
//     * @date 2021/12/7 11:23
//     * @return
//     */
//    public static List<Object> e_meter_event_settleday_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template3(dataList, terminalArchivesObject, dataSrc);
//    }
//    private static List<Object> template3(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(1), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表开端钮盖事件
//     * @author wjj
//     * @date 2021/12/7 11:29
//     * @return
//     */
//    public static List<Object> e_meter_event_open_term_lid(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,BEFORE_PAP_R:DOUBLE,
//        // BEFORE_RAP_R:DOUBLE,BEFORE_QUAD_1_R:DOUBLE,BEFORE_QUAD_2_R:DOUBLE,BEFORE_QUAD_3_R:DOUBLE,
//        // BEFORE_QUAD_4_R:DOUBLE,AFTER_PAP_R:DOUBLE,AFTER_RAP_R:DOUBLE,AFTER_QUAD_1_R:DOUBLE,AFTER_QUAD_2_R:DOUBLE,
//        // AFTER_QUAD_3_R:DOUBLE,AFTER_QUAD_4_R:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));//BEFORE_PAP_R
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));//BEFORE_QUAD_3_R
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));//AFTER_QUAD_2_R
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表正有功需量越限记录
//     * @author wjj
//     * @date 2021/12/7 14:31
//     * @return
//     */
//    public static List<Object> e_meter_event_pap_demd_over_li(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template3(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表反向有功需量越限记录
//     * @author wjj
//     * @date 2021/12/7 14:35
//     * @return
//     */
//    public static List<Object> e_meter_event_rap_demd_over_li(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template3(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表潮流反向事件记录
//     * @author wjj
//     * @date 2021/12/7 14:39
//     * @return
//     */
//    public static List<Object> e_meter_event_curr_return(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,
//        // ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,ST_GRP_1_R_A:DOUBLE,
//        // ST_GRP_2_R_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,
//        // ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), Date.class));
//        finallyList.add(getDataByObj(dataList.get(4), Double.class));
//        finallyList.add(getDataByObj(dataList.get(5), Double.class));//ST_RAP_R
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));//ST_GRP_1_R_A
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));//ST_GRP_2_R_B
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表失流事件
//     * @author wjj
//     * @date 2021/12/7 14:44
//     * @return
//     */
//    public static List<Object> e_meter_event_lose_curr(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//    /**
//     * 电能表过流事件记录
//     * @author wjj
//     * @date 2021/12/7 15:13
//     * @return
//     */
//    public static List<Object> e_meter_event_over_i(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    private static List<Object> template4(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));//EVENT_ET
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));//ST_RAP_R_A
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));//ST_Q_A
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(getDataByObj(dataList.get(20), Double.class));
//        finallyList.add(getDataByObj(dataList.get(21), Double.class));
//        finallyList.add(getDataByObj(dataList.get(22), Double.class));
//        finallyList.add(getDataByObj(dataList.get(23), Double.class));//ST_U_B
//        finallyList.add(getDataByObj(dataList.get(24), Double.class));
//        finallyList.add(getDataByObj(dataList.get(25), Double.class));
//        finallyList.add(getDataByObj(dataList.get(26), Double.class));
//        finallyList.add(getDataByObj(dataList.get(27), Double.class));
//        finallyList.add(getDataByObj(dataList.get(28), Double.class));
//        finallyList.add(getDataByObj(dataList.get(29), Double.class));//ST_RAP_R_C
//        finallyList.add(getDataByObj(dataList.get(30), Double.class));
//        finallyList.add(getDataByObj(dataList.get(31), Double.class));
//        finallyList.add(getDataByObj(dataList.get(32), Double.class));
//        finallyList.add(getDataByObj(dataList.get(33), Double.class));
//        finallyList.add(getDataByObj(dataList.get(34), Double.class));
//        finallyList.add(getDataByObj(dataList.get(35), Double.class));//ST_Q_C
//        finallyList.add(getDataByObj(dataList.get(36), Double.class));
//        finallyList.add(getDataByObj(dataList.get(37), Double.class));
//        finallyList.add(getDataByObj(dataList.get(38), Double.class));
//        finallyList.add(getDataByObj(dataList.get(39), Double.class));
//        finallyList.add(getDataByObj(dataList.get(40), Double.class));
//        finallyList.add(getDataByObj(dataList.get(41), Double.class));//EN_PAP_R_A
//        finallyList.add(getDataByObj(dataList.get(42), Double.class));
//        finallyList.add(getDataByObj(dataList.get(43), Double.class));
//        finallyList.add(getDataByObj(dataList.get(44), Double.class));
//        finallyList.add(getDataByObj(dataList.get(45), Double.class));
//        finallyList.add(getDataByObj(dataList.get(46), Double.class));//EN_RAP_R_B
//        finallyList.add(getDataByObj(dataList.get(47), Double.class));
//        finallyList.add(getDataByObj(dataList.get(48), Double.class));
//        finallyList.add(getDataByObj(dataList.get(49), Double.class));
//        finallyList.add(getDataByObj(dataList.get(50), Double.class));
//        finallyList.add(getDataByObj(dataList.get(51), Double.class));//EN_GRP_1_R_C
//        finallyList.add(getDataByObj(dataList.get(52), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表断流事件记录
//     * @author wjj
//     * @date 2021/12/7 15:38
//     * @return
//     */
//    public static List<Object> e_meter_event_no_curr(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(1), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表有功功率反向事件记录
//     * @author wjj
//     * @date 2021/12/10 15:44
//     * @return
//     */
//    public static List<Object> e_meter_event_p_return(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,
//        // ST_GRP_2_R_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,
//        // EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,EN_RAP_R_A:DOUBLE,
//        // EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,EN_GRP_1_R_B:DOUBLE,
//        // EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,EN_GRP_2_R_C:DOUBLE,
//        // SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(new Date());
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));//EVENT_ET
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));//ST_RAP_R_A
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));//ST_GRP_1_R_B
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(getDataByObj(dataList.get(20), Double.class));
//        finallyList.add(getDataByObj(dataList.get(21), Double.class));//ST_GRP_2_R_C
//        finallyList.add(getDataByObj(dataList.get(22), Double.class));
//        finallyList.add(getDataByObj(dataList.get(23), Double.class));
//        finallyList.add(getDataByObj(dataList.get(24), Double.class));
//        finallyList.add(getDataByObj(dataList.get(25), Double.class));
//        finallyList.add(getDataByObj(dataList.get(26), Double.class));
//        finallyList.add(getDataByObj(dataList.get(27), Double.class));//EN_RAP_R_A
//        finallyList.add(getDataByObj(dataList.get(28), Double.class));
//        finallyList.add(getDataByObj(dataList.get(29), Double.class));
//        finallyList.add(getDataByObj(dataList.get(30), Double.class));
//        finallyList.add(getDataByObj(dataList.get(31), Double.class));
//        finallyList.add(getDataByObj(dataList.get(32), Double.class));//EN_GRP_1_R_B
//        finallyList.add(getDataByObj(dataList.get(33), Double.class));
//        finallyList.add(getDataByObj(dataList.get(34), Double.class));
//        finallyList.add(getDataByObj(dataList.get(35), Double.class));
//        finallyList.add(getDataByObj(dataList.get(36), Double.class));
//        finallyList.add(getDataByObj(dataList.get(37), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表失压事件
//     * @author wjj
//     * @date 2021/12/8 9:36
//     * @return
//     */
//    public static List<Object> e_meter_event_lose_vol(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表欠压事件记录
//     * @author wjj
//     * @date 2021/12/8 9:42
//     * @return
//     */
//    public static List<Object> e_meter_event_low_vol(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表过压事件记录
//     * @author wjj
//     * @date 2021/12/8 11:15
//     * @return
//     */
//    public static List<Object> e_meter_event_over_vol(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表断相事件记录
//     * @author wjj
//     * @date 2021/12/8 14:38
//     * @return
//     */
//    public static List<Object> e_meter_event_vol_break(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
//        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
//        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
//        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
//        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
//        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
//        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
//        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        return template4(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表电压逆相序事件记录
//     * @author wjj
//     * @date 2021/12/8 16:33
//     * @return
//     */
//    public static List<Object> e_meter_event_reve_phase(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.remove(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,ST_PAP_R:DOUBLE,
//        // ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
//        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,
//        // ST_GRP_2_R_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,
//        // EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,EN_RAP_R_A:DOUBLE,
//        // EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,EN_GRP_1_R_B:DOUBLE,
//        // EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,EN_GRP_2_R_C:DOUBLE,
//        // SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));//ST_PAP_R
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));//ST_RAP_R_A
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));//ST_GRP_1_R_B
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(getDataByObj(dataList.get(20), Double.class));
//        finallyList.add(getDataByObj(dataList.get(21), Double.class));//ST_GRP_2_R_C
//        finallyList.add(getDataByObj(dataList.get(22), Double.class));
//        finallyList.add(getDataByObj(dataList.get(23), Double.class));
//        finallyList.add(getDataByObj(dataList.get(24), Double.class));
//        finallyList.add(getDataByObj(dataList.get(25), Double.class));
//        finallyList.add(getDataByObj(dataList.get(26), Double.class));
//        finallyList.add(getDataByObj(dataList.get(27), Double.class));//EN_RAP_R_A
//        finallyList.add(getDataByObj(dataList.get(28), Double.class));
//        finallyList.add(getDataByObj(dataList.get(29), Double.class));
//        finallyList.add(getDataByObj(dataList.get(30), Double.class));
//        finallyList.add(getDataByObj(dataList.get(31), Double.class));
//        finallyList.add(getDataByObj(dataList.get(32), Double.class));//EN_GRP_1_R_B
//        finallyList.add(getDataByObj(dataList.get(33), Double.class));
//        finallyList.add(getDataByObj(dataList.get(34), Double.class));
//        finallyList.add(getDataByObj(dataList.get(35), Double.class));
//        finallyList.add(getDataByObj(dataList.get(36), Double.class));
//        finallyList.add(getDataByObj(dataList.get(37), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表电流逆相序事件记录
//     * @author wjj
//     * @date 2021/12/8 16:37
//     * @return
//     */
//    public static List<Object> e_meter_event_i_reve_phase(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表电压不平衡事件记录
//     * @author wjj
//     * @date 2021/12/8 16:41
//     * @return
//     */
//    public static List<Object> e_meter_event_vol_unbalance(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
//        // MAX_UNBALANCE_RATE:DOUBLE,ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,
//        // ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,ST_GRP_1_R_A:DOUBLE,STz_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,
//        // ST_P_A:DOUBLE,ST_Q_A:DOUBLE,ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,
//        // ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,
//        // ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,
//        // ST_P_C:DOUBLE,ST_Q_C:DOUBLE,ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,
//        // EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,
//        // EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,
//        // EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));//EVENT_ET
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));//ST_GRP_2_R
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(getDataByObj(dataList.get(13), Double.class));
//        finallyList.add(getDataByObj(dataList.get(14), Double.class));
//        finallyList.add(getDataByObj(dataList.get(15), Double.class));
//        finallyList.add(getDataByObj(dataList.get(16), Double.class));//ST_I_A
//        finallyList.add(getDataByObj(dataList.get(17), Double.class));
//        finallyList.add(getDataByObj(dataList.get(18), Double.class));
//        finallyList.add(getDataByObj(dataList.get(19), Double.class));
//        finallyList.add(getDataByObj(dataList.get(20), Double.class));
//        finallyList.add(getDataByObj(dataList.get(21), Double.class));
//        finallyList.add(getDataByObj(dataList.get(22), Double.class));//ST_GRP_1_R_B
//        finallyList.add(getDataByObj(dataList.get(23), Double.class));
//        finallyList.add(getDataByObj(dataList.get(24), Double.class));
//        finallyList.add(getDataByObj(dataList.get(25), Double.class));
//        finallyList.add(getDataByObj(dataList.get(26), Double.class));
//        finallyList.add(getDataByObj(dataList.get(27), Double.class));
//        finallyList.add(getDataByObj(dataList.get(28), Double.class));//ST_F_B
//        finallyList.add(getDataByObj(dataList.get(29), Double.class));
//        finallyList.add(getDataByObj(dataList.get(30), Double.class));
//        finallyList.add(getDataByObj(dataList.get(31), Double.class));
//        finallyList.add(getDataByObj(dataList.get(32), Double.class));
//        finallyList.add(getDataByObj(dataList.get(33), Double.class));
//        finallyList.add(getDataByObj(dataList.get(34), Double.class));//ST_I_C
//        finallyList.add(getDataByObj(dataList.get(35), Double.class));
//        finallyList.add(getDataByObj(dataList.get(36), Double.class));
//        finallyList.add(getDataByObj(dataList.get(37), Double.class));
//        finallyList.add(getDataByObj(dataList.get(38), Double.class));
//        finallyList.add(getDataByObj(dataList.get(39), Double.class));
//        finallyList.add(getDataByObj(dataList.get(40), Double.class));//EN_GRP_1_R
//        finallyList.add(getDataByObj(dataList.get(41), Double.class));
//        finallyList.add(getDataByObj(dataList.get(42), Double.class));
//        finallyList.add(getDataByObj(dataList.get(43), Double.class));
//        finallyList.add(getDataByObj(dataList.get(44), Double.class));
//        finallyList.add(getDataByObj(dataList.get(45), Double.class));//EN_GRP_2_R_A
//        finallyList.add(getDataByObj(dataList.get(46), Double.class));
//        finallyList.add(getDataByObj(dataList.get(47), Double.class));
//        finallyList.add(getDataByObj(dataList.get(48), Double.class));
//        finallyList.add(getDataByObj(dataList.get(49), Double.class));
//        finallyList.add(getDataByObj(dataList.get(50), Double.class));//EN_PAP_R_C
//        finallyList.add(getDataByObj(dataList.get(51), Double.class));
//        finallyList.add(getDataByObj(dataList.get(52), Double.class));
//        finallyList.add(getDataByObj(dataList.get(53), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表电流严重不平衡事件记录
//     * @author wjj
//     * @date 2021/12/9 17:01
//     * @return
//     */
//    public static List<Object> e_meter_event_cur_high_unbalan(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//    private static List<Object> template5(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表跳闸事件记录
//     * @author wjj
//     * @date 2021/12/9 17:01
//     * @return
//     */
//    public static List<Object> e_meter_event_trip(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,
//        // OP_CODE:BIGINT,PAP_R:DOUBLE,RAP_R:DOUBLE,QUAD_1_R:DOUBLE,QUAD_2_R:DOUBLE,
//        // QUAD_3_R:DOUBLE,QUAD_4_R:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));//EVENT_ST
//        finallyList.add(getDataByObj(dataList.get(5), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));//QUAD_2_R
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表合闸事件
//     * @author wjj
//     * @date 2021/12/9 17:30
//     * @return
//     */
//    public static List<Object> e_meter_event_switch_on(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,COLL_TIME:TIMESTAMP,
//        // EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,PAP_R:DOUBLE,RAP_R:DOUBLE,
//        // QUAD_1_R:DOUBLE,QUAD_2_R:DOUBLE,QUAD_3_R:DOUBLE,QUAD_4_R:DOUBLE,
//        // SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));//COLL_TIME
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));//RAP_R
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(getDataByObj(dataList.get(11), Double.class));
//        finallyList.add(getDataByObj(dataList.get(12), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表功率因数越下限事件记录
//     * @author wjj
//     * @date 2021/12/9 17:33
//     * @return
//     */
//    public static List<Object> e_meter_event_fact_lower_limit(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,COLL_TIME:TIMESTAMP,
//        // EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));//COLL_TIME
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表电流不平衡事件记录
//     * @author wjj
//     * @date 2021/12/9 18:09
//     * @return.
//     */
//    public static List<Object> e_meter_event_cur_unbalance(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表辅助电源掉电事件记录
//     * @author wjj
//     * @date 2021/12/10 9:40
//     * @return
//     */
//    public static List<Object> e_meter_event_assi_power_shut(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表周休日编程事件记录
//     * @author wjj
//     * @date 2021/12/10 9:43
//     * @return
//     */
//    public static List<Object> e_meter_event_weekday_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表密钥更新事件记录
//     * @author wjj
//     * @date 2021/12/10 9:48
//     * @return
//     */
//    public static List<Object> e_meter_event_esam_key_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表异常插卡事件记录
//     * @author wjj
//     * @date 2021/12/10 9:50
//     * @return
//     */
//    public static List<Object> e_meter_event_insert_card(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表时区表编程事件记录
//     * @author wjj
//     * @date 2021/12/10 9:51
//     * @return
//     */
//    public static List<Object> e_meter_event_tzone_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表时段表编程事件记录
//     * @author wjj
//     * @date 2021/12/10 9:58
//     * @return
//     */
//    public static List<Object> e_meter_event_tframe_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表费率参数表编程事件记录
//     * @author wjj
//     * @date 2021/12/10 10:01
//     * @return
//     */
//    public static List<Object> e_meter_event_rate_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表退费记录
//     * @author wjj
//     * @date 2021/12/10 10:03
//     * @return
//     */
//    public static List<Object> e_meter_event_back_fee(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表阶梯表编程事件记录
//     * @author wjj
//     * @date 2021/12/10 10:09
//     * @return
//     */
//    public static List<Object> e_meter_event_stair_pro(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
//        return template5(dataList, terminalArchivesObject, dataSrc);
//    }
//
//    /**
//     * 电能表负荷过载事件记录
//     * @author wjj
//     * @date 2021/12/10 10:24
//     * @return
//     */
//    public static List<Object> e_meter_event_over_load(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRIN
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表电源异常事件记录
//     * @author wjj
//     * @date 2021/12/10 10:27
//     * @return
//     */
//    public static List<Object> e_meter_event_power_abn(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), Double.class));
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表编程事件记录
//     * @author wjj
//     * @date 2021/12/10 10:36
//     * @return
//     */
//    public static List<Object> e_meter_event_program(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,DATA_TAG_1:BIGINT,
//        // DATA_TAG_2:BIGINT,DATA_TAG_3:BIGINT,DATA_TAG_4:BIGINT,DATA_TAG_5:BIGINT,DATA_TAG_6:BIGINT,DATA_TAG_7:BIGINT,
//        // DATA_TAG_8:BIGINT,DATA_TAG_9:BIGINT,DATA_TAG_10:BIGINT,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(6), BigDecimal.class));//DATA_TAG_1
//        finallyList.add(getDataByObj(dataList.get(7), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(8), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(9), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(10), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(11), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(12), BigDecimal.class));//DATA_TAG_7
//        finallyList.add(getDataByObj(dataList.get(13), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(14), BigDecimal.class));
//        finallyList.add(getDataByObj(dataList.get(15), BigDecimal.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//    /**
//     * 电能表负荷开关误动拒动
//     * @author wjj
//     * @date 2021/12/10 10:45
//     * @return
//     */
//    public static List<Object> e_meter_event_switch_err(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
//        List<Object> finallyList=new ArrayList<>();
//        String shardIndex=terminalArchivesObject.getMeterId();
////        String dataDate = dataList.get(1).toString();
//        //用于缓存删除,事件类型用默认值
//        String clearDate="00000000000000";
////        if (dataSrc.equals("0")||dataSrc.equals("10")){
////            clearDate=dataDate;
////        }
//        //占用前2个索引
//        finallyList.add(shardIndex);
//        finallyList.add(clearDate);
//        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,AFTER_STATE:STRING,
//        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,SHARD_NO:STRING
//        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
//        finallyList.add(getDataByObj(dataList.get(2), Date.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
//        finallyList.add(getDataByObj(dataList.get(4), Date.class));
//        finallyList.add(getDataByObj(dataList.get(5), Date.class));
//        finallyList.add(getDataByObj(dataList.get(6), String.class));//AFTER_STATE
//        finallyList.add(getDataByObj(dataList.get(7), Double.class));
//        finallyList.add(getDataByObj(dataList.get(8), Double.class));
//        finallyList.add(getDataByObj(dataList.get(9), Double.class));
//        finallyList.add(getDataByObj(dataList.get(10), Double.class));
//        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
//        return finallyList;
//    }
//
//
//    /**
//     * 通用数据类型转换
//     * @author wjj
//     * @date 2021/11/30 10:36
//     * @return
//     */
//    private static Object getDataByObj(Object obj, Class cl){
//        if (null != obj && StringUtils.isNotBlank(obj.toString())&& !"null".equals(obj.toString())) {
//            if (cl == String.class) {
//                return obj;
//            } else if (cl == Double.class) {
//                return Double.parseDouble(obj.toString());
//            }else if (cl == Float.class) {
//                return Float.parseFloat(obj.toString());
//            } else if (cl == Integer.class) {
//                return Integer.parseInt(obj.toString());
//            } else if (cl == BigDecimal.class) {
//                return new BigDecimal(obj.toString());
//            } else if (cl == Date.class) {//日期类型 参数为Long
//                if (obj instanceof Long) {
//                    return new Date(Long.parseLong(obj.toString()));
//                } else if (obj instanceof String) {
//                    String dateStr = obj.toString();
//                    try {
//                        if (dateStr.indexOf(":") > 0) {
//                            return DateUtil.parse(dateStr.replace("-", ""), DateUtil.getDateTimePattern());
//                        } else {
//                            return DateUtil.parse(dateStr.replace("-", ""), DateUtil.getDatePattern());
//                        }
//                    } catch (ParseException e) {
//                        e.printStackTrace();
//                    }
//                }
//
//            }
//        }
//        return null;
//    }
//}
