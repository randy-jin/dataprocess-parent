package com.tl.dataprocess.ConversionUtil;

import com.tl.archives.TerminalArchivesObject;
import com.tl.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wangjunjie
 * @date 2021/12/22 15:03
 */
public class ProcessUtil {

    /**
     * 通用数据类型转换
     * @author wjj
     * @date 2021/11/30 10:36
     * @return
     */
    public static Object getDataByObj(Object obj, Class cl){
        if (null != obj && StringUtils.isNotBlank(obj.toString())&& !"null".equals(obj.toString())) {
            if (cl == String.class) {
                return obj;
            } else if (cl == Double.class) {
                return Double.parseDouble(obj.toString());
            }else if (cl == Float.class) {
                return Float.parseFloat(obj.toString());
            } else if (cl == Integer.class) {
                return Integer.parseInt(obj.toString());
            } else if (cl == BigDecimal.class) {
                return new BigDecimal(obj.toString());
            } else if (cl == Date.class) {//日期类型 参数为Long
                if (obj instanceof Long) {
                    return new Date(Long.parseLong(obj.toString()));
                } else if (obj instanceof String) {
                    String dateStr = obj.toString();
                    try {
                        if (dateStr.indexOf(":") > 0) {
                            return DateUtil.parse(dateStr.replace("-", ""), DateUtil.getDateTimePattern());
                        } else {
                            return DateUtil.parse(dateStr.replace("-", ""), DateUtil.getDatePattern());
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        return null;
    }

    /**
     * 状态字1 状态字3
     * @author wjj
     * @date 2021/12/22 16:04
     * @return
     */
    public static List<Object> template1(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //mped_id:BIGINT
        //data_date:STRING
        //org_no:STRING
        //status:STRING
        //shard_no:STRING
        //insert_time:TIMESTAMP
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(1));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(dataList.get(3));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));
        finallyList.add(new Date());
        return finallyList;
    }

    /**
     * 内部时钟电池电压 透支金额
     * @author wjj
     * @date 2021/12/22 16:05
     * @return
     */
    public static List<Object> template2(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
//        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,BATTERY_VOLTAGE:DOUBLE,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(1));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(getDataByObj(dataList.get(3), Double.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        finallyList.add(new Date());
        return finallyList;
    }

    /**
     * 电流 总功率因数 瞬时有功功率 电压
     * @author wjj
     * @date 2021/12/22 16:06
     * @return
     */
    public static List<Object> template3(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);

        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(1));
        finallyList.add(dataList.get(2));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(getDataByObj(dataList.get(4), Double.class));
        finallyList.add(new Date());
        finallyList.add("00");
        finallyList.add(dataSrc);
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }

    /**
     * (上1结算日)正向有功总最大需量及发生时间 月需量
     * @author wjj
     * @date 2021/12/22 16:03
     * @return
     */
    public static List<Object> template4(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc) {
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //ID:BIGINT,DATA_TYPE:STRING,COL_TIME:TIMESTAMP,DEMAND:DOUBLE,DEMAND_TIME:TIMESTAMP,DEMAND1:DOUBLE,
        // DEMAND_TIME1:TIMESTAMP,DEMAND2:DOUBLE,DEMAND_TIME2:TIMESTAMP,DEMAND3:DOUBLE,DEMAND_TIME3:TIMESTAMP,
        // DEMAND4:DOUBLE,DEMAND_TIME4:TIMESTAMP,ORG_NO:STRING,STATUS:STRING,DATA_SRC:STRING,
        // INSERT_TIME:TIMESTAMP,SHARD_NO:STRING,DATA_DATE:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(2));

        Object callObj=dataList.get(3);//抄表时间处理
        Date callDate=new Date();
        if(callObj!=null){
            callDate=new Date(Long.parseLong(callObj.toString()));
        }
        finallyList.add(callDate);
        finallyList.add(getDataByObj(dataList.get(4), Double.class));
        finallyList.add(getDataByObj(dataList.get(5), Date.class));
        finallyList.add(getDataByObj(dataList.get(6), Double.class));//DEMAND1
        finallyList.add(getDataByObj(dataList.get(7), Date.class));
        finallyList.add(getDataByObj(dataList.get(8), Double.class));
        finallyList.add(getDataByObj(dataList.get(9), Date.class));
        finallyList.add(getDataByObj(dataList.get(10), Double.class));
        finallyList.add(getDataByObj(dataList.get(11), Date.class));//DEMAND_TIME3
        finallyList.add(getDataByObj(dataList.get(12), Double.class));
        finallyList.add(getDataByObj(dataList.get(13), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add("00");
        finallyList.add(dataSrc);
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        finallyList.add(dataList.get(1));
        return finallyList;
    }

    /**
     * 电能表有功组合方式编程事件记录
     * 电能表无功组合方式1/2编程事件记录
     * 电能表结算日编程事件记录
     * 电能表正有功需量越限记录
     * 电能表反向有功需量越限记录
     * @author wjj
     * @date 2021/12/22 16:36
     * @return
     */
    public static List<Object> template5(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
        String dataDate = dataList.remove(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(getDataByObj(dataList.get(1), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(getDataByObj(dataList.get(3), Date.class));
        finallyList.add(getDataByObj(dataList.get(4), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }

    /**
     * 电能表失流事件
     * 电能表过流事件记录
     * 电能表失压事件
     * 电能表欠压事件记录
     * 电能表过压事件记录
     * 电能表断相事件记录
     * @author wjj
     * @date 2021/12/22 16:45
     * @return
     */
    public static List<Object> template6(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
        String dataDate = dataList.remove(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(getDataByObj(dataList.get(3), BigDecimal.class));
        finallyList.add(getDataByObj(dataList.get(4), Date.class));
        finallyList.add(getDataByObj(dataList.get(5), Date.class));//EVENT_ET
        finallyList.add(getDataByObj(dataList.get(6), Double.class));
        finallyList.add(getDataByObj(dataList.get(7), Double.class));
        finallyList.add(getDataByObj(dataList.get(8), Double.class));
        finallyList.add(getDataByObj(dataList.get(9), Double.class));
        finallyList.add(getDataByObj(dataList.get(10), Double.class));
        finallyList.add(getDataByObj(dataList.get(11), Double.class));//ST_RAP_R_A
        finallyList.add(getDataByObj(dataList.get(12), Double.class));
        finallyList.add(getDataByObj(dataList.get(13), Double.class));
        finallyList.add(getDataByObj(dataList.get(14), Double.class));
        finallyList.add(getDataByObj(dataList.get(15), Double.class));
        finallyList.add(getDataByObj(dataList.get(16), Double.class));
        finallyList.add(getDataByObj(dataList.get(17), Double.class));//ST_Q_A
        finallyList.add(getDataByObj(dataList.get(18), Double.class));
        finallyList.add(getDataByObj(dataList.get(19), Double.class));
        finallyList.add(getDataByObj(dataList.get(20), Double.class));
        finallyList.add(getDataByObj(dataList.get(21), Double.class));
        finallyList.add(getDataByObj(dataList.get(22), Double.class));
        finallyList.add(getDataByObj(dataList.get(23), Double.class));//ST_U_B
        finallyList.add(getDataByObj(dataList.get(24), Double.class));
        finallyList.add(getDataByObj(dataList.get(25), Double.class));
        finallyList.add(getDataByObj(dataList.get(26), Double.class));
        finallyList.add(getDataByObj(dataList.get(27), Double.class));
        finallyList.add(getDataByObj(dataList.get(28), Double.class));
        finallyList.add(getDataByObj(dataList.get(29), Double.class));//ST_RAP_R_C
        finallyList.add(getDataByObj(dataList.get(30), Double.class));
        finallyList.add(getDataByObj(dataList.get(31), Double.class));
        finallyList.add(getDataByObj(dataList.get(32), Double.class));
        finallyList.add(getDataByObj(dataList.get(33), Double.class));
        finallyList.add(getDataByObj(dataList.get(34), Double.class));
        finallyList.add(getDataByObj(dataList.get(35), Double.class));//ST_Q_C
        finallyList.add(getDataByObj(dataList.get(36), Double.class));
        finallyList.add(getDataByObj(dataList.get(37), Double.class));
        finallyList.add(getDataByObj(dataList.get(38), Double.class));
        finallyList.add(getDataByObj(dataList.get(39), Double.class));
        finallyList.add(getDataByObj(dataList.get(40), Double.class));
        finallyList.add(getDataByObj(dataList.get(41), Double.class));//EN_PAP_R_A
        finallyList.add(getDataByObj(dataList.get(42), Double.class));
        finallyList.add(getDataByObj(dataList.get(43), Double.class));
        finallyList.add(getDataByObj(dataList.get(44), Double.class));
        finallyList.add(getDataByObj(dataList.get(45), Double.class));
        finallyList.add(getDataByObj(dataList.get(46), Double.class));//EN_RAP_R_B
        finallyList.add(getDataByObj(dataList.get(47), Double.class));
        finallyList.add(getDataByObj(dataList.get(48), Double.class));
        finallyList.add(getDataByObj(dataList.get(49), Double.class));
        finallyList.add(getDataByObj(dataList.get(50), Double.class));
        finallyList.add(getDataByObj(dataList.get(51), Double.class));//EN_GRP_1_R_C
        finallyList.add(getDataByObj(dataList.get(52), Double.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }

    /**
     * 电能表电流逆相序事件记录
     * 电能表电流严重不平衡事件记录
     * 电能表电流不平衡事件记录
     * 电能表辅助电源掉电事件记录
     * 电能表周休日编程事件记录
     * 电能表密钥更新事件记录
     * 电能表异常插卡事件记录
     * 电能表时区表编程事件记录
     * 电能表时段表编程事件记录
     * 电能表费率参数表编程事件记录
     * 电能表退费记录
     * 电能表阶梯表编程事件记录
     * @author wjj
     * @date 2021/12/22 17:00
     * @return
     */
    public static List<Object> template7(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.get(1).toString();
        //用于缓存删除,事件类型用默认值
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(getDataByObj(dataList.get(2), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(getDataByObj(dataList.get(4), Date.class));
        finallyList.add(getDataByObj(dataList.get(5), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
