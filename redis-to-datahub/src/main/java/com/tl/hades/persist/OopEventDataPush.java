package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author Dong.wei-CHEN
 * Date 2019/12/25 21:03
 * @Descrintion 组装DataList
 */
public class OopEventDataPush {

    private static Map<String, String> eventMap = new HashMap<>(16);

    static {

        //掉电事件
        eventMap.put("03110001", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110002", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110003", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110004", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110005", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110006", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110007", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110008", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("03110009", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("0311000A", "E_MTER_EVENT_NO_POWER_SOURCE");
        eventMap.put("30110200", "E_MTER_EVENT_NO_POWER_SOURCE");
        //开表盖
        eventMap.put("03300D01", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D02", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D03", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D04", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D05", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D06", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D07", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D08", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D09", "E_METER_EVENT_OPEN_LID_SOURCE");
        eventMap.put("03300D0A", "E_METER_EVENT_OPEN_LID_SOURCE");
        //失压
        eventMap.put("1001FF01", "E_METER_EVENT_LOSE_VOL_SOURCE");
        eventMap.put("1002FF01", "E_METER_EVENT_LOSE_VOL_SOURCE");
        eventMap.put("1003FF01", "E_METER_EVENT_LOSE_VOL_SOURCE");
        //欠压
        eventMap.put("1101FF01", "E_METER_EVENT_LOW_VOL_SOURCE");
        eventMap.put("1102FF01", "E_METER_EVENT_LOW_VOL_SOURCE");
        eventMap.put("1103FF01", "E_METER_EVENT_LOW_VOL_SOURCE");
        //过压
        eventMap.put("1201FF01", "E_METER_EVENT_OVER_VOL_SOURCE");
        eventMap.put("1202FF01", "E_METER_EVENT_OVER_VOL_SOURCE");
        eventMap.put("1203FF01", "E_METER_EVENT_OVER_VOL_SOURCE");
        //断相
        eventMap.put("1301FF01", "E_METER_EVENT_VOL_BREAK_SOURCE");
        eventMap.put("1302FF01", "E_METER_EVENT_VOL_BREAK_SOURCE");
        eventMap.put("1303FF01", "E_METER_EVENT_VOL_BREAK_SOURCE");
        //失流
        eventMap.put("1801FF01", "E_METER_EVENT_LOSE_CURR_SOURCE");
        eventMap.put("1802FF01", "E_METER_EVENT_LOSE_CURR_SOURCE");
        eventMap.put("1803FF01", "E_METER_EVENT_LOSE_CURR_SOURCE");
        //过流
        eventMap.put("1901FF01", "E_METER_EVENT_OVER_I_SOURCE");
        eventMap.put("1902FF01", "E_METER_EVENT_OVER_I_SOURCE");
        eventMap.put("1903FF01", "E_METER_EVENT_OVER_I_SOURCE");
        //全失压
        eventMap.put("03050001", "E_METER_EVENT_ALL_VOL_LOSE_SOURCE");
        //校时
        eventMap.put("03300401", "E_METER_EVENT_CHEK_TIME_SOURCE");

    }

    public static List getDataList(List dataList, String... obj) {
        if (dataList == null) {
            return null;
        }
        List<Object> dataListFinal = new ArrayList<>();
        String topicName = eventMap.get(obj[0]);

        try {
            if ("E_MTER_EVENT_NO_POWER_SOURCE".equals(topicName)) {//电表停上电事件召测--645
                if (obj[1] == null) {
                    return null;
                }
                Object obs = obj[1];
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String type="0";
                Date startDate;
                try {
                    startDate = sdf.parse(String.valueOf(dataList.get(0)));
                } catch (ParseException e) {
                    return null;
                }
                Date endDate;
                try{
                    type="1";
                    endDate = sdf.parse(String.valueOf(dataList.get(1)));
                }catch (ParseException e){
                    type="0";
                    endDate=null;
                }
                dataListFinal.add(obs);
                dataListFinal.add(new Date());
                dataListFinal.add(obj[2]);
                dataListFinal.add(type);//停上电类型
                dataListFinal.add(startDate);
                dataListFinal.add(endDate);
                dataListFinal.add(obj[2].substring(0, 5));
                dataListFinal.add(DateUtil.format(startDate, DateUtil.defaultDatePattern_YMD));
            } else if ("E_METER_EVENT_OPEN_LID_SOURCE".equals(topicName)) {//开表盖事件
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date st = sdf.parse(dataList.get(0).toString());
                Date ed = sdf.parse(dataList.get(0).toString());
                dataListFinal.add(obj[1]);//meterId
                dataListFinal.add(new Date());//meterId
                dataListFinal.add(obj[2]);//orgNO
                dataListFinal.add(st);//st
                dataListFinal.add(ed);//ed
                //7-9 终端回复为FFF时仍要入库
                for (int i = 2; i < dataList.size(); i++) {
                    Object oo = dataList.get(i);
                    if (oo == null || String.valueOf(oo).contains("F")) {
                        dataListFinal.add(null);
                        continue;
                    }
                    dataListFinal.add(oo);
                }
                String obs = obj[0].substring(obj[0].length() - 1);
                if ("A".equals(obs)) {
                    dataListFinal.add("10");
                } else {
                    dataListFinal.add(obs);
                }
                dataListFinal.add(null);
                dataListFinal.add(obj[2].substring(0, 5));
                dataListFinal.add(DateUtil.format(st, DateUtil.defaultDatePattern_YMD));
            } else if ("E_METER_EVENT_LOSE_VOL_SOURCE".equals(topicName)
                    || "E_METER_EVENT_LOW_VOL_SOURCE".equals(topicName)
                    || "E_METER_EVENT_OVER_VOL_SOURCE".equals(topicName)
                    || "E_METER_EVENT_VOL_BREAK_SOURCE".equals(topicName)
                    || "E_METER_EVENT_LOSE_CURR_SOURCE".equals(topicName)
                    || "E_METER_EVENT_OVER_I_SOURCE".equals(topicName)) {//ABC失压
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date st = sdf.parse((String) dataList.get(0));

                Date ed;
                int nex = 37;
                if ("E_METER_EVENT_LOSE_CURR_SOURCE".equals(topicName)
                        || "E_METER_EVENT_OVER_I_SOURCE".equals(topicName)) {
                    ed = sdf.parse((String) dataList.get(32));
                    nex = 33;
                } else {
                    ed = sdf.parse((String) dataList.get(36));
                }
                dataListFinal.add(obj[1]);
                dataListFinal.add(new Date());
                dataListFinal.add(obj[2]);
                dataListFinal.add(obj[0].substring(obj[0].length() - 5, obj[0].length() - 4));
                dataListFinal.add(st);//st
                dataListFinal.add(ed);//ed
                for (int i = 1; i < 32; i++) {
                    dataListFinal.add(dataList.get(i));
                }
                for (int i = nex; i < dataList.size(); i++) {
                    dataListFinal.add(dataList.get(i));
                }
                dataListFinal.add(obj[2].substring(0, 5));
                dataListFinal.add(DateUtil.format(ed, DateUtil.defaultDatePattern_YMD));
            } else if ("E_METER_EVENT_ALL_VOL_LOSE_SOURCE".equals(topicName)) {
                dataListFinal.add(obj[1]);//METER_ID:BIGINT:电能表ID
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date st = sdf.parse((String) dataList.get(0));
                Date ed = sdf.parse((String) dataList.get(2));
                dataListFinal.add(new Date());//INPUT_TIME:DATETIME:入库时间，yyyy-mm-ddhh24：mi：ss
                dataListFinal.add(obj[2]);//ORG_NO:VARCHAR:供电单位
                dataListFinal.add(st);//EVENT_ST:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                dataListFinal.add(ed);//EVENT_ET:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                dataListFinal.add(dataList.get(1));//CURR_VAL:DECIMAL:电流值
                dataListFinal.add(obj[2].substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                dataListFinal.add(DateUtil.format(ed, DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
            } else if ("E_METER_EVENT_CHEK_TIME_SOURCE".equals(topicName)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date st = sdf.parse((String) dataList.get(1));
                Date ed = sdf.parse((String) dataList.get(2));
                dataListFinal.add(obj[1]);//METER_ID:BIGINT:电能表ID
                dataListFinal.add(new Date());//INPUT_TIME:DATETIME:入库时间，yyyy-mm-ddhh24：mi：ss
                dataListFinal.add(obj[2]);//ORG_NO:VARCHAR:供电单位
                dataListFinal.add(st);//BEFORE_CHECK_TIME:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                dataListFinal.add(ed);//AFTER_CHECK_TIME:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                dataListFinal.add(dataList.get(0));//OP_CODE:VARCHAR:操作者代码
                dataListFinal.add(obj[2].substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                dataListFinal.add(DateUtil.format(ed, DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
            }
            return dataListFinal;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
