package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/8/19.
 */
public class MeterEventConcat {

    /**
     * 填充不同电表事件的list
     *
     * @param valueList
     * @param dataSign
     * @param realData
     * @throws Exception
     */
    public static void fillData(List<Object> valueList, String dataSign, List<Object> realData,String codeVal) throws Exception {
        String v;//获取list值用
        if (codeVal.equals("E_METER_EVENT_CLEAR_DEMAND")) {//电能表需量清零事件
            int j = 2;
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(realData.get(1));//操作者代码
            while (j < realData.size() - 1) {
                v = realData.get(j).toString();
                if (v.contains("F"))
                    v = v.replaceAll("F", "0");

                valueList.add(Double.valueOf(v));
                valueList.add(EventUtils.getDate(realData, j + 1));
                j += 2;
            }
            if (realData.size() < 50) {
                for (j = 0; j < 50 - realData.size(); ++j)
                    valueList.add(null);
            }
        } else if (codeVal.equals("E_METER_EVENT_OVER_I")) {//电能表过流事件记录
            if (dataSign.equals("1901FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1902FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1903FF01")) {//C相
                valueList.add("3");
            }
            //过流事件发生时刻和结束时刻
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 32));
            for (int i = 1; i < 32; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 33; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_OVER_VOL")) {//电能表过压事件记录
            if (dataSign.equals("1201FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1202FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1203FF01")) {//C相
                valueList.add("3");
            }
            //电能表过压事件发生时刻和结束时刻
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 36));
            for (int i = 1; i < 36; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 37; i < 49; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_ASSI_POWER_SHUT")) {//电能表辅助电源掉电事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 1));//事件结束时刻
        } else if (codeVal.equals("E_METER_EVENT_TRIP")) {//电能表跳闸事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(realData.get(1));//操作者代码
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
            if (valueList.size() == 5) {
                for (int i = 0; i < 6; i++) {
                    valueList.add(null);
                }
            }
        } else if (codeVal.equals("E_METER_EVENT_OVER_LOAD")) {//电能表负荷过载事件记录
            if (dataSign.equals("1C01FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1C02FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1C03FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 17));
        } else if (codeVal.equals("E_METER_EVENT_SWITCH_ERR")) {//电能表负荷开关误动拒动记录
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 1));
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_PROGRAM")) {//电能表编程事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(realData.get(1));//操作者代码
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                if (v.contains("F"))
                    v = v.replaceAll("F", "0");
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_POWER_ABN")) {//电能表电源异常事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 1));//事件发生时刻
            for (int i = 2; i < 4; i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_REVE_PHASE")) {//电能表电压逆相序事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 17));//事件发生时刻
            for (int i = 1; i < 17; i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
            for (int i = 18; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_I_REVE_PHASE")) {//电能表电流逆相序事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 17));//事件发生时刻
        } else if (codeVal.equals("E_METER_EVENT_VOL_UNBALANCE")) {//电能表电压不平衡事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 18));//事件发生时刻
            valueList.add(realData.get(17));
            for (int i = 1; i <= 16; i++) {
                valueList.add(realData.get(i));
                if (i == 8 || i == 12 || i == 16) {
                    for (int j = 0; j < 5; j++) {
                        valueList.add(null);
                    }
                }
            }
            for (int i = 19; i < realData.size(); i++) {
                valueList.add(realData.get(i));
            }
        } else if (codeVal.equals("E_METER_EVENT_CURR_RETURN")) {//电能表潮流反向事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            for (int i = 1; i < 17; i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_CLEAR")) {//电能表清零事件记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(realData.get(1));//操作者代码
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                if (v.contains("F"))
                    v.replaceAll("F", "0");
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_PAP_DEMD_OVER_LI")) {//电能表正向有功需量越限记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 1));//事件结束时刻
        } else if (codeVal.equals("E_METER_EVENT_LOW_VOL")) {//电能表欠压事件记录
            if (dataSign.equals("1101FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1102FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1103FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 36));
            for (int i = 1; i < 36; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 37; i < 49; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_CHEK_TIME")) {//电能表校时事件记录
            valueList.add(EventUtils.getDate(realData, 1));
            valueList.add(EventUtils.getDate(realData, 2));
            //操作者代码
            valueList.add(realData.get(0));
        } else if (codeVal.equals("E_METER_EVENT_P_RETURN")) {//电能表有功功率反向事件记录
            if (dataSign.equals("1B01FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1B02FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1B03FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 17));
            for (int i = 1; i < 17; i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
            for (int i = 18; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_RQ_DEMD_OVER_LIM")) {//电能表无功需量越限记录
            valueList.add(EventUtils.getDate(realData, 0));//事件发生时刻
            valueList.add(EventUtils.getDate(realData, 1));//事件结束时刻
        } else if (codeVal.equals("E_METER_EVENT_VOL_BREAK")) {//电能表断相事件记录
            if (dataSign.equals("1301FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1302FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1303FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 36));
            for (int i = 1; i < 36; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 37; i < 49; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_NO_CURR")) {//电能表断流事件记录
            if (dataSign.equals("1A01FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1A02FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1A03FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 32));
            for (int i = 1; i < 32; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 33; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_MAG_INTERF")) {//电能表恒定磁场干扰事件记录
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 1));
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(Double.valueOf(v));
            }
        } else if (codeVal.equals("E_METER_EVENT_OPEN_LID")) {//电能表开表盖事件记录
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 1));
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                if (v.contains("F"))
                    v = v.replaceAll("F", "0");
                valueList.add(Double.valueOf(v));
            }
            valueList.add(0);
            valueList.add(null);
        } else if (codeVal.equals("E_METER_EVENT_OPEN_TERM_LID")) {//电能表开端钮盖事件记录
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 1));
            for (int i = 2; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_LOSE_CURR")) {//电能表失流事件记录
            if (dataSign.equals("1801FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1802FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1803FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 32));
            for (int i = 1; i < 32; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 33; i < realData.size(); i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_LOSE_VOL")) {//电能表失压事件记录
            if (dataSign.equals("1001FF01")) {//A相
                valueList.add("1");
            } else if (dataSign.equals("1002FF01")) {//B相
                valueList.add("2");
            } else if (dataSign.equals("1003FF01")) {//C相
                valueList.add("3");
            }
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(EventUtils.getDate(realData, 36));
            for (int i = 1; i < 36; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
            for (int i = 37; i < 49; i++) {
                v = realData.get(i).toString();
                valueList.add(v);
            }
        } else if (codeVal.equals("E_METER_EVENT_CLEAR_EVENT")) {//电能表事件清零
            valueList.add(null);//采集时间
            valueList.add(new Date());
            valueList.add(EventUtils.getDate(realData, 0));
            valueList.add(realData.get(1));
            valueList.add(realData.get(2));
        } else if (codeVal.equals("E_MTER_EVENT_NO_POWER")) {//电能表停电事件  无数据项ID
            Date stTime = EventUtils.getDate(realData, 0);
            Date endTime = EventUtils.getDate(realData, 1);
            String dataType = "0";
            if (stTime == null) {
                valueList = null;
            }
            if (DateFilter.isLateWeek(stTime, -7)) {
                valueList = null;
            }
            if (endTime != null) {
                dataType = "1";
            }
            valueList.add(dataType);
            valueList.add(stTime);
            valueList.add(endTime);
        } else if (codeVal.equals("E_METER_EVENT_ALL_VOL_LOSE")) {//电能表全失压事件
            if (realData.get(0) != null) {
                valueList.add(EventUtils.getDate(realData, 0));
            } else {
                valueList.add(null);
            }
            if (realData.get(2) != null) {
                valueList.add(EventUtils.getDate(realData, 2));
            } else {
                valueList.add(null);
            }
            valueList.add(realData.get(1));
        } else if (codeVal.equals("E_METER_EVENT_FACT_LOWER_LIMIT")) {//电能表功率因数越下限事件记录
            valueList.add(null);//采集时间
            if (realData.get(0) != null) {
                valueList.add(EventUtils.getDate(realData, 0));
            } else {
                valueList.add(null);
            }
            if (realData.get(5) != null) {
                valueList.add(EventUtils.getDate(realData, 5));
            } else {
                valueList.add(null);
            }
        } else if (codeVal.equals("E_METER_EVENT_SWITCH_ON")) {//电能表合闸事件
            valueList.add(null);//采集时间
            if (realData.get(0) != null) {
                valueList.add(EventUtils.getDate(realData, 0));
            } else {
                valueList.add(null);
            }
            valueList.add(realData.get(1));
            for (int i = 2; i < 8; i++) {
                valueList.add(realData.get(i));
            }
        } else {//表结构一样的只有五个字段的
            //事件发生时刻和结束时刻
            if (realData.get(0) != null) {
                valueList.add(EventUtils.getDate(realData, 0));
            } else {
                valueList.add(null);
            }
            if (realData.size() > 1 && realData.get(1) != null) {
                valueList.add(EventUtils.getDate(realData, 1));
            } else {
                valueList.add(null);
            }
        }
    }

    /**
     * 格式化comm
     *
     * @param comm
     * @return
     */
    public static String reverseComm(String comm) {
        String newComm = "";
        if (comm == null)
            return "0";

        if (comm.length() < 12)
            comm = comm + "000000000000".substring(0, 12 - comm.length());

        newComm = comm.substring(comm.length() - 2, comm.length())
                + comm.substring(comm.length() - 4, comm.length() - 2)
                + comm.substring(comm.length() - 6, comm.length() - 4)
                + comm.substring(comm.length() - 8, comm.length() - 6)
                + comm.substring(comm.length() - 10, comm.length() - 8)
                + comm.substring(comm.length() - 12, comm.length() - 10);
        return newComm;
    }
}
