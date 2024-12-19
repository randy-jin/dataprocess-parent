package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;
import com.tl.promethues.PromethuesAllDayUtil;

import java.util.*;

/**
 * @author Dongwei-Chen
 * @Date 2020/1/7 9:39
 * @Description
 */
public class VoluntrayDataUtils {
    public static List<Integer> FN_LIST = new ArrayList<>();

    private static Integer[] curveArray = {92, 93, 94, 95, 89, 90, 91, 73, 74, 75, 76, 138, 105, 106, 107, 108, 81, 82, 83, 84, 85, 86, 87, 88, 101, 102, 103, 104, 145, 146, 147, 148, 149, 150, 151, 152, 97, 98, 99, 100};
    /**
     * 主动上报曲线
     */
    public static List<Integer> curveList = new ArrayList<Integer>() {{
        addAll(Arrays.asList(curveArray));
    }};

    static {
        FN_LIST.addAll(new ArrayList<Integer>() {
            {
                add(250);
                add(251);
                add(252);
                add(37);
                add(35);
                add(36);
                add(44);
                add(153);
                add(154);
                add(155);
                add(156);
                add(28);
                add(246);
                add(210);
            }
        });
    }

    @SuppressWarnings("rawtypes")
    public static boolean allNull(List list) { //传入了当前pnfn测量点的数据项列表list
        boolean allNull = true;
        for (Object o : list) {
            if (o instanceof List) {
                allNull((List) o);
            } else {
                if (o != null && !(o instanceof Date)
                        && !(o instanceof Integer)) {
                    allNull = false;
                    return allNull;
                }
            }
        }
        return allNull;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List getDataList(List dataList, int afn, int fn, String mpedId) { //fnpn测量点的数据项list，
        List newDataList = null;

        if (afn == 13) {
            if (fn >= 161 && fn <= 168) {
                newDataList = new ArrayList();
                newDataList.add(1);//从缓存取测量点标识，这里占位
                if (fn == 161)
                    newDataList.add("1");//1正向有功
                else if (fn == 162)
                    newDataList.add("2");//正向无功
                else if (fn == 165)
                    newDataList.add("3");//一象限无功
                else if (fn == 168)
                    newDataList.add("4");//四象限无功
                else if (fn == 163)
                    newDataList.add("5");//反向有功
                else if (fn == 164)
                    newDataList.add("6");//反向无功
                else if (fn == 166)
                    newDataList.add("7");//二象限无功
                else if (fn == 167)
                    newDataList.add("8");//三象限无功
                else
                    newDataList.add("0");//备用

                int m = (Integer) dataList.remove(2);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(3)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                Object colDate = dataList.get(1);
                if (null != colDate && colDate instanceof Date) {
                    newDataList.add(colDate);
                } else {
                    newDataList.add(null);
                }
                newDataList.add(dataList.get(2)); //正向有功总电能示值
                newDataList.addAll(list);//4个值的list，一次性添加
                for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.add(1, mpedId);//首元素id
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    Date readDate = (Date) dataDate;
                    newDataList.add(DateUtil.format(readDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                    if (DateFilter.isLateWeek(readDate, -45)) {
                        return null;
                    }
                } else {
                    return null;
                }
                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
            } else if (fn >= 5 && fn <= 8) {
                newDataList = new ArrayList();
                newDataList.add(1);
                if (fn == 5)
                    newDataList.add("1");//1正向有功
                else if (fn == 6)
                    newDataList.add("2");//正向无功
                else if (fn == 7)
                    newDataList.add("5");//反向有功
                else if (fn == 8)
                    newDataList.add("6");//反向无功
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                newDataList.add(dataDate);//这个时间是个填充值，后面要删掉的，只是站位
                newDataList.add(dataList.get(1)); //日正向有功总电能示值
                //newDataList.addAll(list);//需要添加4个值，若为空，就没有添加值了，
                newDataList.addAll(list);
                for (int i = 0; i < 4 - list.size(); i++) {
                    newDataList.add(null);
                }
                for (int j = m; j < 14; j++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.set(0, mpedId);

                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if ((fn >= 185 && fn <= 188) || (fn >= 193 && fn <= 196)) {//需量+时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                if (fn == 185 || fn == 193) {
                    newDataList.add("1");
                } else if (fn == 186 || fn == 194) {
                    newDataList.add("2");
                } else if (fn == 187 || fn == 195) {
                    newDataList.add("5");
                } else if (fn == 188 || fn == 196) {
                    newDataList.add("6");
                }

                newDataList.add(dataList.get(1));//

                Object v_obj=dataList.get(3);
                if(v_obj==null){
                    return null;
                }
                newDataList.add(v_obj);
                Calendar calDate = Calendar.getInstance();
                Calendar calDataDate = Calendar.getInstance();
                Object obj4 = dataList.get(4);
                calDataDate.setTime((Date) dataList.get(0));
                if (obj4 != null) {
                    calDate.setTime((Date) obj4);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    obj4 = calDate.getTime();
                }
//                if (!(obj4 instanceof Date)) {
//                    return null;
//                }
                newDataList.add(obj4);
                List dt = (List) ((List) dataList.get(5)).get(0);
                for (int i = 0; i < dt.size(); i++) {
                    Object o = dt.get(i);
                    if (i % 2 != 0) {
                        if(o!=null){
                            try{
                                Calendar cel = Calendar.getInstance();
                                Date time = (Date) o;
                                cel.setTime(time);
                                cel.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                                newDataList.add(cel.getTime());
                            }catch (Exception e){
                                newDataList.add(null);
                            }
                        }else{
                            newDataList.add(null);
                        }
                    } else {
                        newDataList.add(o);
                    }
                }
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }

            } else if (fn == 43) {//日功率因数区段累计时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                for (int i = 1; i < 4; i++) {
                    newDataList.add(dataList.get(i));
                }
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn == 19 || fn == 20 || fn == 4 || fn == 3) {//正/反向有功/无功需量
                List<List> dt = new ArrayList<>();
                int fNum = 4;
                int tNum = 6;
                String type = "1";
                if (fn == 20 || fn == 4) {
                    type = "5";
                }
                for (int j = 0; j < 2; j++) {
                    newDataList = new ArrayList();
                    Date dataDate = (Date) dataList.get(0);

                    newDataList.add(mpedId);
                    newDataList.add(type);
                    newDataList.add(dataDate);
                    newDataList.add(dataList.get(fNum - 1));
                    newDataList.add(dataList.get(tNum - 1));
                    List fee = (List) ((List) dataList.get(fNum)).get(0);
                    List time = (List) ((List) dataList.get(tNum)).get(0);
                    for (int i = 0; i < fee.size(); i++) {
                        newDataList.add(fee.get(i));
                        newDataList.add(time.get(i));
                    }
                    newDataList.add(DateUtil.format(dataDate, DateUtil.defaultDatePattern_YMD));
                    fNum = 8;
                    tNum = 10;
                    type = "2";
                    if (fn == 20 || fn == 4) {
                        type = "6";
                    }
                    dt.add(newDataList);
                }
                return dt;

            } else if (fn == 25) {//日总及分相最大有功功率及发生时间、有功功率为零时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);

                Calendar calDate = Calendar.getInstance();
                Calendar calDataDate = Calendar.getInstance();
                calDataDate.setTime((Date) dataList.get(0));
                Object obj;

                newDataList.add(dataList.get(1));
                obj = dataList.get(2);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                newDataList.add(obj);
                //				newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
                obj = dataList.get(4);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                newDataList.add(obj);
                newDataList.add(dataList.get(5));
                obj = dataList.get(6);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                newDataList.add(obj);
                //				newDataList.add(dataList.get(6));
                newDataList.add(dataList.get(7));
                obj = dataList.get(8);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                } else {
                    obj = null;
                }
                newDataList.add(obj);
                //				newDataList.add(dataList.get(8));
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn == 29) {//日电流越限统计
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                for (int i = 1; i < 9; i++) {
                    newDataList.add(dataList.get(i));
                }
                Calendar calDate = Calendar.getInstance();
                Calendar calDataDate = Calendar.getInstance();
                calDataDate.setTime((Date) dataList.get(0));
                Object obj = new Object();
                obj = dataList.get(9);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                newDataList.add(obj);
                newDataList.add(dataList.get(10));
                obj = dataList.get(11);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                newDataList.add(obj);
                newDataList.add(dataList.get(12));
                obj = dataList.get(13);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                newDataList.add(obj);
                newDataList.add(dataList.get(14));
                obj = dataList.get(15);
                if (obj != null) {
                    calDate.setTime((Date) obj);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    calDate.set(Calendar.MONTH, calDataDate.get(Calendar.MONTH));
                    obj = calDate.getTime();
                }
                newDataList.add(obj);
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn >= 177 && fn <= 184) {//********************以下是月冻结判断******************************
                newDataList = new ArrayList();
                newDataList.add(1);//从缓存取测量点标识，这里占位
                if (fn == 177)
                    newDataList.add("1");//1正向有功
                else if (fn == 178)
                    newDataList.add("2");//正向无功
                else if (fn == 181)
                    newDataList.add("3");//一象限无功
                else if (fn == 184)
                    newDataList.add("4");//四象限无功
                else if (fn == 179)
                    newDataList.add("5");//反向有功
                else if (fn == 180)
                    newDataList.add("6");//反向无功
                else if (fn == 182)
                    newDataList.add("7");//二象限无功
                else if (fn == 183)
                    newDataList.add("8");//三象限无功
                else
                    newDataList.add("0");//备用

                int m = (Integer) dataList.remove(2);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(3)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                Object colDate = dataList.get(1);
                if (null != colDate && colDate instanceof Date) {
                    newDataList.add(colDate);
                } else {
                    newDataList.add(null);
                }
                newDataList.add(dataList.get(2)); //正向有功总电能示值
                newDataList.addAll(list);//4个值的list，一次性添加
                for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.add(1, mpedId);//首元素id
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                    //					newDataList.add(DateUtil.format(DateUtil.addDaysOfMonth((Date)dataDate, -1), DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
            } else if (fn >= 21 && fn <= 24) {//yue
                newDataList = new ArrayList();
                newDataList.add(1);//从缓存取测量点标识，这里占位
                if (fn == 21)
                    newDataList.add("1");//1正向有功
                else if (fn == 22)
                    newDataList.add("2");//正向无功
                else if (fn == 23)
                    newDataList.add("5");//一象限无功
                else if (fn == 24)
                    newDataList.add("6");//四象限无功
                else
                    newDataList.add("0");//备用

                int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                newDataList.add(null);//这个时间是个填充值，后面要删掉的，只是站位
                newDataList.add(dataList.get(1)); //日正向有功总电能示值
                //newDataList.addAll(list);//需要添加4个值，若为空，就没有添加值了，
                if (list.size() < 4) {
                    newDataList.addAll(list);
                    for (int i = 0; i < 4 - list.size(); i++) {
                        newDataList.add(null);
                    }
                }
                for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.set(0, mpedId);
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                    //					newDataList.add(DateUtil.format(DateUtil.addDaysOfMonth((Date)dataDate, -1), DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn == 44) {//yue功率因数区段累计时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
                for (int i = 1; i < 4; i++) {
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 33) {//yue总及分相最大有功功率及发生时间、有功功率为零时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                for (int i = 1; i < 9; i++) {
                    newDataList.add(dataList.get(i));
                }
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn == 37) {//yue电流越限统计
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
                for (int i = 1; i < 16; i++) {
                    newDataList.add(dataList.get(i));

                }
            } else if (fn == 27) {//日电压统计记录
                newDataList = new ArrayList();
                Calendar calendar = Calendar.getInstance();
                Calendar calDate = Calendar.getInstance();
                calDate.setTime((Date) dataList.get(0));

                newDataList.add(mpedId);
                for (int i = 1; i < 17; i++) {
                    newDataList.add(dataList.get(i));
                }
                if (dataList.get(17) != null) {
                    calendar.setTime((Date) dataList.get(17));//2018-01-01 //2018-06-06
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(17));
                }
                newDataList.add(dataList.get(18));
                if (dataList.get(19) != null) {
                    calendar.setTime((Date) dataList.get(19));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(19));
                }
                newDataList.add(dataList.get(20));
                if (dataList.get(21) != null) {
                    calendar.setTime((Date) dataList.get(21));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(21));
                }
                newDataList.add(dataList.get(22));
                if (dataList.get(23) != null) {
                    calendar.setTime((Date) dataList.get(23));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(23));
                }
                newDataList.add(dataList.get(24));
                if (dataList.get(25) != null) {
                    calendar.setTime((Date) dataList.get(25));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(25));
                }
                newDataList.add(dataList.get(26));
                if (dataList.get(27) != null) {
                    calendar.setTime((Date) dataList.get(27));
                    calendar.set(Calendar.YEAR, calDate.get(Calendar.YEAR));
                    calendar.set(Calendar.MONTH, calDate.get(Calendar.MONTH));
                    newDataList.add(calendar.getTime());
                } else {
                    newDataList.add(dataList.get(27));
                }
                newDataList.add(dataList.get(28));
                newDataList.add(dataList.get(29));
                newDataList.add(dataList.get(30));
                if (dataList.size() < 32) {
                    for (int i = 0; i < 9; i++) {
                        newDataList.add(null);
                    }
                } else {
                    newDataList.add(dataList.get(31));//a
                    newDataList.add(dataList.get(34));
                    newDataList.add(dataList.get(37));

                    newDataList.add(dataList.get(32));//b
                    newDataList.add(dataList.get(35));
                    newDataList.add(dataList.get(38));

                    newDataList.add(dataList.get(33));//c
                    newDataList.add(dataList.get(36));
                    newDataList.add(dataList.get(39));
                }

                // 把数据时标放在最后，方便外层处理
                Object dataDate = dataList.get(0);
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format((Date) dataDate, DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)
                }
            } else if (fn == 250 || fn == 251 || fn == 252) {//固定时间点单相电压
                newDataList = new ArrayList<>();
                newDataList.add(mpedId);//测量点标示
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据日期
                newDataList.add("");//ORG_NO
                if (fn == 250) {
                    for (int i = 1; i < 5; i++) {
                        newDataList.add(dataList.get(i));
                    }
                } else if (fn == 251) {
                    int m = 1;
                    int n = 13;
                    for (int j = 0; j < 4; j++) {//A、B、C三相电压电流
                        newDataList.add(dataList.get(m));
                        newDataList.add(dataList.get(m + 1));
                        newDataList.add(dataList.get(m + 2));
                        m = m + 3;
                        newDataList.add(dataList.get(n));
                        newDataList.add(dataList.get(n + 1));
                        newDataList.add(dataList.get(n + 2));
                        newDataList.add(dataList.get(n + 3));
                        n = n + 4;
                    }
                } else if (fn == 252) {
                    int m = 1;
                    int n = 2;
                    for (int j = 0; j < 4; j++) {//A、B、C三相电压电流
                        newDataList.add(dataList.get(m));
                        m = m + 2;
                    }
                    for (int u = 0; u < 4; u++) {
                        newDataList.add(dataList.get(n));
                        n = n + 2;
                    }
                }
            } else if (fn == 35) {// 月电压统计
                newDataList = new ArrayList<>();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                for (int i = 1; i < dataList.size(); i++) {
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 36) {
                newDataList = new ArrayList<>();
                newDataList.add(mpedId);
                newDataList.add(dataList.get(0));
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
            } else if (fn >= 153 && fn <= 156) {//分相示值
                newDataList = new ArrayList<>();
                String type = "";
                boolean allNull = true;
                newDataList.add(mpedId);
                if (fn == 153) {//日冻结分相正相有功电能示值
                    type = "1";
                } else if (fn == 154) {
                    type = "2";
                } else if (fn == 155) {
                    type = "5";
                } else if (fn == 156) {
                    type = "6";
                }
                newDataList.add(type);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                newDataList.add(dataList.get(1));
                for (int i = 2; i < dataList.size(); i++) {
                    if (dataList.get(i) instanceof Double) {
                        allNull = false;
                        newDataList.add(dataList.get(i));
                    } else {
                        newDataList.add(null);
                    }
                }
                if (allNull) {
                    return null;
                }
            } else if (fn == 28) {//日不平衡度越限累计时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);//mpedid
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                for (int i = 1; i < dataList.size(); i++) {
                    //电流不平衡度越限累计时间
                    //电流不平衡最大值
                    //电流不平衡最大值发生时间
                    //电压不平衡最大值
                    //电压不平衡最大值发生时间
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 246) {//日冻结掉电记录数据
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(null);//终端id占位
                String obj = (dataList.get(1)).toString();
                if (obj.contains("E")) {
                    newDataList.add(null);
                } else {
                    newDataList.add(obj);
                }
                for (int i = 2; i < dataList.size(); i++) {
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 210) {//电能表购、用电信息
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(dataList.get(1));//抄表日期
                newDataList.add(null);//orgNo占位
                newDataList.add(dataList.get(5));//剩余电量
                newDataList.add(dataList.get(3));//剩余金额
                newDataList.add(dataList.get(9));//报警电量
                newDataList.add(dataList.get(10));//故障电量
                newDataList.add(dataList.get(7));//累计购电量
                newDataList.add(dataList.get(4));//累计购电金额
                newDataList.add(((Double) dataList.get(2)).intValue());//购电次数
                newDataList.add(dataList.get(8));//赊欠门限值
                newDataList.add(dataList.get(6));//透支电量
            } else if (fn == 26 || fn == 34) {//日/月总及分相最大需量及发生时间
                newDataList = new ArrayList();
                Date dataDate = (Date) dataList.get(0);
                newDataList.add(mpedId);
                newDataList.add("0");
                newDataList.add(dataDate);
                for (int i = 1; i < dataList.size(); i++) {
                    Object d = dataList.get(i);
                    if (fn == 34) {
                        if (d instanceof Date) {
                            newDataList.add(LastMonth(d, dataDate));
                        } else {
                            newDataList.add(d);
                        }
                    } else {
                        newDataList.add(d);
                    }
                }
                newDataList.add(null);
                newDataList.add(null);
                newDataList.add(DateUtil.format(dataDate, DateUtil.defaultDatePattern_YMD));
                //正向有/无功电能示值、一/四象限无功电能示值（总、费率1～M，1≤M≤12）
            } else if (fn == 1) {
                List<List> resultList = new ArrayList<>();
                int num = 3;
                for (int i = 0; i < 4; i++) {
                    newDataList = new ArrayList();
                    Object objDate=dataList.get(0);
                    if(objDate==""||objDate==null){
                        continue;
                    }
                    String dataDate = DateUtil.format((Date) objDate, DateUtil.defaultDatePattern_YMD);
                    newDataList.add(mpedId + "_" + dataDate);
                    newDataList.add(mpedId);//ID:BIGINT:测量点标识（MPED_ID）
                    newDataList.add(String.valueOf(i + 1));//DATA_TYPE:VARCHAR:数据类型(0：备用、1：正向有功、2：正向无功、3：一象限无功、4：四象限无功、5：反向有功、6：反向无功、7：二象限无功、8：三象限无功)
                    newDataList.add(dataList.get(1));//COL_TIME:DATETIME:终端抄表时间
                    newDataList.add(dataList.get(num));//R:DECIMAL:总电能示值
                    List dl = (List) ((List) dataList.get(num + 1)).get(0);
                    for (int j = 0; j < 14; j++) {
                        if (j < dl.size()) {
                            newDataList.add(dl.get(j));
                        } else {
                            newDataList.add(null);
                        }
                    }
                    newDataList.add("");//ORG_NO:VARCHAR:供电单位编号:本实体记录的唯一标识，创建供电单位的唯一编码。
                    newDataList.add("00");//STATUS:VARCHAR:数据状态
                    newDataList.add("93");//DATA_SRC:VARCHAR:抄表方式
                    newDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    newDataList.add("");//SHARD_NO:VARCHAR:分库字段
                    newDataList.add(dataDate);//DATA_DATE:DATE:数据日期
                    resultList.add(newDataList);
                    num += 2;
                }
                return resultList;
                //反向有功总电能示值
            } else if (curveList.contains(fn)) {
                Date date = (Date) dataList.get(0);
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format(date, DateUtil.defaultDatePattern_YMD));
                return curveData(dataList, fn, newDataList);
            } else {
                throw new RuntimeException("Can not find fn type:" + afn+"_"+fn);
            }
        }
        return newDataList;
    }

    /**
     * @author Dongwei-Chen
     * @Date 2020/1/16 14:44
     * @Description 曲线数据
     */
    private static List curveData(List dataList, int fn, List newDataList) {
        List<List> curvePointList = new ArrayList<>();
        Date date = (Date) dataList.get(0);

        int datapointflag = (Integer) dataList.get(1);
        int interval;
        try {
            interval = getInterval(datapointflag);
        } catch (Exception e) {
            //TODO
            interval=0;
        }
        int databaseinterval=1;
        if(interval==60){
            databaseinterval=4;
        }else if(interval==30){
            databaseinterval=2;
        }else{
            databaseinterval=1;
        }
        if (fn <= 152 && fn >= 149) {
            Object o = dataList.get(3);
            Calendar calendarRate = Calendar.getInstance();
            calendarRate.setTime(date);
            if (o instanceof List) {
                List obValue = (List) o;
                for (Object o1 : obValue) {
                    List feeList = (List) o1;
                    for (int i = 0, j = feeList.size(); i < j; i++ ) {
                        List<Object> objectList = new ArrayList<>();
                        int minrate = calendarRate.get(Calendar.HOUR_OF_DAY) * 60 + calendarRate.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                        int dataTime = getDataPoint(interval, minrate)*databaseinterval + 1;

                        objectList.addAll(newDataList);
                        //特殊处理
                        objectList.set(1,DateUtil.format(calendarRate.getTime(), DateUtil.defaultDatePattern_YMD));


                        objectList.add(dataTime);//数据点数DATA_TIME
                        objectList.add(datapointflag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
                        CurveDataUtils.addDataType(objectList, fn);
                        Object value=feeList.get(i);
                        if(value==null){continue;}
                        objectList.add(value);//示值
                        objectList.add(null);//占位：ORG_NO
                        objectList.add(new Date());
                        objectList.add("93");
                        objectList.add(i);
                        curvePointList.add(objectList);
                    }
                    calendarRate.add(Calendar.MINUTE, interval);
                }
            }
            return curvePointList;
        }
        List data = (List) ((List) dataList.get(2)).get(0);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        for (Object obj : data) {
            int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);  //获取给定数据时标在当天所在的分钟数2
            int dataTime = getDataPoint(interval, min)*databaseinterval + 1;
            if(obj==null){
                calendar.add(Calendar.MINUTE, interval);
                continue;
            }
            List<Object> objs = new ArrayList<>();
            objs.addAll(newDataList);
            //特殊处理
            objs.set(1,DateUtil.format(calendar.getTime(), DateUtil.defaultDatePattern_YMD));

            objs.add(dataTime);
            objs.add(datapointflag);
            CurveDataUtils.addDataType(objs, fn);
            objs.add(obj);
            curvePointList.add(objs);
            calendar.add(Calendar.MINUTE, interval);
        }
        return curvePointList;

    }
    private static int getDataPoint(int interval, int min) {
        return min / interval;
    }

    private static int getInterval(int m) throws Exception {
        // 数据密度
        int interval = 0;
        switch (m) {
            case 0:
                throw new RuntimeException("错误的冻结密度，无法运行");
            case 1:
                interval = 15;//96
                break;
            case 2:
                interval = 30;//48
                break;
            case 3:
                interval = 60;//24
                break;
            case 254:
                interval = 5;//288
                break;
            case 255:
                interval = 1;//1440
                break;
        }
        return interval;
    }

    /**
     * @author Dongwei-Chen
     * @Date 2019/12/9 16:15
     * @Description 月冻结取上一月
     */
    private static Date LastMonth(Object lastDate, Date callDate) {
        Date ld = (Date) lastDate;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(callDate);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        Calendar cs = Calendar.getInstance();
        cs.setTime(ld);
        cs.set(Calendar.YEAR, year);
        cs.set(Calendar.MONTH, month);
        return cs.getTime();
    }
}
