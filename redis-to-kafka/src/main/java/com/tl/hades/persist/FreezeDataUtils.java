package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubTopic;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class FreezeDataUtils {
    public static List<Integer> FN_LIST = new ArrayList<>();

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
    public static boolean isHistory(List dataList) {
        boolean isHistory = true;
        if (dataList != null && dataList.size() > 0) {
            if (dataList.get(0) instanceof Date) {
                Date requireDate = (Date) dataList.get(0);
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, -1);
                System.out.println(requireDate.compareTo(cal.getTime()));
                String ori = new SimpleDateFormat("yyyy-MM-dd").format(requireDate);
                String yes = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
                if (ori.equals(yes)) {
                    isHistory = false;
                }
            }
        }
        return isHistory;
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
                newDataList.add(dataList.get(3));
                Object obj4 = dataList.get(4);
                Calendar calDate = Calendar.getInstance();
                Calendar calDataDate = Calendar.getInstance();
                calDataDate.setTime((Date) dataList.get(0));
                if (obj4 != null) {
                    calDate.setTime((Date) obj4);
                    calDate.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                    obj4 = calDate.getTime();
                }
                if (!(obj4 instanceof Date)) {
                    return null;
                }
                newDataList.add(obj4);
                List dt = (List) ((List) dataList.get(5)).get(0);
                for (int i = 0; i < dt.size(); i++) {
                    Object o = dt.get(i);
                    if (i % 2 != 0) {
                        Calendar cel = Calendar.getInstance();
                        Date time = (Date) o;
                        cel.setTime(time);
                        cel.set(Calendar.YEAR, calDataDate.get(Calendar.YEAR));
                        newDataList.add(cel.getTime());
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
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
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
                //				newDataList.add(dataList.get(4));
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
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
                newDataList.add(dataList.get(4));
                newDataList.add(dataList.get(5));
                newDataList.add(dataList.get(6));
                newDataList.add(dataList.get(7));
                newDataList.add(dataList.get(8));
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
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));

            } else if (fn == 33) {//yue总及分相最大有功功率及发生时间、有功功率为零时间
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
                newDataList.add(dataList.get(4));
                newDataList.add(dataList.get(5));
                newDataList.add(dataList.get(6));
                newDataList.add(dataList.get(7));
                newDataList.add(dataList.get(8));
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
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
                newDataList.add(dataList.get(4));
                newDataList.add(dataList.get(5));
                newDataList.add(dataList.get(6));
                newDataList.add(dataList.get(7));
                newDataList.add(dataList.get(8));
                newDataList.add(dataList.get(9));
                newDataList.add(dataList.get(10));
                newDataList.add(dataList.get(11));
                newDataList.add(dataList.get(12));
                newDataList.add(dataList.get(13));
                newDataList.add(dataList.get(14));
                newDataList.add(dataList.get(15));

            } else if (fn == 27) {//日电压统计记录
                newDataList = new ArrayList();
                Calendar calendar = Calendar.getInstance();
                Calendar calDate = Calendar.getInstance();
                calDate.setTime((Date) dataList.get(0));

                newDataList.add(mpedId);
                newDataList.add(dataList.get(1));
                newDataList.add(dataList.get(2));
                newDataList.add(dataList.get(3));
                newDataList.add(dataList.get(4));
                newDataList.add(dataList.get(5));
                newDataList.add(dataList.get(6));
                newDataList.add(dataList.get(7));
                newDataList.add(dataList.get(8));
                newDataList.add(dataList.get(9));
                newDataList.add(dataList.get(10));
                newDataList.add(dataList.get(11));
                newDataList.add(dataList.get(12));
                newDataList.add(dataList.get(13));
                newDataList.add(dataList.get(14));
                newDataList.add(dataList.get(15));
                newDataList.add(dataList.get(16));
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
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
                    newDataList.add(null);
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
                newDataList.add(((Date) dataList.get(1)));
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
            } else {
                throw new RuntimeException("Can not find fn type:" + fn);
            }
        }
        return newDataList;
    }

    /**
     * fn=3的日冻结最大需量入库
     *
     * @param dataListFinal
     * @param terminal
     * @param mpedId
     * @param businessDataitemId
     * @param dataList
     * @param protocolId
     * @return
     */
    public static DataObject getFinalDataList(String type, Object[] refreshKey, List<Object> dataListFinal, TerminalArchivesObject terminal, String mpedId, String businessDataitemId, List<?> dataList) {
        DataObject dataObj = null;
        try {
            dataListFinal.add(terminal.getPowerUnitNumber());//供电单位编号
            dataListFinal.add("00");//未分析
            dataListFinal.add(type);//自动抄表
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminal.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
            dataListFinal.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间

            DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
            int index = Math.abs(mpedId.hashCode()) % dataHubTopic.getShardCount();
            String shardId = dataHubTopic.getActiveShardList().get(index);
            dataObj = new DataObject(dataListFinal, refreshKey, dataHubTopic.topic(), shardId);//给这个数据类赋值:list key classname (fn改为161)

        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataObj;
    }

    public static String lastDayByMonth(String dataDate) throws Exception {
        Date data = DateUtil.parse(dataDate);
        Calendar first = Calendar.getInstance();
        first.setTime(data);
        Calendar last = Calendar.getInstance();
        last.set(Calendar.YEAR, first.get(Calendar.YEAR));
        last.set(Calendar.MONTH, first.get(Calendar.MONTH));
        int day = last.getActualMaximum(Calendar.DATE);
        last.set(Calendar.DAY_OF_MONTH, day);
        return DateUtil.format(last.getTime(), DateUtil.defaultDatePattern_YMD);
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
