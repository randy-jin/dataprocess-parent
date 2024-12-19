package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.text.SimpleDateFormat;
import java.util.*;

public class CurrentDataUtils {

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
                if (o != null && !(o instanceof Date) && !(o instanceof Integer)) {
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
        if (afn == 12) {
            if (fn >= 129 && fn <= 136) {
                newDataList = new ArrayList();
                newDataList.add(1);//从缓存取测量点标识，这里占位
                if (fn == 129)
                    newDataList.add("1");//1正向有功
                else if (fn == 130)
                    newDataList.add("2");//正向无功
                else if (fn == 131)
                    newDataList.add("5");//反向有功
                else if (fn == 132)
                    newDataList.add("6");//反向无功
                else if (fn == 133)
                    newDataList.add("3");//一象限无功
                else if (fn == 134)
                    newDataList.add("4");//二象限无功
                else if (fn == 135)
                    newDataList.add("7");//三象限无功
                else if (fn == 136)
                    newDataList.add("8");//四象限无功
                else
                    newDataList.add("0");//备用

                int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                Object colDate = dataList.get(0);
                if (null != colDate && colDate instanceof Date) {
                    newDataList.add(colDate);
                }
                newDataList.add(dataList.get(1)); //正向有功总电能示值
                newDataList.addAll(list);//4个值的list，一次性添加
                for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.add(1, mpedId);//首元素id
                // 把数据时标放在最后，方便外层处理
                Object dataDate = colDate;
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format(DateUtil.addDaysOfMonth((Date) dataDate, -1), DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)，取当前采集数据日期-1
                }
                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
            } else if (fn == 246) {//当前掉电记录数据
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(null);//终端id占位
                newDataList.add(dataList.get(0));
                for (int i = 1; i < dataList.size(); i++) {
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 167) {//购用电信息
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(dataList.get(0));//抄表日期
                newDataList.add(null);//orgNo占位
                newDataList.add(dataList.get(4));//剩余电量
                newDataList.add(dataList.get(2));//剩余金额
                newDataList.add(dataList.get(8));//报警电量
                newDataList.add(dataList.get(9));//故障电量
                newDataList.add(dataList.get(6));//累计购电量
                newDataList.add(dataList.get(3));//累计购电金额
                newDataList.add(((Double) dataList.get(1)).intValue());//购电次数
                newDataList.add(dataList.get(7));//赊欠门限值
                newDataList.add(dataList.get(5));//透支电量
                //当前三相及总有/无功功率，功率因数，三相电压，电流，零序电流，视在功率
            } else if (fn == 25) {
                List<List> obList = new ArrayList<>();
                Date colTime = (Date) dataList.get(0);
                String eventDate = DateUtil.format(colTime, DateUtil.defaultDatePattern_YMD);
                if (dataList.size() != 24) {
                    return null;
                }
                Map<String, List> bodyMap = new HashMap<>(6);
                bodyMap.put("active_power", dataList.subList(1, 5));
                bodyMap.put("reactive_power", dataList.subList(5, 9));
                bodyMap.put("power_factor", dataList.subList(9, 13));
                bodyMap.put("voltage", dataList.subList(13, 16));
                bodyMap.put("electricity", dataList.subList(16, 20));
                bodyMap.put("power", dataList.subList(20, 24));

                for (Map.Entry<String, List> m : bodyMap.entrySet()
                ) {
                    String key = m.getKey();
                    List body = m.getValue();
                    newDataList = new ArrayList();
                    newDataList.add(mpedId);//ID:BIGINT:测量点标识（MPED_ID）
                    newDataList.add(eventDate);//DATA_DATE:DATE:数据日期
                    newDataList.add(colTime);//COL_TIME:DATETIME:终端抄表日期时间
                    newDataList.add("");//ORG_NO:VARCHAR:供电单位编号
                    for (int i = 0, j = body.size(); i < j; i++) {
                        newDataList.add(body.get(i));
                    }
                    newDataList.add(key);
                    obList.add(newDataList);
                }
                return obList;
            } else {//电能表购、用电信息
                throw new RuntimeException("Can not find fn type:" + fn);
            }
        }
        return newDataList;
    }

    public static void main(String[] args) {
        String[] str = {"1", "2", "3", "4", "5"};
        List list = Arrays.asList(str);
        for (Object oj : list.subList(0, 5)
        ) {
            System.out.println(oj);
        }
    }
}
