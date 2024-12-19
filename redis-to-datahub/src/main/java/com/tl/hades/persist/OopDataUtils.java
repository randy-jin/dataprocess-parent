package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
/**
 * 面向对象e_mp_day_read_source入库list
 *
 * @author easb
 */
public class OopDataUtils {

    @SuppressWarnings("all")
    public static List<Object> getDataList(String flag, List dataList, String dataItemsId, Date date, String mpedId, Date callDate) {
        //实时数据存储时标需要减一
        if ("current".equals(flag)) {
            date = timeAddOrMinus(date, -1);
        }
        List<Object> newDataList = new ArrayList<Object>();

        newDataList.add(mpedId + "_" + DateUtil.format(date, DateUtil.defaultDatePattern_YMD));
        newDataList.add(mpedId);//从缓存取测量点标识，这里占位
        if (dataItemsId.equals("00100200")) {
            newDataList.add("1");//1正向有功
        } else if (dataItemsId.equals("00300200")) {
            newDataList.add("2");//2正向无功
        } else if (dataItemsId.equals("00200200")) {
            newDataList.add("5");//5反向有功
        } else if (dataItemsId.equals("00400200")) {
            newDataList.add("6");//6反向无功
        } else if (dataItemsId.equals("00500200")) {
            newDataList.add("3");//3一象限无功
        } else if (dataItemsId.equals("00600200")) {
            newDataList.add("7");//7二象限无功
        } else if (dataItemsId.equals("00700200")) {
            newDataList.add("8");//8三象限无功
        } else if (dataItemsId.equals("00800200")) {
            newDataList.add("4");//4四象限无功
        }
        newDataList.add(callDate);
        for (int i = 0; i < dataList.size(); i++) {
            newDataList.add(dataList.get(i));
        }
        for (int i = dataList.size(); i < 15; i++) {
            newDataList.add(null);
        }
        return newDataList;
    }


    public static List<Object> getLastRead(String flag, List dataList, String dataItemsId, Date date, String mpedId, Date callDate) {
        List<Object> newDataList = new ArrayList<>();
        return newDataList;
    }

    @SuppressWarnings("rawtypes")
    public static boolean allNull(List list) { //传入了当前pnfn测量点的数据项列表list
        boolean allNull = true;
        for (Object o : list) {
            if (o instanceof List) {
                allNull((List) o);
            } else {
                if (o != null&& !(o instanceof Integer)) {
                    allNull = false;
                    return allNull;
                }
            }
        }
        return allNull;
    }

    public static Date timeAddOrMinus(Date collDate, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(collDate);
        calendar.add(Calendar.DAY_OF_MONTH, day);
        return calendar.getTime();
    }

    /**
     * 判断类型
     *
     * @param obj
     * @param clazz
     * @return
     */
    public static boolean notNull(Object obj, Class<?> clazz) {
        if (clazz.isInstance(obj)) {
            return true;
        }
        return false;
    }

    /**
     * @Author Dong.wei-CHEN
     * Date 2019/8/22 11:33
     * @Descrintion 面向对象版本日期格式化
     */
    public static Date versionDate(Object obj) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            if (obj == null || "".equals(obj)) {
                return null;
            }
            String date = String.valueOf(obj);
            if (date.length() < 6) {
                return  null;
            }
            String yyyy = "20" + date.substring(date.length() - 2);
            String mm = date.substring(date.length() - 4, date.length() - 2);
            String dd = date.substring(0, 2);
            try {
                Integer.parseInt(mm);
            } catch (Exception e) {
                mm = String.valueOf(Integer.parseInt(mm, 16));
            }
            String result = yyyy + mm + dd;
            return sdf.parse(result);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * @Author Dong.wei-CHEN
     * Date 2019/8/22 11:33
     * @Descrintion 面向对象版本日期格式化
     */
    public static Date versionSoftDate(Object obj) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            if (obj == null || "".equals(obj)) {
                return null;
            }
            String date = String.valueOf(obj);
            if (date.length() < 6) {
                return  null;
            }
            String yyyy = "20" + date.substring(0, 2);
            String mm = date.substring(date.length() - 4, date.length() - 2);
            String dd = date.substring(date.length() - 2);
            try {
                Integer.parseInt(mm);
            } catch (Exception e) {
                mm = String.valueOf(Integer.parseInt(mm, 16));
            }
            String result = yyyy + mm + dd;
            return sdf.parse(result);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * @Author Dong.wei-CHEN
     * Date 2019/11/4 14:22
     * @Descrintion 相位端口
     */
    public static Integer getPort(String oad) {
        switch (oad) {
            case "F2080201":
                return 29;
            case "F2010201":
                return 1;
            case "F2010202":
                return 2;
            case "F2090201":
                return 31;
            default:
                return null;
        }
    }


    /**
     * 芯片信息
     *
     * @param str
     * @return
     */
    public static String splitCore(String str, int index) {
        List<String> result = new ArrayList<>();
        try {
//             str = "type=3;mfrs=TC;idformat=2;id=01029C01C1FB0254435632000007B3F52AE92C1C5CC952EC;mmfrs=TC;midformat=1;mid=4130054000000046836176";
            String[] sp = str.split(";");
            for (int i = 0; i < sp.length; i++) {
                String[] sps = sp[i].split("=");
                result.add(sps[1]);
            }
            return result.get(index);
        } catch (Exception e) {
            return null;
        }
    }
}