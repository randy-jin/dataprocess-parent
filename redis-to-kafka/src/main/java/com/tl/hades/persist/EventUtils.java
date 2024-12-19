package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubTopic;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 全事件
 *
 * @author easb
 */
public class EventUtils {
    /**
     * @Author chendongwei
     * Date 2019/2/25 14:23
     * @Descrintion 时间格式转换
     */
    public static Date getDate(List<Object> dataList, int index) {
        if (dataList.isEmpty() || dataList.size() < index + 1) {
            return null;
        }
        Object obj = dataList.get(index);
        if (null == obj || "".equals(obj)) {
            return null;
        }
        String date = null;
        Date finalDate = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            if (obj instanceof Date) {
                finalDate = (Date) obj;
            } else {
                date = String.valueOf(obj);
                finalDate = sdf.parse(date);
            }
            //mysql数据库支持的最早时间
            long startDate = sdf.parse("1970-01-01 00:00:00").getTime();
            //mysql数据库支持的最大时间
            long endDate = sdf.parse("9999-12-31 23:59:59").getTime();

            long callDate = finalDate.getTime();
            if (startDate <= callDate && callDate <= endDate) {
                return finalDate;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /**
     * 判断时间是否正常
     *
     * @param date
     * @return
     */
    public static Date dataFilter(Date date) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            //mysql数据库支持的最早时间
            long startDate = sdf.parse("1970-01-01 00:00:00").getTime();
            //mysql数据库支持的最大时间
            long endDate = sdf.parse("9999-12-31 23:59:59").getTime();

            long callDate = date.getTime();
            if (startDate <= callDate && callDate <= endDate) {
                return date;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /**
     * 事件日期
     *
     * @param event_time
     * @return
     */
    public static String getEventDate(Object event_time) {
        if (event_time == null || "".equals(event_time)) {
            return DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD);
        }
        String date;
        Date finalDate;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            if (event_time instanceof Date) {
                finalDate = (Date) event_time;
            } else {
                date = String.valueOf(event_time);
                finalDate = sdf.parse(date);
            }
            //mysql数据库支持的最早时间
            long startDate = sdf.parse("1970-01-01 00:00:00").getTime();
            //mysql数据库支持的最大时间
            long endDate = sdf.parse("9999-12-31 23:59:59").getTime();

            long callDate = finalDate.getTime();
            if (startDate <= callDate && callDate <= endDate) {
                String result = DateUtil.format(finalDate, DateUtil.defaultDatePattern_YMD);
                return result;
            }
        } catch (Exception e) {
            return DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD);
        }
        return DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD);
    }


    public static void toDataHub(String businessDataitemId, String mpedIdStr, List<Object> dataListFinal, Object[] refreshKey, List<DataObject> listDataObj) throws Exception {
        DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
        int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
        String shardId = dataHubTopic.getActiveShardList().get(index);
        DataObject dataObj = new DataObject(dataListFinal, refreshKey, dataHubTopic.topic(), shardId);
        listDataObj.add(dataObj);
    }
}
