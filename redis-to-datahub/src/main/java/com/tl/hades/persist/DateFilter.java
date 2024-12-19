package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFilter {

    /**
     * 时间区间判断
     * 判断当前时间之前到当前时间之后的时间区间内
     *
     * @param date
     * @return
     */
    public static boolean dateFilter(String date, int start, int end) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Long dates = sdf.parse(date).getTime();
            Long afterDate = clenderDate(new Date(), start).getTime();
            Long befoDate = clenderDate(new Date(), end).getTime();
            if (afterDate <= dates && dates <= befoDate) {
                return true;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 时间操作
     *
     * @param date
     * @param day
     * @return
     */
    public static Date clenderDate(Date date, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, day);
        Date befoDate = calendar.getTime();
        return befoDate;
    }

    /**
     * 获取上月最后一天时间
     *
     * @return
     */
    public static Date getPrevMonthLastDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, -1);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    /**
     * 判断当前时间是否在最近day天之内
     *
     * @param eventDate
     * @return
     */
    public static boolean isLateWeek(Object eventDate, Integer day) {
        try {
            if (eventDate == null || "".equals(eventDate)) {
                eventDate = new Date();
            }
            Date date = (Date) eventDate;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.DAY_OF_WEEK, day);
            Date lastWeek = calendar.getTime();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            Date last = sdf.parse(sdf.format(lastWeek));

            Date event = sdf.parse(sdf.format(date));

            if (last.getTime() <= event.getTime()) {
                return false;
            }
        } catch (Exception e) {
            return true;
        }
        return true;
    }

    /**
     * 判断日期是否在start和end的天数之间
     *
     * @param eventDate
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public static boolean betweenDay(Object eventDate, int start, int end) throws Exception {
        boolean jg = false;
        Date now = new Date();
        Date event_date;
        try {
            event_date = DateUtil.parse(eventDate.toString(), "yyyy-MM-dd HH:mm:ss");
        } catch (Exception e) {
            event_date = DateUtil.parse(eventDate.toString(), DateUtil.defaultDatePattern_YMD);
        }
        long bet = (now.getTime() - event_date.getTime()) / (1000 * 60 * 60 * 24);
        if (start <= bet && bet <= end) {
            jg = true;
        }
        return jg;
    }
}
