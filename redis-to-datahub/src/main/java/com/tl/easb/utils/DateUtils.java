
package com.tl.easb.utils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public DateUtils() {
    }

    public static String getCurrTime() {
        return getCurrTime("yyyy-MM-dd HH:mm:ss");
    }

    public static Long getLongTime(Date time) throws ParseException {
        return getLongTime(getDateTimeStr(time, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss");
    }

    public static String getCurrTime(String formatstr) {
        return (new SimpleDateFormat(formatstr)).format(new Date());
    }

    public static Long getLongTime(String timeStr, String formatstr) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(formatstr);

        try {
            Date d = sdf.parse(timeStr);
            calendar.setTime(d);
        } catch (ParseException var6) {
            throw new ParseException("时间格式转换错误!", 0);
        }

        return calendar.getTimeInMillis();
    }

    public static Date getDateTime(String timeStr, String formatstr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(formatstr);
        Date d = null;
        if (timeStr != null && !"".equals(timeStr)) {
            d = sdf.parse(timeStr);
        }

        return d;
    }

    public static String getDateTimeStr(BigDecimal time, String formatstr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(formatstr);
        return BigDecimal.valueOf(-1L).equals(time) ? "" : sdf.format(time);
    }

    public static Date getDateTime(BigDecimal time, String formatstr) throws ParseException {
        return getDateTime(getDateTimeStr(time, formatstr), formatstr);
    }

    public static String getDateTimeStr(Date time, String formatstr) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        return getDateTimeStr(BigDecimal.valueOf(calendar.getTimeInMillis()), formatstr);
    }

    public static Long getLongTime(Date time, String formatstr) throws ParseException {
        return getLongTime(getDateTimeStr(time, formatstr), formatstr);
    }

    public static Long getLongTime(BigDecimal time, String formatstr) throws ParseException {
        return getLongTime(getDateTimeStr(time, formatstr), formatstr);
    }

    public static String addSecond(Date date, int n) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar cd = Calendar.getInstance();
            cd.setTime(date);
            cd.add(14, n * 1000);
            return sdf.format(cd.getTime());
        } catch (Exception var4) {
            return " ";
        }
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(getCurrTime());
        Long lTime = getLongTime("2013-08-16 16:01:29", "yyyy-MM-dd HH:mm:ss");
        System.out.println(lTime);
        String time = getDateTimeStr(new BigDecimal("1591027200000"), "yyyy-MM-dd HH:mm:ss");
        System.out.println(time);
    }
}
