package com.tl.test;


import com.tl.easb.utils.DateUtil;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TestDw {

    public static void main(String[] args) throws Exception {

       SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");



        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date startTime=sdf.parse("2021-04-01 23:40:00");

        System.out.println(format.format(startTime));


//明天数据时标及日期
        Calendar c = getCalendar();
        c.setTime(startTime);
        c.add(Calendar.DATE, 1);
        String mtDataDate = DateUtil.format(c.getTime(), "yyyy-MM-dd") + " 00:00:00";

        Calendar calendarMt = getCalendar();
        calendarMt.setTime(DateUtil.parse(mtDataDate));


        Calendar calendar = getCalendar();
        calendar.setTime(startTime);
        int databaseinterval=4;
        int interval=60;
        for(int m=0;m<4;m++){
            int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
            System.out.println(getDataPoint(interval, min));
            System.out.println(getDataPoint(interval, min)*databaseinterval);
            System.out.println(getDataPoint(interval, min)*databaseinterval+1);
//            System.out.println(getDataPoint(30, min)*2+1);
//            point1 = getDataPoint(interval, min1) * databaseinterval + 1;
//            System.out.println(getDataPoint(15, min) + 1);
//            System.out.println(getDataPoint(5, min) + 1);
            calendar.add(Calendar.MINUTE, interval);
            System.out.println(calendar.getTime());
        }

//        point1 = getDataPoint(databaseinterval, min1) + interval / databaseinterval;
    }
    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();
    static Calendar getCalendar() {
        Calendar calendar = currentCalendar.get();
        if (null == calendar) {
            calendar = Calendar.getInstance();
            currentCalendar.set(calendar);
        }
        return calendar;
    }

    static SimpleDateFormat getFormat() {
        SimpleDateFormat format = currentFormat.get();
        if (null == format) {
            format = new SimpleDateFormat("yyyy-MM-dd");
            currentFormat.set(format);
        }
        return format;
    }

    private static int getDataPoint(int interval, int min) {
        return min / interval;
    }

    /**
     * 曲线数据冻结密度
     *
     * @param m
     * @return
     * @throws Exception
     */
    private int getInterval(int m) throws Exception {
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

}
