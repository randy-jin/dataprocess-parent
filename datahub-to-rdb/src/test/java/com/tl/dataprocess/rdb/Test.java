package com.tl.dataprocess.rdb;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

import java.sql.Date;

/**
 * Created by jinzhiqiang on 2018/6/19.
 */
public class Test {
    public static void main(String[] args) {
        Config config = ConfigService.getAppConfig(); //config instance is singleton for each namespace and is never null
        String someKey = "dburl";
        String someDefaultValue = "someDefaultValueForTheKey";
        String value = config.getProperty(someKey, someDefaultValue);
        System.out.println(value);
//        String fieldValStr = "2000-09-20";

//        final int YEAR_LENGTH = 4;
//        final int MONTH_LENGTH = 2;
//        final int DAY_LENGTH = 2;
//        final int MAX_MONTH = 12;
//        final int MAX_DAY = 31;
//
//        int firstDash;
//        int secondDash;
//        Date d = null;
//        firstDash = fieldValStr.indexOf('-');
//        secondDash = fieldValStr.indexOf('-', firstDash + 1);
//
//        if ((firstDash > 0) && (secondDash > 0) && (secondDash < fieldValStr.length() - 1)) {
//            String yyyy = fieldValStr.substring(0, firstDash);
//            String mm = fieldValStr.substring(firstDash + 1, secondDash);
//            String dd = fieldValStr.substring(secondDash + 1);
//            if (yyyy.length() == YEAR_LENGTH &&
//                    (mm.length() >= 1 && mm.length() <= MONTH_LENGTH) &&
//                    (dd.length() >= 1 && dd.length() <= DAY_LENGTH)) {
//                int year = Integer.parseInt(yyyy);
//                int month = Integer.parseInt(mm);
//                int day = Integer.parseInt(dd);
//
//                if ((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY)) {
//                    d = new Date(year - 1900, month - 1, day);
//                }
//            }
//        } else {
//            System.out.println("The date value is " + fieldValStr);
//        }

//        System.out.println(dateStr(fieldValStr));
    }

    public static Date dateStr(String s) {
        final int YEAR_LENGTH = 4;
        final int MONTH_LENGTH = 2;
        final int DAY_LENGTH = 2;
        final int MAX_MONTH = 12;
        final int MAX_DAY = 31;
        int firstDash;
        int secondDash;
        Date d = new Date(1000 - 1900, 10 - 1, 10);
        firstDash = s.indexOf('-');
        secondDash = s.indexOf('-', firstDash + 1);

        String yyyy = null;
        String mm = null;
        String dd = null;

        if (firstDash < 0) {
            yyyy = s.substring(0, 4);
            mm = s.substring(4, 6);
            dd = s.substring(6);
        } else if ((firstDash > 0) && (secondDash > 0) && (secondDash < s.length() - 1)) {
            yyyy = s.substring(0, firstDash);
            mm = s.substring(firstDash + 1, secondDash);
            dd = s.substring(secondDash + 1);
        } else {
            System.out.println("The date value is [" + s + "],will be initialized to 1000-10-10");
            yyyy = "1000";
            mm = "10";
            dd = "10";
        }

        if (yyyy.length() == YEAR_LENGTH &&
                (mm.length() >= 1 && mm.length() <= MONTH_LENGTH) &&
                (dd.length() >= 1 && dd.length() <= DAY_LENGTH)) {
            int year = Integer.parseInt(yyyy);
            int month = Integer.parseInt(mm);
            int day = Integer.parseInt(dd);

            if ((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY)) {
                d = new Date(year - 1900, month - 1, day);
            } else {
                System.out.println("The date value is year=[" + year + "],mouth=[" + month + "],day=[" + day + "]");
            }
        }

        System.out.println(d);
        return d;
    }
}
