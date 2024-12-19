package com;



import org.springframework.util.DigestUtils;

import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author Dongwei-Chen
 * @Date 2020/4/29 18:39
 * @Description 共用方法
 */
public class PublicUtil {

    /**
     * @author Dongwei-Chen
     * @Date 2020/4/29 18:05
     * @Description 计算冻结密度
     */
    public static String getDateTime(String pointFlag, Long point, String dataDate) throws ParseException {
        int pointSplit;
        switch (pointFlag) {
            case "1"://96
                pointSplit = 15;
                break;
            case "2"://48
                pointSplit = 30;
                break;
            case "3"://24
                pointSplit = 60;
                break;
            case "254"://288
                pointSplit = 5;
                break;
            case "255"://1440
                pointSplit = 1;
                break;
            default:
                return null;
        }

        long currentTime = pointSplit * (point-1) * 60 * 1000;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        long date = sdf.parse(dataDate).getTime();

        Date finalDate = new Date(date + currentTime);

        return new SimpleDateFormat("MMddyyyyHHmmss").format(finalDate);
    }

    public static void main(String[] args) throws Exception {

        System.out.println(DigestUtils.md5DigestAsHex("Am_123456".getBytes()));
        int s=101;
        if ("4 5 10".contains(s+"")) {
            System.out.println(true);
        }

        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(1626278400000L)));

//        System.out.println(getDateTime("1", 96L, "20200717"));
    }


}
