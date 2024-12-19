package com;

import com.tl.utils.DateUtil;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * @author wangjunjie
 * @date 2022/1/13 15:52
 */
public class UtilTest {

    @Test
    public void test1() {
        Calendar calendar = Calendar.getInstance();
        Date d = new Date();
        d.setTime(1638417600000l);
        calendar.setTime(d);
//        calendar.add(Calendar.MINUTE, 10);
//        String mtDataDate = DateUtil.format(calendar.getTime(), DateUtil.defaultDatePattern_YMDHMS);
        int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
        System.out.println(min);
    }
}
