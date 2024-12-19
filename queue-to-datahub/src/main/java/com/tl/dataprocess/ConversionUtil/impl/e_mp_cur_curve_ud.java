package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;
import com.tl.utils.DateUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 终端实时时钟信息
 * @author wjj
 * @date 2021/11/30 10:10
 * @return
 */
public class e_mp_cur_curve_ud implements IConversionUtil {

    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> resultList = new ArrayList<>();
        int datapointflag = (int) ProcessUtil.getDataByObj(dataList.get(3), Integer.class);
        int interval;
        try {
            interval = getInterval(datapointflag);
        } catch (Exception e) {
            return null;
        }
        int databaseinterval;
        if (interval == 60) {
            databaseinterval = 4;
        } else if (interval == 30) {
            databaseinterval = 2;
        } else {
            databaseinterval = 1;
        }
        Date dataTime = (Date) ProcessUtil.getDataByObj(dataList.get(2), Date.class);


        Calendar calendar = getCalendar();
        calendar.setTime(dataTime);
        Object o = dataList.get(5);
        if (o instanceof List) {
            List obValue = (List) o;
            for (int i = 0; i < obValue.size(); i++) {
                Object oo = obValue.get(i);//示值
                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                if (oo == null) {
                    calendar.add(Calendar.MINUTE, interval);
                    continue;
                }
                //ID:BIGINT,DATA_DATE:STRING,DATA_TIME:STRING,DATA_POINT_FLAG:BIGINT,PHASE_FLAG:BIGINT,V:DOUBLE,ORG_NO:STRING,
                // INSERT_TIME:TIMESTAMP,DATA_SRC:STRING
                List<Object> finallyList=new ArrayList<>();
                String shardIndex=terminalArchivesObject.getTerminalId();
                String dataDate = DateUtil.format(calendar.getTime(), DateUtil.defaultDatePattern_YMD);
                //用于缓存删除
                String clearDate="00000000000000";
                if (dataSrc.equals("0")||dataSrc.equals("10")){
                    clearDate=dataDate;
                }
                //占用前2个索引
                finallyList.add(shardIndex);
                finallyList.add(clearDate);

                //ID:BIGINT,DATA_DATE:STRING,DATA_TIME:STRING,DATA_POINT_FLAG:BIGINT,PHASE_FLAG:BIGINT,V:DOUBLE,ORG_NO:STRING,
                // INSERT_TIME:TIMESTAMP,DATA_SRC:STRING
                finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
                finallyList.add(dataDate);
                finallyList.add(min / interval * databaseinterval + 1);
                finallyList.add(datapointflag);
                finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), BigDecimal.class));
                finallyList.add(oo);
                finallyList.add(terminalArchivesObject.getPowerUnitNumber());
                finallyList.add(new Date());
                finallyList.add(dataSrc);
                calendar.add(Calendar.MINUTE, interval);

//                tempList.add(finallyList);
                resultList.add(finallyList);
            }
        }
        return resultList;
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

    static Calendar getCalendar() {
        Calendar calendar = currentCalendar.get();
        if (null == calendar) {
            calendar = Calendar.getInstance();
            currentCalendar.set(calendar);
        }
        return calendar;
    }

    private int getDataPoint(int interval, int min) {
        return min / interval;
    }
}

