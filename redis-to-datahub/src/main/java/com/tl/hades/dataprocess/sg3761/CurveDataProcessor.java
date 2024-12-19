package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.CurveDataUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import com.tl.promethues.PromethuesAllDayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CurveDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(CurveDataProcessor.class);


    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET CURVE DATA===");
        Object obj = context.getContent();
        //TODO
        PromethuesAllDayUtil.fromRedisCountAllDay.labels("CURVE_DATA:COUNT").inc();
        if (obj instanceof TmnlMessageResult) {
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class<?> clazz = ParamConstants.classMap.get(protocolId);
            if (null == clazz) {
                throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
            }
            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
                int afn = terminalDataObject.getAFN();
                String areaCode = terminalDataObject.getAreaCode();
                int terminalAddr = terminalDataObject.getTerminalAddr();
                List<PersistentObject> list = new ArrayList<>();//持久类
                List<DataObject> listDataObj = new ArrayList<>();//数据类
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    try {
                        List dataList = data.getList();
                        logger.info("===MAKE CURVE DATA===");
                        int pn = data.getPn();//706 4156 1 81
                        int fn = data.getFn();
                        int databaseinterval;

                        Date startTime = (Date) dataList.get(0);


                        int datapointflag = (Integer) dataList.get(1);
//                        if (datapointflag == 1000) {
//                            datapointflag = 1;
//                        }
                        int interval;
                        try {
                            interval = getInterval(datapointflag);
                        } catch (Exception e) {
                            //TODO
//                            PromethuesAllDayUtil.fromRedisCountAllDay.labels("CURVE_DATA:ERROR_DATAPOINT_COUNT").inc();
                            logger.error("datapointflag:" + datapointflag, e);
                            continue;
                        }

                        logger.info("===MAKE CURVE protocolId===" + protocolId);
                        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                        protocol3761ArchivesObject.setAfn(afn);
                        protocol3761ArchivesObject.setFn(fn);
                        //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
                        TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
                        String mpedIdStr = terminalArchivesObject.getID();

                        if (null == mpedIdStr || "".equals(mpedIdStr)) {
                            logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                            PromethuesAllDayUtil.fromRedisCountAllDay.labels("CURVE_DATA:NO_Archives_COUNT").inc();
                            continue;
                        }
                        BigDecimal mpedId = new BigDecimal(mpedIdStr);
                        String businessDataitemId;
                        if (dataList.get(1).equals(254)) {
                            businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject288(protocol3761ArchivesObject).getBusiDataItemId();
                            logger.info("vol_288 " + businessDataitemId);
//                            databaseinterval = 5;
                        } else {
                            businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
//                            databaseinterval = 15;
                        }

                        //hch add
                        if (interval == 60) {
                            databaseinterval = 4;
                        } else if (interval == 30) {
                            databaseinterval = 2;
                        } else {
                            databaseinterval = 1;
                        }

                        //TODO
                        //明天数据时标及日期
                        Calendar c = getCalendar();
                        c.setTime(startTime);
                        c.add(Calendar.DATE, 1);
                        String mtDataDate = DateUtil.format(c.getTime(), "yyyy-MM-dd") + " 00:00:00";

                        Calendar calendarMt = getCalendar();
                        calendarMt.setTime(DateUtil.parse(mtDataDate));


                        Calendar calendar = getCalendar();
                        calendar.setTime(startTime);
                        if (dataList.size() < 3) {
                            continue;
                        }
                        if (afn == 13 && (fn <= 152 && fn >= 149)) {
                            Object o = dataList.get(3);
                            if (o instanceof List) {
                                List obValue = (List) o;
                                Object[] refreshKey = new Object[5];
                                refreshKey[0] = terminalArchivesObject.getTerminalId();
                                refreshKey[1] = mpedIdStr;
                                refreshKey[2] = startTime; // 数据召测时间
                                refreshKey[3] = businessDataitemId;
                                refreshKey[4] = protocolId;
                                for (Object o1 : obValue) {
                                    List feeList = (List) o1;

                                    for (int i = 0, j = feeList.size(); i < j; i++
                                            ) {
                                        List<Object> objectList = new ArrayList<>();
                                        String dataDate = DateUtil.format(startTime, DateUtil.defaultDatePattern_YMD);
                                        int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                        objectList.add(mpedId);//ID
                                        objectList.add(dataDate);//数据时标DATA_DATE
//                                        objectList.add(getDataPoint(databaseinterval, min) + interval / databaseinterval);//数据点数DATA_TIME

                                        objectList.add(getDataPoint(interval, min) * databaseinterval + 1);//数据点数DATA_TIME

                                        objectList.add(datapointflag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288

                                        CurveDataUtils.addDataType(objectList, fn);
                                        objectList.add(feeList.get(i));//示值
                                        objectList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                                        objectList.add(new Date());
                                        objectList.add("0");
                                        objectList.add(i);
                                        DataObject dataObj = getDataObj(mpedIdStr, businessDataitemId, refreshKey, objectList);
                                        listDataObj.add(dataObj);
                                    }
                                    calendar.add(Calendar.MINUTE, interval);
                                }
                            }
                            continue;
                        }
                        Object o = dataList.get(2);
                        if (o instanceof List) {
                            List listValue = (List) o;
                            for (Object ob : listValue) {
                                try {
                                    List obValue = (List) ob;
                                    if (CommonUtils.allNull(obValue)) {
                                        //TODO
                                        PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:ALLNULL_COUNT", String.valueOf(fn)).inc();
                                        continue;
                                    }
                                    Object[] refreshKey = new Object[5];
                                    refreshKey[0] = terminalArchivesObject.getTerminalId();
                                    refreshKey[1] = mpedIdStr;
                                    refreshKey[2] = startTime; // 数据召测时间
                                    refreshKey[3] = businessDataitemId;
                                    refreshKey[4] = protocolId;

                                    //TODO
                                    int j = -1;
                                    int point1 = 0;
                                    for (int index = 0; index < obValue.size(); index++) {
                                        try {
                                            Object oo = obValue.get(index);//shizhi
                                            if (oo == null) {
                                                int min1 = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                                if (point1 == 96) {
                                                    calendarMt.setTime(DateUtil.parse(mtDataDate));
                                                    calendarMt.add(Calendar.MINUTE, interval);
                                                } else {
//                                                    point1 = getDataPoint(databaseinterval, min1) + interval / databaseinterval;
                                                    point1 = getDataPoint(interval, min1) * databaseinterval + 1;
                                                    calendar.add(Calendar.MINUTE, interval);
                                                }
                                                continue;
                                            }
                                            if (dataList.get(1).equals(254)) {
                                                List dataValue288 = new ArrayList();
                                                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                                String dataDate = DateUtil.format(startTime, DateUtil.defaultDatePattern_YMD);

                                                dataValue288.add(mpedId);//ID
                                                dataValue288.add(dataDate);//数据时标DATA_DATE
//                                                dataValue288.add(getDataPoint(databaseinterval, min) + interval / databaseinterval);//数据点数DATA_TIME

                                                dataValue288.add(getDataPoint(interval, min) * databaseinterval + 1);//数据点数DATA_TIME

                                                dataValue288.add(datapointflag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288

                                                CurveDataUtils.addDataType(dataValue288, fn);
                                                dataValue288.add(oo);//示值
                                                dataValue288.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                                                dataValue288.add("00");
                                                dataValue288.add("0");
                                                dataValue288.add(new Date());
                                                dataValue288.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                                                CurveDataUtils.addDataSrc(dataValue288, fn, "0");
                                                calendar.add(Calendar.MINUTE, interval);
                                                if (index >= 1) {//曲线第二个数据点入库时，无需再执行清理缓存操作
                                                    refreshKey = null;
                                                }

                                                DataObject dataObj = getDataObj(mpedIdStr, businessDataitemId, refreshKey, dataValue288);
                                                listDataObj.add(dataObj);
                                            } else {
                                                List dataValue = new ArrayList();

                                                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                                int point;
                                                if (point1 == 96) {
                                                    point = 96;
                                                } else {
//                                                    point = getDataPoint(databaseinterval, min) + interval / databaseinterval;
                                                    point = getDataPoint(interval, min) * databaseinterval + 1;
                                                }
                                                //TODO
                                                if (j == -1) {
                                                    j = 96 - point;
                                                }


                                                String dataDate;
                                                int dataTime;
                                                if (j >= index) {//抄表时间
                                                    dataDate = DateUtil.format(startTime, DateUtil.defaultDatePattern_YMD);
//                                                    dataTime = getDataPoint(databaseinterval, min) + interval / databaseinterval;
                                                    dataTime = getDataPoint(interval, min) * databaseinterval + 1;
                                                    calendar.add(Calendar.MINUTE, interval);
                                                } else {//当天
                                                    dataDate = DateUtil.format(calendarMt.getTime(), DateUtil.defaultDatePattern_YMD);
                                                    int mtMin = calendarMt.get(Calendar.HOUR_OF_DAY) * 60 + calendarMt.get(Calendar.MINUTE);
//                                                    dataTime = getDataPoint(databaseinterval, mtMin) + interval / databaseinterval;
                                                    dataTime = getDataPoint(interval, mtMin) * databaseinterval + 1;
                                                    calendarMt.add(Calendar.MINUTE, interval);
                                                }
                                                dataValue.add(mpedId);//ID
                                                dataValue.add(dataDate);//数据时标DATA_DATE
                                                dataValue.add(dataTime);//数据点数DATA_TIME

                                                dataValue.add(datapointflag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288

                                                CurveDataUtils.addDataType(dataValue, fn);
                                                dataValue.add(oo);//示值
                                                dataValue.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                                                dataValue.add(new Date());
                                                CurveDataUtils.addDataSrc(dataValue, fn, "0");
                                                if (index >= 1) {//曲线第二个数据点入库时，无需再执行清理缓存操作
                                                    refreshKey = null;
                                                }
                                                DataObject dataObj = getDataObj(mpedIdStr, businessDataitemId, refreshKey, dataValue);
                                                listDataObj.add(dataObj);
                                            }
                                        } catch (Exception e1) {
                                            logger.error("obValue has error:", e1);
                                            continue;
                                        }
                                    }
                                    //TODO
                                    pByfn(fn);
                                } catch (Exception e2) {
                                    logger.error("listValue has error:", e2);
                                    continue;
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("DataItemObject has error:", e);
                        continue;
                    }
                }
                PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                PromethuesAllDayUtil.toDataHubCountAllDay.labels("CURVE_DATA:TO_DATAHUB_COUNT", String.valueOf(afn)).inc(listDataObj.size());
                persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                list.add(persistentObject);//这个完整报文添加到了持久化list了
                context.setContent(list);
                return false;
            }
        }
        return true;
    }

    private void pByfn(int fn) {
        if (fn >= 92 && fn <= 95) {
            PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:DATA_OBJ_COUNT", "CUR").inc();
        }
        if (fn >= 89 && fn <= 91) {
            PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:DATA_OBJ_COUNT", "VOL").inc();
        }
        if (fn >= 105 && fn <= 108) {
            PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:DATA_OBJ_COUNT", "FACTOR").inc();
        }
        if (fn >= 81 && fn <= 88) {
            PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:DATA_OBJ_COUNT", "POWER").inc();
        }
        if ((fn >= 101 && fn <= 104) || (fn >= 145 && fn <= 148)) {
            PromethuesAllDayUtil.fromRedisCountFnAllDay.labels("CURVE_DATA:DATA_OBJ_COUNT", "READ").inc();
        }
    }

    private DataObject getDataObj(String mpedIdStr, String businessDataitemId, Object[] refreshKey, List dataValue)
            throws Exception {
        DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
        int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
        String shardId = dataHubTopic.getActiveShardList().get(index);
        DataObject dataObj = new DataObject(dataValue, refreshKey, dataHubTopic.topic(), shardId.toString());
        return dataObj;
    }

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

    private int getDataPoint(int interval, int min) {
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
