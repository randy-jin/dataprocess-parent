package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.MeterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class OopCurveConcat {
    private final static Logger logger = LoggerFactory.getLogger(OopCurveConcat.class);

    private static ThreadLocal<Calendar> currentCalendar = new ThreadLocal<Calendar>();
    private static ThreadLocal<SimpleDateFormat> currentFormat = new ThreadLocal<SimpleDateFormat>();
    private final static String redisName = "Q_BASIC_DATA_50020200_R";

    public static void getDataList(MeterData meterData, int protocolId, String areaCode, String termAddr, int dar , List<DataObject> listDataObj, String src) throws Exception{
        try {
            List dataList = null;
            String meterAddr = null;
            String businessDataitemId;
            String dataitemid = null;
            Date collDate = null;
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
            if (doList != null && doList.size() > 0) {
                for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                    if (dataObject.getDataItem().equals("60420200")) {//抄表时间
                        if (dataObject.getData() instanceof Date) {
                            collDate = (Date) dataObject.getData();
                        }
                    } else if (dataObject.getDataItem().equals("202A0200")) {
                        //表地址
                        meterAddr = String.valueOf(dataObject.getData());
                    } else {
                        dataitemid = dataObject.getDataItem();
                        if (dataObject.getData() != null) {
                            if (dataObject.getData() instanceof List) {
                                dataList = (List) dataObject.getData();
                            } else {
                                dataList = new ArrayList();
                                dataList.add(String.valueOf(dataObject.getData()));
                            }
                        }
                    }
                }
            }//010004540618
            if (dataitemid == null || collDate == null || meterAddr == null || dataList == null) {
                return;
            }
            boolean allNull = OopDataUtils.allNull(dataList);
            if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                return;
            }

            //获取档案信息
            TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
            String mpedIdStr = terminalArchivesObject.getID();
            if (null == mpedIdStr || "".equals(mpedIdStr)) {
                logger.error("入库类型"+src+"无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                return;
            }
            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, dataitemid);
            if (businessDataitemId == null) {
                logger.error("入库类型"+src+"无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + dataitemid);
                return;
            }
            int databaseinterval = 15;
            String pointFlag = "1";
            boolean isTrue = false;
            if (dar == 1) {
                databaseinterval = 5;
                pointFlag = "254";
                isTrue = true;
            }
            BigDecimal mpedId = new BigDecimal(mpedIdStr);

            Calendar calendar = getCalendar();
            calendar.setTime(collDate);

            Object[] refreshKey = new Object[5];
            refreshKey[0] = terminalArchivesObject.getTerminalId();
            refreshKey[1] = mpedIdStr;
            refreshKey[2] = DateUtil.parse(DateUtil.format(collDate), DateUtil.defaultDatePattern_YMD); // 数据召测时间
            refreshKey[3] = businessDataitemId;
            refreshKey[4] = protocolId;
            String business = businessDataitemId;
            for (int index = 0; index < dataList.size(); index++) {
                Object oo = dataList.get(index);
                if(oo==null){
                    continue;
                }
                List dataValue = new ArrayList();
                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                String dataDate = DateUtil.format(collDate, DateUtil.defaultDatePattern_YMD);

                dataValue.add(mpedId);//ID
                dataValue.add(dataDate);//数据时标DATA_DATE
                int a =getDataPoint(databaseinterval, min);

                dataValue.add(a + 1);//数据点数DATA_TIME
                dataValue.add(pointFlag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
                CurveDataUtils.addDataTypeByOop(dataValue, businessDataitemId, index);//data_type
                dataValue.add(oo);//示值
                dataValue.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                if (isTrue && !"232005".equals(businessDataitemId)) {
                    dataValue.add("00");
                    dataValue.add(src);
                    dataValue.add(new Date());
                    dataValue.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    business = "vol_288";
                } else {
                    if (isTrue && "232005".equals(businessDataitemId)) {
                        business = "power_288";
                    }
                    dataValue.add(new Date());
                }
                if ("1".equals(pointFlag) && CurveDataUtils.curveList.contains(business)) {
                    if (protocolId == 8) {
                        dataValue.add("20");
                    } else {
                        dataValue.add("0");
                    }
                }
                if (index >= 1) {//曲线第二个数据点入库时，无需再执行清理缓存操作
                    refreshKey = null;
                }
                CommonUtils.putToDataHub(business,mpedIdStr,dataValue,refreshKey,listDataObj);
                if ("231005".equals(businessDataitemId) || "231006".equals(businessDataitemId) || "231007".equals(businessDataitemId) || "231008".equals(businessDataitemId)) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private static int getDataPoint(int interval, int min) {
        return min / interval;
    }
}
