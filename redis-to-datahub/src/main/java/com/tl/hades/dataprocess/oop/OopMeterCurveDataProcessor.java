package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 批量曲线
 *
 * @author easb
 * dateMap1.put("00100200", "231005");	//日测量点总电能示值曲线		1
 * dateMap1.put("20000200", "232013");//日测量点电压曲线 1 2 3
 * dateMap1.put("20010200", "232016");//日测量点电流曲线 1 2 3
 * dateMap1.put("200A0200", "232020");//日测量点功率因数曲线 0 1 2 3
 * dateMap1.put("20040200", "232005");//日测量点功率曲线 1 2 3 4
 * dateMap1.put("20050200", "232009");//日测量点无功功率曲线 wu 5 6 7 8
 * oop-curve-dataprocessor.xml
 */
public class OopMeterCurveDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopMeterCurveDataProcessor.class);

    private final static String redisName = "Q_BASIC_DATA_50020200_R";


    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET OOP METER CURVE DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            int protocolId = ternData.getCommandType();
            if (protocolId != 8) {
                protocolId = 9;
            }
            List<PersistentObject> list = new ArrayList<>();//持久类
            List<DataObject> listDataObj = new ArrayList<>();//数据类

            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                try {
                    List dataListAll = new ArrayList();
                    String meterAddr = meterData.getMeterAddr();
                    String businessDataitemId;
                    String dataitemid = null;
                    Object v;
                    Date collDate = new Date();
                    List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
                    if (doList != null && doList.size() > 0) {
                        if (doList.size() == 3) {
                            for (com.tl.hades.objpro.api.beans.DataObject doj : doList) {
                                if ("20210200".equals(doj.getDataItem())) {
                                    Object pointTime = doj.getData();
                                    dataListAll.add(pointTime);
                                } else if ("202A0200".equals(doj.getDataItem())) {
                                    Object o = doj.getData();
                                    if (o != null) {
                                        meterAddr = doj.getData().toString();
                                    }
                                } else {
                                    dataitemid = doj.getDataItem();
                                    v = doj.getData();
                                    if (v != null) {
                                        dataListAll.add(v);
                                    }
                                }
                            }
                        } else {
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
                                            dataListAll = (List) dataObject.getData();
                                        } else {
                                            dataListAll.add(String.valueOf(dataObject.getData()));
                                        }
                                    }
                                }
                            }
                        }
                    }//010004540618
                    if (dataitemid == null || meterAddr == null) {
                        continue;
                    }
                    List dataList;
                    if (dataListAll.get(0) instanceof List) {
                        dataList = (List) dataListAll.get(0);
                    } else {
                        dataList = dataListAll;
                    }
                    boolean allNull = OopDataUtils.allNull(dataList);
                    if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                        continue;
                    }


                    //获取档案信息
                    TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                    String mpedIdStr = terminalArchivesObject.getID();
                    if (null == mpedIdStr || "".equals(mpedIdStr)) {
                        logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                        continue;
                    }
                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, dataitemid);
                    if (businessDataitemId == null) {
                        logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + dataitemid);
                        continue;
                    }
                    String pointFlag = "1";
                    //点位间隔
                    int databaseinterval = 15;
                    BigDecimal mpedId = new BigDecimal(mpedIdStr);


                    Object[] refreshKey = new Object[5];
                    refreshKey[0] = terminalArchivesObject.getTerminalId();
                    refreshKey[1] = mpedIdStr;
                    refreshKey[2] = collDate == null ? "000000000000" : DateUtil.parse(DateUtil.format(collDate), DateUtil.defaultDatePattern_YMD); // 数据召测时间
                    refreshKey[3] = businessDataitemId;
                    refreshKey[4] = "9";
                    String business = businessDataitemId;
                    String dateStr = (String) dataList.get(0);
                    Calendar calendar = Calendar.getInstance();
                    Date eventDate;
                    try {
                        eventDate = DateUtil.parse(dateStr, "yyyy-MM-dd HH:mm");
                        calendar.setTime(eventDate);
                    } catch (ParseException e) {
                        continue;
                    }
                    int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                    int initPoint = min / databaseinterval + 1;
                    String dataDate = DateUtil.format(eventDate, DateUtil.defaultDatePattern_YMD);
                    for (int index = 1, size = dataList.size(); index < size; index++) {
                        try {
                            Object oo = dataList.get(index);
                            if (oo.toString().contains("FF") || oo == null) {
                                initPoint++;
                                continue;
                            }
                            List dataValue = new ArrayList();
                            dataValue.add(mpedId);//ID
                            dataValue.add(dataDate);//数据时标DATA_DATE
                            dataValue.add(initPoint);//数据点数DATA_TIME
                            dataValue.add(pointFlag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
                            CurveDataUtils.addMeterCurveTypeByOop(dataValue, businessDataitemId);//data_type
                            dataValue.add(oo);//示值
                            dataValue.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                            dataValue.add(new Date());
                            if (CurveDataUtils.curveList.contains(business)) {
                                dataValue.add("20");
                            }
                            if (index > 1) {//曲线第二个数据点入库时，无需再执行清理缓存操作
                                refreshKey = null;
                            }
                            CommonUtils.putToDataHub(business, mpedIdStr, dataValue, refreshKey, listDataObj);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        initPoint++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
            persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
            list.add(persistentObject);//这个完整报文添加到了持久化list了
            context.setContent(list);
            return false;
        }

        return true;
    }
}
