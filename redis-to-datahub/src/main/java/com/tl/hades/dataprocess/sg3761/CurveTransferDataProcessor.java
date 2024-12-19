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
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CurveTransferDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(CurveTransferDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET TRANSFER CURVE DATA===");
        Object obj = context.getContent();
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
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    try {
                        int pn = data.getPn();
                        int fn = data.getFn();
                        List<Object> data645List = new ArrayList<Object>();
                        try {
                            data645List = Split645DataPacket.split645Monitor(data);
                        } catch (Exception e) {
                            logger.error("无法解析成645");
                        }

                        if (data645List == null || data645List.size() == 0) {
                            continue;
                        }
                        if (data645List.size()<=3) {
                            continue;
                        }
                        //解析的List去掉电表地址【start_time,23,223,1123,123,......,meterAddr,dataitemId】
                        //得到【start_time,23,223,1123,123,......,dataitemId】
                        String meterAdd = data645List.remove(data645List.size() - 2).toString();



                        //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/

                        TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + meterAdd);
                        String mpedIdStr = terminalArchivesObject.getID();

                        if (null == mpedIdStr || "".equals(mpedIdStr)) {
                            logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                            continue;
                        }
                        BigDecimal mpedId = new BigDecimal(mpedIdStr);
                        //解析的List去掉数据项【start_time,23,223,1123,123,......】
                        //得到【start_time,23,223,1123,123,......】
                        String dataItemId = data645List.remove(data645List.size() - 1).toString();

                        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                        protocol3761ArchivesObject.setAfn(afn);
                        protocol3761ArchivesObject.setFn(fn);
                        String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, dataItemId).getBusiDataItemId();
                        if (businessDataitemId == null) {
                            continue;
                        }

                        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm");
                        //解析的List去掉起始事件【start_time,23,223,1123,123,......】
                        //得到【23,223,1123,123,......】
                        Date startTime = sdf.parse(data645List.remove(0).toString());
                        Calendar c = CommonUtils.getCalendar();
                        c.setTime(startTime);
                        c.add(Calendar.DATE, 1);
                        String mtDataDate = DateUtil.format(c.getTime(), "yyyy-MM-dd") + " 00:00:00";

                        Calendar calendarMt = CommonUtils.getCalendar();
                        calendarMt.setTime(DateUtil.parse(mtDataDate));


                        Calendar calendar = CommonUtils.getCalendar();
                        calendar.setTime(startTime);

                        int datapointflag = 1;
                        int interval = CurveDataUtils.getInterval(datapointflag);
                        //hch add
                        int databaseinterval = 1;
                        if(interval==60){
                            databaseinterval=4;
                        }else if(interval==30){
                            databaseinterval=2;
                        }else{
                            databaseinterval=1;
                        }

                        String type = CurveDataUtils.getType(businessDataitemId);

                        Object[] refreshKey = new Object[5];
                        refreshKey[0] = terminalArchivesObject.getTerminalId();
                        refreshKey[1] = mpedIdStr;
                        refreshKey[2] = "00000000000000"; // 数据召测时间
                        refreshKey[3] = businessDataitemId;
                        refreshKey[4] = protocolId;
                        int j = -1;
                        int point1 = 0;
                        for (int v = 0; v < data645List.size(); v++) {
                            try {
                                Object oo = data645List.get(v);//shizhi
                                if(oo.toString().equals(meterAdd)){
                                    logger.error("fuck oo equal meterAdd "+data645List.size()+" "+oo);
                                    continue;
                                }
                                if (oo == null) {
                                    int min1 = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                    if (point1 == 96) {
                                        calendarMt.setTime(DateUtil.parse(mtDataDate));
                                        calendarMt.add(Calendar.MINUTE, interval);
                                    } else {
//                                        point1 = CurveDataUtils.getDataPoint(databaseinterval, min1) + interval / databaseinterval;
                                        point1 = CurveDataUtils.getDataPoint(interval, min1)*databaseinterval +1;
                                        calendar.add(Calendar.MINUTE, interval);
                                    }
                                    continue;
                                }

                                if ("FF".contains(oo.toString())) {
                                    int min1 = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                    if (point1 == 96) {
                                        calendarMt.setTime(DateUtil.parse(mtDataDate));
                                        calendarMt.add(Calendar.MINUTE, interval);
                                    } else {
//                                        point1 = CurveDataUtils.getDataPoint(databaseinterval, min1) + interval / databaseinterval;
                                        point1 = CurveDataUtils.getDataPoint(interval, min1)*databaseinterval +1;
                                        calendar.add(Calendar.MINUTE, interval);
                                    }
                                    continue;
                                }

                                List dataValue = new ArrayList();

                                int min = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);//获取给定数据时标在当天所在的分钟数2
                                int point = 0;
                                if (point1 == 96) {
                                    point = 96;
                                } else {
//                                    point = CurveDataUtils.getDataPoint(databaseinterval, min) + interval / databaseinterval;
                                    point = CurveDataUtils.getDataPoint(interval, min)*databaseinterval + 1;
                                }
                                //TODO
                                if (j == -1) {
                                    j = 96 - point;
                                }


                                String dataDate = null;
                                int dataTime = 0;
                                if (j >= v) {//抄表时间
                                    dataDate = DateUtil.format(startTime, DateUtil.defaultDatePattern_YMD);
//                                    dataTime = CurveDataUtils.getDataPoint(databaseinterval, min) + interval / databaseinterval;
                                    dataTime = CurveDataUtils.getDataPoint(interval, min)*databaseinterval +1;
                                    calendar.add(Calendar.MINUTE, interval);
                                } else {//当天
                                    dataDate = DateUtil.format(calendarMt.getTime(), DateUtil.defaultDatePattern_YMD);
                                    int mtMin = calendarMt.get(Calendar.HOUR_OF_DAY) * 60 + calendarMt.get(Calendar.MINUTE);
//                                    dataTime = CurveDataUtils.getDataPoint(databaseinterval, mtMin) + interval / databaseinterval;
                                    dataTime = CurveDataUtils.getDataPoint(interval, mtMin)*databaseinterval +1;
                                    calendarMt.add(Calendar.MINUTE, interval);

                                }
                                dataValue.add(mpedId);//ID
                                dataValue.add(dataDate);//数据时标DATA_DATE
                                dataValue.add(dataTime);//数据点数DATA_TIME

                                dataValue.add(datapointflag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
                                dataValue.add(type);
                                dataValue.add(oo);//示值
                                dataValue.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                                dataValue.add(new Date());
                                if (CurveDataUtils.curveList.contains(businessDataitemId)) {
                                    dataValue.add("20");
                                }
                                if (v >= 1) {//曲线第二个数据点入库时，无需再执行清理缓存操作
                                    refreshKey = null;
                                }

                                CommonUtils.putToDataHub(businessDataitemId,mpedIdStr,dataValue, refreshKey, listDataObj);

                            } catch (Exception e1) {
                                logger.error("obValue has error:", e1);
                                continue;
                            }
                        }


                    } catch (Exception e) {
                        logger.error("DataItemObject has error:", e);
                        continue;
                    }
                }
                PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                list.add(persistentObject);//这个完整报文添加到了持久化list了
                context.setContent(list);
                return false;
            }
        }
        return true;
    }




}
