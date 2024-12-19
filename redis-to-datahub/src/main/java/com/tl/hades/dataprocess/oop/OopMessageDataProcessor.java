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
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.CurveDataUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 报文压缩入库
 */
public class OopMessageDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopFreezeFrontDataProcessor.class);
    private final static String redisName = "Q_BASIC_DATA_50020200_R";

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET OOP MESSAGE DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            int protocolId = 8;
            List<PersistentObject> list = new ArrayList<>();//持久类
            List<DataObject> listDataObj = new ArrayList<>();//数据类
            Date now = new Date();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(now);
            calendar.add(Calendar.DATE, -1);
            Date  dataTime = calendar.getTime();
            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                try {
                    List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
                    if (doList == null || doList.size() < 0) {continue;}

                        List firstDataList = null;
                        for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                            firstDataList = (List) dataObject.getData();
                            String meterAddr = (String) firstDataList.get(0);
                            String dataitemid = (String) firstDataList.get(1);
                            String pointFlag = checkDatabaseinterval((String) firstDataList.get(2));
                            List dataList = (List) firstDataList.get(3);
                            TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                            String mpedIdStr = terminalArchivesObject.getID();
                            if (null == mpedIdStr || "".equals(mpedIdStr)) {
                                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                                continue;
                            }
                            BigDecimal mpedId = new BigDecimal(mpedIdStr);
                            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                            String businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, dataitemid.toUpperCase());
                            if (businessDataitemId == null) {
                                logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + dataitemid);
                                continue;
                            }
                            for (Object data : dataList) {
                                String[] d = data.toString().split("\\|");
                                String v = d[1];
                                String[] split = v.split(",");
                                for (int i = 0; i < split.length; i++) {
                                    String value = split[i];
                                    if ("null".equals(value)) {
                                        continue;
                                    }
                                    List dataValue = new ArrayList();
                                    dataValue.add(mpedId);//ID
                                    dataValue.add(DateUtil.format(dataTime, DateUtil.defaultDatePattern_YMD));//数据时标DATA_DATE
                                    dataValue.add(d[0]);//数据点数DATA_TIME
                                    dataValue.add(pointFlag);//数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
                                    CurveDataUtils.addDataTypeByOop(dataValue, businessDataitemId, i);//data_type
                                    dataValue.add(Double.parseDouble(value));//示值
                                    dataValue.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO
                                    dataValue.add(new Date());
                                    dataValue.add("0");
                                    CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataValue, null, listDataObj);
                                }
                            }
                        }
                    }catch (Exception e){

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

    //数据点数标志DATA_POINT_FLAG 1:96 2:48 254:288
    public static String checkDatabaseinterval(String databaseinterval){
        String pointFlag="1";
        switch (databaseinterval){
            case "15":
                pointFlag="1";
                break;
            case "30":
                pointFlag="2";
                break;
            case "60":
                pointFlag="3";
                break;
            case "5":
                pointFlag = "254";//288
                break;
            case "1":
                pointFlag = "255";//1440
                break;
        }
          return pointFlag;
    }


}
