package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.objpro.api.beans.MeterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 前后台电表事件
 */
public class OopEventConcat {
    private final static Logger logger = LoggerFactory.getLogger(OopEventConcat.class);
    private final static String redisName = "Q_BASIC_DATA_50040200_R";
    public static void getDataList(MeterData meterData, int protocolId, String areaCode, String termAddr, List<DataObject> listDataObj, int src, String oad) throws Exception {
        List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
        if (doList == null || doList.size() == 0) {
            return;
        }
        List dataList = null;
        String meterAddr = null;
        String businessDataitemId;
        Date dates = null;//存储时标
        String oopDataItemId = oad;
        if (doList != null && doList.size() > 0) {
            for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                if (dataObject.getDataItem().equals("20220200")) {
                    continue;
                }
                if (dataObject.getDataItem().equals("60420200")) {//抄表时间
                    dates = (Date) dataObject.getData();
                    //抄表时间
                } else if (dataObject.getDataItem().equals("202A0200")) {
                    //表地址
                    meterAddr = String.valueOf(dataObject.getData());
                } else {
                    if ("30110200".equals(oopDataItemId)) {
                        if (dataList == null) {
                            dataList = new ArrayList();
                        }
                        if (dataObject.getData() != null) {
                            if (dataObject.getData() instanceof List) {
                                dataList = (List) dataObject.getData();
                            } else {
                                dataList.add(String.valueOf(dataObject.getData()));
                            }
                        }
                    } else {
                        oopDataItemId = dataObject.getDataItem();//数据项
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
            }
        }

        if (dates == null) {
            dates = new Date();
        }
        if (meterAddr == null) {
            meterAddr = meterData.getMeterAddr();
        }
        if (oopDataItemId == null || meterAddr == null || dataList == null || dates == null) {
            return;
        }

        //获取档案信息
        TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
        String orgNo = terminalArchivesObject.getPowerUnitNumber();
        String mtId = terminalArchivesObject.getMeterId();
        if (null == mtId || "".equals(mtId)) {
            logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
            return;
        }
        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
        businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
        if (businessDataitemId == null) {
            businessDataitemId = oopDataItemId;
        }

        List<Object> dataListFinal = new ArrayList<Object>();
        if (oopDataItemId.equals("03300D00")) {//开表盖总次数　特殊处理,数据获取第一位不是一个list  dataList:[0000,03300D00]
            String mpedId = terminalArchivesObject.getID();
            if (null == mpedId || "".equals(mpedId)) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                return;
            }
            dataListFinal.add(new BigDecimal(mpedId));//mped_id
            dataListFinal.add(businessDataitemId);//dataitem_id
            dataListFinal.add(new BigDecimal(terminalArchivesObject.getTerminalId()));//terminal_id
            dataListFinal.add(Integer.valueOf(dataList.get(0).toString()));//total_num
            dataListFinal.add(orgNo.substring(0, 5));//shard_no
            dataListFinal.add(new Date());//insert_time

        } else {
            List endList;
            if("30110200".equals(oopDataItemId)){
                endList=dataList;
            }else{
                endList =(List)dataList.get(0);
            }
            dataListFinal = OopEventDataPush.getDataList(endList, oopDataItemId, mtId, orgNo);
        }

        if (dataListFinal == null || dataListFinal.isEmpty()) {
            return;
        }
        if (oopDataItemId.startsWith("0311000")) {
            Object checkDate = dataListFinal.get(dataListFinal.size() - 1);
            boolean err = DateFilter.betweenDay(checkDate, 0, 180);
            if (!err) {
                businessDataitemId = "no_power_err";
            }
        }
        Object[] refreshKey = null;
        if (src == 0) {
            refreshKey = new Object[5];
            refreshKey[0] = terminalArchivesObject.getTerminalId();
            refreshKey[1] = terminalArchivesObject.getID();
            refreshKey[2] = "00000000000000"; // 数据召测时间
            refreshKey[3] = businessDataitemId;
            refreshKey[4] = protocolId;
        }
        CommonUtils.putToDataHub(businessDataitemId, mtId, dataListFinal, refreshKey, listDataObj);
    }

}
