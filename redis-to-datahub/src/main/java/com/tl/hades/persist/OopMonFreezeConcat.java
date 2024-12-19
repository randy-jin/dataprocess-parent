package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.MeterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OopMonFreezeConcat {
    private final static Logger logger = LoggerFactory.getLogger(OopMonFreezeConcat.class);

    private final static String redisName = "Q_BASIC_DATA_50060200_R";

    public static void getDataList(MeterData meterData, int protocolId, String areaCode, String termAddr, String oopDataItemId, List<DataObject> listDataObj, String src) throws ParseException {
        List dataList = null;
        String meterAddr = null;
        String businessDataitemId;
        Date dates = null;//存储时标
        Date collDate = null;//采集成功时标

        List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
        if (doList != null && doList.size() > 0) {
            for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                if ("60420200".equals(dataObject.getDataItem())) {//抄表时间
                    dates = (Date) dataObject.getData();
                    //抄表时间
                } else if ("202A0200".equals(dataObject.getDataItem())) {
                    //表地址
                    meterAddr = String.valueOf(dataObject.getData());
                } else if ("60410200".equals(dataObject.getDataItem())) {
                    collDate = (Date) dataObject.getData();//采集成功时标
                } else {
                    if ("30110200".equals(oopDataItemId)) {
                        if (dataList == null) {
                            dataList = new ArrayList();
                        }
                        if (dataObject.getData() != null) {
                            dataList.add(String.valueOf(dataObject.getData()));
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
            if (dates == null) {
                dates = new Date();
            }
            if (collDate == null) {
                collDate = new Date();
            }
        }

        if (oopDataItemId == null || meterAddr == null || dataList == null || dates == null) {
            return;
        }

        //当前areaCode, termAddr, meterAddr并非真实数据，需要查询面向对象缓存后再查询档案缓存
        TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);

        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
        businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
        if (businessDataitemId == null) {
            logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + oopDataItemId);
            return;
        }

        String mpedId = terminalArchivesObject.getID();
        String orgNo = terminalArchivesObject.getPowerUnitNumber();

        Object[] refreshKey = null;
        if (src.equals("0")) {
            refreshKey = CommonUtils.refreshKey(terminalArchivesObject.getTerminalId(), mpedId, DateUtil.parse(DateUtil.format(dates), DateUtil.defaultDatePattern_YMD), businessDataitemId, protocolId);
        }

        List<Object> finalDataList = new ArrayList<>();

        switch (oopDataItemId) {
            case "10100200":
            case "10200200":
            case "10300200":
            case "10400200":
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //表中目前只要总，没有费率1~4
                    List<Object> res = (List<Object>) dataList.get(0);
                    finalDataList.add(mpedId);
                    String dataType;
                    switch (oopDataItemId) {
                        case "10100200":
                            dataType = "1";
                            break;
                        case "10200200":
                            dataType = "2";
                            break;
                        case "10300200":
                            dataType = "5";
                            break;
                        case "10400200":
                            dataType = "6";
                            break;
                        default:
                            dataType = "";
                    }
                    finalDataList.add(dataType);
                    finalDataList.add(collDate);
                    finalDataList.add(res.get(0));
                    finalDataList.add(sdf.parse(res.get(1).toString()));
                    finalDataList.add(orgNo);
                    finalDataList.add("00");
                    finalDataList.add(src);
                    finalDataList.add(new Date());
                    finalDataList.add(orgNo.substring(0, 5));
                    finalDataList.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
                    CommonUtils.putToDataHub(businessDataitemId, mpedId, finalDataList, refreshKey, listDataObj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }

    }
}
