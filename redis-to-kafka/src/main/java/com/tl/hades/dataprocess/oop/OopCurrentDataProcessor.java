package com.tl.hades.dataprocess.oop;

import com.datasource.DataSourceControl;
import com.datasource.DataSourceObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.OopMeterEvent;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 实时批量
 *
 * @author easb
 * oop-dataprocess-current-spring.xml
 */
public class OopCurrentDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(OopCurrentDataProcessor.class);
    private final static String redisName = "Q_BASIC_DATA_50040200_R";

    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();

    @AutoInsert
    private DataSourceControl dataSourceControl;

    @Override
    protected String doCanFinish(Object object) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET OOP Current  DATA===");
        TerminalData ternData = ClinkedList.getObject(object, TerminalData.class);
        String areaCode = ternData.getAreaCode();
        String termAddr = ternData.getTerminalAddr();
        int protocolId = ternData.getCommandType();
        if (protocolId != 8) {
            protocolId = 9;
        }
        List<DataSourceObject> listDataObj = new ArrayList<>();//数据类
        List<MeterData> meterList = ternData.getMeterDataList();
        for (MeterData meterData : meterList) {
            List dataList = null;
            String meterAddr = null;
            String businessDataitemId = null;
            Date dates = null;//存储时标
            Date collDate = null;//采集成功时标
            String oopDataItemId = null;
            String mpedIdStr = null;
            String meterId = null;
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
                }//20040201   0400010C
                if (dates == null) {
                    dates = new Date();
                }
            }
            if (!"F2090700".equals(oopDataItemId) && !"F2090200".equals(oopDataItemId) && !"45000500".equals(oopDataItemId) && !"F2090500".equals(oopDataItemId) && !"FF100200".equals(oopDataItemId)) {
                if (oopDataItemId == null || meterAddr == null || dataList == null || dates == null) {
                    continue;
                }
            }
            //获取前置写入redis中的真实表地址
            String realAddr = TerminalArchives.getInstance().getRealMeterAddrFromLocalOop(areaCode, termAddr, meterAddr);
            //拼接终端地址用于后续查找档案
            String full = areaCode + termAddr;
            String realAc = full.substring(3, 7);
            String realTd1 = full.substring(7, 12);
            String realTd = String.valueOf(Integer.parseInt(realTd1));
            //获取档案信息
            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesOop(realAc, realTd, realAddr);
            String orgNo = terminalArchivesObject.getPowerUnitNumber();
            if (orgNo == null || "".equals(orgNo)) {
                logger.error("无法从缓存获取正确的档案信息real:" + realAc + "_" + realTd + "_COMMADDR" + realAddr);
                continue;
            }
            if (!"F2090700".equals(oopDataItemId) && !"F2090200".equals(oopDataItemId) && !"45000500".equals(oopDataItemId) && !"F2090500".equals(oopDataItemId) && !"FF100200".equals(oopDataItemId)) {
                mpedIdStr = terminalArchivesObject.getID();
                meterId = terminalArchivesObject.getMeterId();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + realAc + "_" + realTd + "_COMMADDR" + realAddr);
                    continue;
                }
            } else {
                mpedIdStr = terminalArchivesObject.getTerminalId();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + realAc + "_" + realTd + "_COMMADDR" + realAddr);
                    continue;
                }
            }
            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
            if (businessDataitemId == null) {
                logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + oopDataItemId);
                continue;
            }
            List<Object> dataListFinal = new ArrayList<Object>();

            Object[] refreshKey = new Object[5];
            refreshKey[0] = terminalArchivesObject.getTerminalId();
            refreshKey[1] = mpedIdStr;
            refreshKey[2] = "00000000000000"; // 数据召测时间
            refreshKey[3] = businessDataitemId;
            refreshKey[4] = protocolId;

            if (oopDataItemId.startsWith("1")) {
                dataListFinal.add(mpedIdStr);
                if (oopDataItemId.equals("10100200")) {//正向有功需量
                    dataListFinal.add("1");//flag
                } else if (oopDataItemId.equals("10200200")) {//反向有功需量
                    dataListFinal.add("5");//flag
                } else if (oopDataItemId.equals("10300200")) {//组合无功1
                    dataListFinal.add("2");//flag
                } else if (oopDataItemId.equals("10400200")) {//组合无功2
                    dataListFinal.add("6");//flag
                }
                dataListFinal.add(dates);//抄表时间
                //去掉需量时间判断
                Object demand_v = ((List) dataList.get(0)).get(0);
                Object d = ((List) dataList.get(0)).get(1);//demandtime
                dataListFinal.add(demand_v);
                try {
                    dataListFinal.add(d);
                } catch (Exception e2) {
                }
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
                dataListFinal.add("00");//未分析
                if (protocolId == 8) {
                    dataListFinal.add("30");
                } else {
                    dataListFinal.add("10");
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                dataListFinal.add(DateUtil.format(OopDataUtils.timeAddOrMinus(dates, -1), DateUtil.defaultDatePattern_YMD));
            } else if ("20040201".equals(oopDataItemId)) {
                dataListFinal.add(meterId);
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                dataListFinal.add("1");//flag
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
                dataListFinal.add(dataList.get(0));
                dataListFinal.add(new Date());//电压时间点
                dataListFinal.add("00");//未分析
                //区分直抄137/预抄133
                if (ternData.getCommandType() == 8) {
                    dataListFinal.add("30");//直抄
                } else {
                    dataListFinal.add("10");//预抄
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            } else if ("04000501".equals(oopDataItemId)) {//状态字1入库
                dataListFinal.add(mpedIdStr);
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位

                StringBuffer sb = new StringBuffer();
                //dataList中序列化问题，导致第一个0正常，后面零前多了一个字符
                sb.append(dataList.remove(0));
                for (Object o : dataList) {
                    sb.append(((String) o).charAt(1));
                }
                for (int i = 0; i < 11; i++) {
                    sb.append("0");
                }
                dataListFinal.add(sb.toString());
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                dataListFinal.add(new Date());//入库时间
            } else if ("04000503".equals(oopDataItemId)) {//状态字3入库
                dataListFinal.add(mpedIdStr);
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位

                StringBuffer sb = new StringBuffer();
                sb.append(dataList.remove(0));
                for (Object o : dataList) {
                    sb.append(((String) o).charAt(1));
                }
                for (int i = 0; i < 6; i++) {
                    sb.append("0");
                }
                dataListFinal.add(sb.toString());
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                dataListFinal.add(new Date());//入库时间
            } else if ("0400010C".equals(oopDataItemId)) {//电表时钟
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                dataListFinal.add(mpedIdStr);
                String date = (String) dataList.get(0);
                if (!date.contains("&")) {
                    continue;
                }
                String[] str = date.split("&");
                if (str.length != 3) {
                    continue;
                }
                StringBuffer sb = new StringBuffer();
                sb.append(str[0]);
                sb.append(" ");
                sb.append(str[2]);
                dataListFinal.add(sb.toString());
                dataListFinal.add(dates);
                dataListFinal.add("03");
                dataListFinal.add(new Date());
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            } else if ("F2090700".equals(oopDataItemId)) {//节点信息
                String taskid = idGenerator.next();
                for (int i = 0; i < dataList.size(); i++) {
                    List dlist = (List) dataList.get(i);
                    List nodeList = (List) dlist.get(4);
                    String nodeAddr = dlist.get(0).toString();
                    if (nodeAddr.contains("F")) {
                        continue;
                    }
                    if (null == mpedIdStr || "".equals(mpedIdStr)) {
                        logger.error("无法从缓存获取正确的档案信息real:" + realAc + "_" + realTd + "_COMMADDR" + realAddr);
                        continue;
                    }
                    List alllList = new ArrayList();
                    alllList.add(mpedIdStr);//终端id
                    alllList.add(taskid);
                    alllList.add(nodeAddr);
                    alllList.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                    alllList.add(new Date());//采集日期
                    alllList.add(orgNo);
                    alllList.add(null);//采集点编号CP_NO
                    alllList.add(realAc);
                    alllList.add(realTd);
                    alllList.add(dataList.size());//节点总数
                    alllList.add(dataList.size());//应答数量
                    alllList.add(i + 1);//节点序号
                    alllList.add(1);///起始序号
                    alllList.add(new BigDecimal(dlist.get(2).toString()));//节点标识
                    alllList.add(new BigDecimal(dlist.get(3).toString()));//代理节点标识
                    alllList.add(nodeList.get(0));//节点层级
                    alllList.add(nodeList.get(1));//节点角色
                    alllList.add(dlist.get(1));//节点类型
                    alllList.add(null);//相位信息
                    alllList.add(null);//线路异常识别码
                    alllList.add(null);
                    alllList.add(null);
                    alllList.add(null);
                    alllList.add(null);
                    alllList.add(null);
                    alllList.add(orgNo.substring(0, 5));
                    alllList.add(new Date());
                    DataSourceObject dso = new DataSourceObject(businessDataitemId, refreshKey, alllList);
                    listDataObj.add(dso);//将组好的数据类添加到数据列表
                }
                continue;
            } else if ("F2090200".equals(oopDataItemId)) {//终端本地通信模块信息
                dataList = (List) dataList.get(0);
                if (dataList.size() != 3) {
                    continue;
                }
                String str = (String) dataList.get(0);
                List datList = (List) dataList.get(1);
                List cpList = (List) dataList.get(2);
                boolean type = str.toUpperCase().contains("PLC");
                String modId = null;
                if (str.contains(";")) {
                    String[] stSplit = str.split(";");
                    if (stSplit.length > 2) {
                        String val = stSplit[2];
                        if (val.contains("=")) {
                            String[] mod = val.split("=");
                            if (mod.length > 1) {
                                modId = mod[1];
                            }
                        }
                    }
                }
                if (modId != null) {
                    modId = "0" + modId;
                }

                dataListFinal.add(mpedIdStr);
                dataListFinal.add(orgNo);
                dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
                dataListFinal.add(cpList.get(0));//厂商代码
                dataListFinal.add(cpList.get(1));//芯片代码
                dataListFinal.add(null);//版本
                dataListFinal.add(cpList.get(2));//版本日期
                dataListFinal.add(null);//模块地址
                dataListFinal.add(null);//模块ID长度
                dataListFinal.add(null);//模块ID格式
                dataListFinal.add(null);//模块ID
                dataListFinal.add(null);//组合ID
                dataListFinal.add(datList.get(0));//波特率
                dataListFinal.add(datList.get(1));//校验位
                dataListFinal.add(datList.get(2));//数据位
                dataListFinal.add(datList.get(3));//停止位
                dataListFinal.add(datList.get(4));//流控
                if (type) {
                    dataListFinal.add("1");//模块类型
                } else {
                    dataListFinal.add("2");//模块类型
                }
                dataListFinal.add(orgNo.substring(0, 5));
                dataListFinal.add(new Date());
            } else if ("45000500".equals(oopDataItemId)) {//终端远程通信模块信息 end
                logger.info("进入方法");
                if (!(dataList instanceof List)) {
                    continue;
                }
                dataList = (List) dataList.get(0);
                if (dataList.size() == 0) {
                    continue;
                }
                dataListFinal.add(mpedIdStr);
                dataListFinal.add(orgNo);
                dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
                dataListFinal.add(dataList.get(0));//厂商编码
                dataListFinal.add(String.valueOf(dataList.get(5)).trim());//模块型号
                dataListFinal.add(dataList.get(1));//主用软件版本
                dataListFinal.add(OopDataUtils.versionDate(dataList.get(2)));//终端软件发布日期
                dataListFinal.add(dataList.get(3));//硬件版本
                dataListFinal.add(OopDataUtils.versionDate(dataList.get(4)));//终端硬件发布日期
                dataListFinal.add(null);//sim卡 CCID号
                dataListFinal.add(null);//sim卡号
                dataListFinal.add(null);//模块ID长度
                dataListFinal.add(null);//模块ID格式
                dataListFinal.add(null);//模块ID
                dataListFinal.add(null);//组合ID
                dataListFinal.add(null);//运行网络类型
                dataListFinal.add(orgNo.substring(0, 5));
                dataListFinal.add(new Date());
            } else if ("F2090500".equals(oopDataItemId)) {//集中器HPLC模块芯片召测
                if (dataList == null || dataList.size() == 0) continue;
                for (Object datas : dataList) {
                    dataList = (List) datas;
                    if (dataList.size() == 0) {
                        continue;
                    }
                    List finalList = new ArrayList<>();
                    //[1, 000413416486, type=2;mfrs= ;idformat=; id=;
                    String longStr = dataList.get(2).toString();
                    String[] dateStrs = longStr.split(";");

                    finalList.add(dateStrs[3].substring(dateStrs[3].indexOf("=") + 1, dateStrs[3].length()));//芯片ID号资产编号
                    finalList.add(orgNo);//所属单位
                    finalList.add(termAddr);//集中器械地址
                    finalList.add(dateStrs[0].substring(dateStrs[0].indexOf("=") + 1, dateStrs[0].length()));//leixing
                    finalList.add(dataList.get(1));//设备地址
                    finalList.add(null);//芯片软件版本信息
                    finalList.add(dateStrs[3].substring(dateStrs[4].indexOf("=") + 1, dateStrs[4].length()));//芯片型号
                    finalList.add(dateStrs[1].substring(dateStrs[1].indexOf("=") + 1, dateStrs[1].length()));//芯片厂商
                    finalList.add(null);//状态
                    finalList.add(null);//建档日期
                    finalList.add(null);//更新时间
                    finalList.add(null);//注销日期
                    finalList.add(null);//备注
                    finalList.add("14");//数据来源
                    finalList.add(orgNo.substring(0, 5));
                    DataSourceObject dso = new DataSourceObject(businessDataitemId, refreshKey, finalList);
                    listDataObj.add(dso);
                }
            } else if ("FF100200".equals(oopDataItemId)) {//错误信息
                if (dataList.size() == 0) {
                    continue;
                }
                for (Object ob : dataList) {
                    List dL = (List) ob;
                    if (dL.size() < 3) {
                        continue;
                    }
                    List alllList = new ArrayList();//sqlmapping list
                    List lis = (List) dL.get(2);
                    String nodeInfo = (String) lis.get(0);
                    alllList.add(mpedIdStr);
                    alllList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    alllList.add(idGenerator.next());
                    alllList.add(dL.get(0));
                    alllList.add(dL.get(1));
                    alllList.add(nodeInfo.substring(0, 1));//表档案状态
                    alllList.add(nodeInfo.substring(1, 3));//台区状态
                    alllList.add(nodeInfo.substring(3, 6));//相序
                    alllList.add(nodeInfo.substring(6, 7));//线路状态
                    alllList.add(dL.get(3));//节点台区地址
                    alllList.add(orgNo.substring(0, 5));
                    alllList.add(new Date());

                    DataSourceObject dso = new DataSourceObject(businessDataitemId, refreshKey, alllList);
                    listDataObj.add(dso);
                }
                continue;
            } else if ("E_METER_EVENT_NO_POWER_SOURCE".equals(OopMeterEvent.itemMap.get(oopDataItemId))) {
                List oopList = OopMeterEvent.putToDataHub(oopDataItemId, dataList, meterId, orgNo);
                if (oopList != null && !oopList.isEmpty()) {
                    DataSourceObject dso = new DataSourceObject(oopDataItemId, refreshKey, oopList);
                    listDataObj.add(dso);
                }
            } else {
                boolean allNull = OopDataUtils.allNull(dataList);
                if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                    continue;
                }
                if (dataList.get(0) == null) {
                    continue;
                }
                dataListFinal = OopDataUtils.getDataList("current", dataList, oopDataItemId, dates, mpedIdStr, dates);
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                dataListFinal.add("00");//未分析
                if (protocolId == 8) {
                    dataListFinal.add("30");
                } else {
                    dataListFinal.add("10");
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                dataListFinal.add(DateUtil.format(OopDataUtils.timeAddOrMinus(dates, -1), DateUtil.defaultDatePattern_YMD));
                businessDataitemId = "other_freeze";
            }
            if (!dataListFinal.isEmpty()) {
                DataSourceObject dso = new DataSourceObject(businessDataitemId, refreshKey, dataListFinal);
                listDataObj.add(dso);
            }
        }
        dataSourceControl.writeToDataSource(listDataObj);
        return null;
    }

    @Override
    protected void cacheException(Object o, Exception e) {
        e.printStackTrace();
    }
}