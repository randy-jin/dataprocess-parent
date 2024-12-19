package com.tl.hades.dataprocess.sg3761;

import com.datasource.DataSourceControl;
import com.datasource.DataSourceObject;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.persist.FreezeDataUtils;
import com.tl.hades.persist.FzxUtils;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FreezeFrontDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(FreezeFrontDataProcessor.class);

    @AutoInsert
    private DataSourceControl dataSourceControl;


    @Override
    protected String doCanFinish(Object obj) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET FREEZE Front DATA===");
        TerminalDataObject terminalDataObject = ClinkedList.getObject(obj, TerminalDataObject.class);
        int afn = terminalDataObject.getAFN();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        List<DataSourceObject> listDataObj = new ArrayList<>();//数据类
        //多测量点pnfn
        for (DataItemObject data : terminalDataObject.getList()) {
            List dataList = data.getList();//本测量点的数据项xxx.xx...
            if (data.getFn() != 43 && data.getFn() != 139 && data.getFn() != 140) {
                boolean allNull = FreezeDataUtils.allNull(dataList);
                if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                    continue;
                }
            }
            int pn = data.getPn();
            int fn = data.getFn();
            logger.info("===MAKE " + afn + "_" + fn + " DATA===");

            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(1);
            protocol3761ArchivesObject.setAfn(afn);
            protocol3761ArchivesObject.setFn(fn);
            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
            if (afn == 13 && fn == 139) {
                List dataLists = (List) dataList.get(2);
                for (int i = 0; i < dataLists.size(); i++) {
                    List meterList = (List) dataLists.get(i);
                    String maddr = (String) meterList.get(1);
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectMeter(areaCode, terminalAddr, "COMMADDR" + maddr, "MI" + maddr);
                    if (terminalArchivesObject == null) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    String mpedIdStr = terminalArchivesObject.getID();
                    if (null == mpedIdStr || "".equals(mpedIdStr)) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    List dataListFinal = new ArrayList<Object>();
                    dataListFinal.add(mpedIdStr);
                    dataListFinal.add(dataList.get(1));
                    dataListFinal.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                    dataListFinal.add(meterList.get(1));
                    dataListFinal.add(meterList.get(0));
                    dataListFinal.add(meterList.get(2));
                    dataListFinal.add(meterList.get(3));

                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                    dataListFinal.add("00");
                    //区分直抄8/预抄9
                    dataListFinal.add("4");//预抄
                    dataListFinal.add(new Date());//入库时间3531778244
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                    DataSourceObject dso = new DataSourceObject(businessDataitemId, null, dataListFinal);
                    listDataObj.add(dso);
                }
            } else if (afn == 13 && fn == 140) {
                List meterList = (List) ((List) dataList.get(4)).get(0);
                String maddr = (String) meterList.get(1);
                TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectMeter(areaCode, terminalAddr, "COMMADDR" + maddr, "MI" + maddr);
                if (terminalArchivesObject == null) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    continue;
                }
                String mpedIdStr = terminalArchivesObject.getID();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                    continue;
                }
                List dataListFinal = new ArrayList<Object>();
                dataListFinal.add(mpedIdStr);
                dataListFinal.add(dataList.get(3));//数据类型
                dataListFinal.add(FzxUtils.dataTime(String.valueOf(dataList.get(1)), (Date) dataList.get(0)));//数据时间类型
                dataListFinal.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                dataListFinal.add(dataList.get(1));//数据点标志
                dataListFinal.add(meterList.get(1));
                dataListFinal.add(meterList.get(0));
                dataListFinal.add(meterList.get(2));
                dataListFinal.add(meterList.get(3));

                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                dataListFinal.add("00");
                //区分直抄8/预抄9
                dataListFinal.add("4");//预抄
                dataListFinal.add(new Date());//入库时间3531778244
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                DataSourceObject dso = new DataSourceObject(businessDataitemId, null, dataListFinal);
                listDataObj.add(dso);
            } else {
                //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
                TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P" + pn);
                String mpedIdStr = terminalArchivesObject.getID();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                    continue;
                }
                List dataListFinal;
                try {
                    dataListFinal = FreezeDataUtils.getDataList(dataList, afn, fn, mpedIdStr);//标识-----14个正向有功
                    if (dataListFinal == null) {
                        continue;
                    }
                } catch (Exception e) {
                    continue;
                }
                if (fn == 19 || fn == 20 || fn == 4 || fn == 3) {//正反向有功最大需量及需量发生时间
                    for (int i = 0; i < dataListFinal.size(); i++) {
                        List oj = (List) dataListFinal.get(i);
                        oj.set(2, terminalDataObject.getUpTime());
                        oj.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                        oj.add("00");//未分析
                        oj.add("4");//人工入库
                        oj.add(new Date());//入库时间
                        oj.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                        oj.add(oj.size(), oj.get(oj.size() - 6));
                        oj.remove(oj.size() - 7);
                        DataSourceObject dso = new DataSourceObject(businessDataitemId, null, oj);
                        listDataObj.add(dso);
                    }
                    continue;
                }
                if (fn == 246) {
                    dataListFinal.set(2, terminalArchivesObject.getTerminalId());
                } else if (fn == 210) {//用电信息
                    dataListFinal.set(3, terminalArchivesObject.getPowerUnitNumber());
                } else {
                    if (fn == 250 || fn == 251 || fn == 252) {
                        dataListFinal.set(2, terminalArchivesObject.getPowerUnitNumber());
                    } else {
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                    }
                    dataListFinal.add("00");//未分析
                    dataListFinal.add("4");//人工入库
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                if (!FreezeDataUtils.FN_LIST.contains(fn)) {
                    dataListFinal.add(dataListFinal.size(), dataListFinal.get(dataListFinal.size() - 6));
                    dataListFinal.remove(dataListFinal.size() - 7);
                }
                DataSourceObject dso = new DataSourceObject(businessDataitemId, null, dataListFinal);
                listDataObj.add(dso);
                if (fn >= 193 && fn <= 196) {
                    List mon2dayList = dataListFinal;
                    int dateIndex = mon2dayList.size() - 1;
                    String dataDate = mon2dayList.get(dateIndex).toString();
                    String lastDataDate = FreezeDataUtils.lastDayByMonth(dataDate);
                    mon2dayList.set(dateIndex, lastDataDate);
                    DataSourceObject dsdo = new DataSourceObject("205003", null, dataListFinal);
                    listDataObj.add(dsdo);
                }
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