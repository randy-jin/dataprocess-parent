package com.tl.hades.dataprocess.sg3761;

import com.datasource.DataSourceControl;
import com.datasource.DataSourceObject;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.persist.FreezeDataUtils;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FreezeDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(FreezeDataProcessor.class);
    private static List finList = new ArrayList();
    @AutoInsert
    private DataSourceControl dataSourceControl;


    @Override
    protected String doCanFinish(Object obj) throws Exception {
        //对获取的redis冻结类数据进行处理
//        logger.info("===GET FREEZE DATA===");
        TerminalDataObject terminalDataObject = ClinkedList.getObject(obj, TerminalDataObject.class);
        int protocolId = 1;
        int afn = terminalDataObject.getAFN();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        List<DataSourceObject> listDataObj = new ArrayList<>();//数据类
        //多测量点pnfn
        for (DataItemObject data : terminalDataObject.getList()) {

            List dataList = data.getList();//本测量点的数据项xxx.xx...
            if (data.getFn() != 43) {
                boolean allNull = FreezeDataUtils.allNull(dataList);
                if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                    continue;
                }
            }
            int pn = data.getPn();
            int fn = data.getFn();
//            logger.info("===MAKE " + afn + "_" + fn + " DATA===");

            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            protocol3761ArchivesObject.setAfn(afn);
            protocol3761ArchivesObject.setFn(fn);

//            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
            String businessDataitemId = "70103";
            //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
//            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P" + pn);
            TerminalArchivesObject terminalArchivesObject = new TerminalArchivesObject();
            terminalArchivesObject.setPowerUnitNumber("414011");
            terminalArchivesObject.setID("110542");
            terminalArchivesObject.setMeterId("800025412");
            terminalArchivesObject.setTerminalId("80000025412");
            String mpedIdStr = terminalArchivesObject.getID();
            if (null == mpedIdStr || "".equals(mpedIdStr)) {
                logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn + "_fn" + fn);
                if (fn == 250 || fn == 251 || fn == 252) {
                    logger.error(areaCode + "_" + terminalAddr + "_P" + pn + "  afn_fn:" + afn + "_" + fn + "   " + businessDataitemId);
                }
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
                    oj.add("0");//自动入库
                    oj.add(new Date());//入库时间
                    oj.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    oj.add(oj.size(), oj.get(oj.size() - 6));
                    oj.remove(oj.size() - 7);

                    DataSourceObject dso = new DataSourceObject(businessDataitemId, null, oj);
                    listDataObj.add(dso);//将组好的数据类添加到数据列表
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
                dataListFinal.add("0");//自动抄表
            }
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
            if (!FreezeDataUtils.FN_LIST.contains(fn)) {
                dataListFinal.add(dataListFinal.size(), dataListFinal.get(dataListFinal.size() - 6));
                dataListFinal.remove(dataListFinal.size() - 7);
            }
            Object[] refreshKey = new Object[5];
            refreshKey[0] = terminalArchivesObject.getTerminalId();
            refreshKey[1] = mpedIdStr;
            refreshKey[2] = "00000000000000"; // 数据召测时间
            refreshKey[3] = businessDataitemId;
            refreshKey[4] = protocolId;

            DataSourceObject dso = new DataSourceObject(businessDataitemId, refreshKey, dataListFinal);
            listDataObj.add(dso);//将组好的数据类添加到数据列表
        }
        dataSourceControl.writeToDataSource(listDataObj);
        return null;
    }

    @Override
    protected void cacheException(Object o, Exception e) {
        e.printStackTrace();
    }
}