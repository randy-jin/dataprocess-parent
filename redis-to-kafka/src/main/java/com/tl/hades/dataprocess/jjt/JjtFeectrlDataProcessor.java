package com.tl.hades.dataprocess.jjt;

import com.datasource.DataSourceControl;
import com.datasource.DataSourceObject;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JjtFeectrlDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(JjtFeectrlDataProcessor.class);

    @AutoInsert
    private DataSourceControl dataSourceControl;

    @Override
    protected Class getClassType() {
        return this.getClass();
    }

    @Override
    protected String doCanFinish(Object o) throws Exception {
        int protocolId = 1;
        TerminalDataObject terminalDataObject = ClinkedList.getObject(o, TerminalDataObject.class);
        int afn = terminalDataObject.getAFN();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        List<DataSourceObject> listDataObj = new ArrayList<>();//数据类
        for (DataItemObject data : terminalDataObject.getList()) {
            List dataList = data.getList();//本测量点的数据项xxx.xx...
            int pn = data.getPn();
            int fn = data.getFn();
            logger.info("===MAKE FREEZE " + afn + "_" + fn + " DATA===");

            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            protocol3761ArchivesObject.setAfn(afn);
            protocol3761ArchivesObject.setFn(fn);
            //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P0");
            String terminalId = terminalArchivesObject.getTerminalId();
            String orgNo = terminalArchivesObject.getPowerUnitNumber();
            if (null == terminalId || "".equals(terminalId)) {
                logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                continue;
            }
            if (orgNo == null || "".equals(orgNo)) {//8-23新增
                logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                continue;
            }

            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
            List dataListFinal = new ArrayList();
            if (afn == 12 && fn == 402) {//210003
                dataListFinal.add(terminalId);//8-23表变更新增字段
                dataListFinal.add(areaCode);
                dataListFinal.add(terminalAddr);
                dataListFinal.add(dataList.get(0));
                dataListFinal.add(dataList.get(1));
                dataListFinal.add(new Date());//8-23新增字段
                dataListFinal.add(orgNo.substring(0, 5));//8-23新增字段
            }
            DataSourceObject dso = new DataSourceObject(businessDataitemId, null, dataListFinal);
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