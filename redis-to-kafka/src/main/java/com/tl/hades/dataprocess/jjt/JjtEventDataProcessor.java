package com.tl.hades.dataprocess.jjt;

import com.datasource.DataSourceControl;
import com.datasource.DataSourceObject;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JjtEventDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(JjtEventDataProcessor.class);

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
            List arrayList = (List) terminalDataObject.getList().get(0).getList().get(0);
            int pn = data.getPn();
            int fn = data.getFn();
            logger.info("===MAKE FREEZE " + afn + "_" + fn + " DATA===");

            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            protocol3761ArchivesObject.setAfn(afn);
            protocol3761ArchivesObject.setFn(fn);
            //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P0");
            String terminalId = terminalArchivesObject.getTerminalId();
            if (null == terminalId || "".equals(terminalId)) {
                logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                continue;
            }

            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
            String businessDataitemIdEnd = businessDataitemId + "_pro";
            for (int i = 0; i < arrayList.size(); i++) {
                List dataList = (List) arrayList.get(i);

                List dataListFinal = new ArrayList();
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                if (afn == 30 && fn == 1) {
                    try {
                        if (dataList.get(6) == null || dataList.get(7) == null || dataList.get(8) == null || dataList.get(9) == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        continue;
                    }
                    dataListFinal.add(null);
                    dataListFinal.add(terminalId);
                    if (Integer.parseInt(dataList.get(0).toString()) == 0) {
                        businessDataitemId = businessDataitemIdEnd;
                    }
                    dataListFinal.add(dataList.get(0));//consume_indx

                    dataListFinal.add(dataList.get(1));//det_type 记录类型

                    String pump_no = dataList.get(5).toString();
                    if (pump_no.indexOf(".") > 0) {
                        pump_no = pump_no.substring(0, pump_no.indexOf("."));
                    }
                    dataListFinal.add(pump_no);
                    dataListFinal.add(dataList.get(2));//card_no 电卡编号

                    if (dataList.get(3) == null) {
                        continue;
                    }
                    dataListFinal.add(dataList.get(3));//begin_time
                    dataListFinal.add(dataList.get(4));//end_time

                    dataListFinal.add(dataList.get(6));
                    dataListFinal.add(dataList.get(8));

                    dataListFinal.add(dataList.get(7));
                    dataListFinal.add(dataList.get(9));

                    //新增字段
                    dataListFinal.add(dataList.get(10));
                    dataListFinal.add(dataList.get(11));
                    dataListFinal.add(dataList.get(12));
                    dataListFinal.add(dataList.get(13));

                    dataListFinal.add(0);

                }
                if (afn == 30 && fn == 2) {
                    //149002
                    dataListFinal.add(terminalId);
                    dataListFinal.add(null);
                    dataListFinal.add(dataList.get(0));
                    dataListFinal.add(dataList.get(1));
                    dataListFinal.add(dataList.get(3));
                    dataListFinal.add(dataList.get(4));
                    dataListFinal.add(dataList.get(5));
                    dataListFinal.add(dataList.get(6));
                }
                if (afn == 30 && fn == 3) {
                    //149003
                    dataListFinal.add(terminalId);
                    dataListFinal.add(null);
                    dataListFinal.add(dataList.get(0));
                    String pump_no = dataList.get(4).toString();
                    if (pump_no.indexOf(".") > 0) {
                        pump_no = pump_no.substring(0, pump_no.indexOf("."));
                    }
                    dataListFinal.add(pump_no);
                    dataListFinal.add(dataList.get(1));
                    dataListFinal.add(dataList.get(2));

                    dataListFinal.add(dataList.get(5));
                    dataListFinal.add(dataList.get(6));


                }
                if (afn == 30 && fn == 4) {
                    //149004
                    dataListFinal.add(null);
                    dataListFinal.add(terminalId);
                    dataListFinal.add(dataList.get(0));
                    dataListFinal.add(dataList.get(1));
                    dataListFinal.add(dataList.get(3));
                    String pump_no = dataList.get(4).toString();
                    if (pump_no.indexOf(".") > 0) {
                        pump_no = pump_no.substring(0, pump_no.indexOf("."));
                    }
                    dataListFinal.add(pump_no);
                    dataListFinal.add(dataList.get(5));

                    dataListFinal.add(dataList.get(6));
                    dataListFinal.add(dataList.get(8));

                    dataListFinal.add(dataList.get(7));
                    dataListFinal.add(dataList.get(9));
                    dataListFinal.add(dataList.get(10));
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]

                DataSourceObject dso = new DataSourceObject(businessDataitemId, null, dataListFinal);
                listDataObj.add(dso);//将组好的数据类添加到数据列表
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