package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.datahub.DataHubControl;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.EventUtils;
import com.tl.hades.persist.VoluntrayDataUtils;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class VoluntrayDataProcessor extends RunnableTask {

    private static final Logger logger = Logger.getLogger(VoluntrayDataProcessor.class);

    @AutoInsert
    private DataHubControl dataHubControl;


    @Override
    protected String doCanFinish(Object o) throws Exception {
        int protocolId = 1;
        TerminalDataObject terminalDataObject = ClinkedList.getObject(o, TerminalDataObject.class);
        int afn = terminalDataObject.getAFN();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        ArrayList list = new ArrayList();
        ArrayList listDataObj = new ArrayList();
        Iterator persistentObject = terminalDataObject.getList().iterator();

        while (persistentObject.hasNext()) {
            DataItemObject data = (DataItemObject) persistentObject.next();
            List dataList = data.getList();
            int fn = data.getFn();
            if (fn != 43 && !VoluntrayDataUtils.curveList.contains(Integer.valueOf(fn))) {
                boolean pn = VoluntrayDataUtils.allNull(dataList);
                if (pn) {
                    continue;
                }
            }

            int var30 = data.getPn();
            logger.info("===MAKE " + afn + "_" + fn + " DATA===");
            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
            protocol3761ArchivesObject.setAfn(afn);
            protocol3761ArchivesObject.setFn(fn);
            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P" + var30);
            String mpedIdStr = terminalArchivesObject.getID();
            if (null != mpedIdStr && !"".equals(mpedIdStr)) {
                List dataListFinal;
                try {
                    dataListFinal = VoluntrayDataUtils.getDataList(dataList, afn, fn, mpedIdStr);
                    if (dataListFinal == null) {
                        continue;
                    }
                } catch (Exception var28) {
                    continue;
                }

                Iterator var31;
                Object var32;
                List var33;
                if (VoluntrayDataUtils.curveList.contains(Integer.valueOf(fn))) {
                    var31 = dataListFinal.iterator();

                    while (var31.hasNext()) {
                        var32 = var31.next();
                        var33 = (List) var32;
                        var33.add(terminalArchivesObject.getPowerUnitNumber());
                        var33.add(new Date());
                        var33.add("93");
                        EventUtils.toDataHub(businessDataitemId, mpedIdStr, var33, (Object[]) null, listDataObj);
                    }
                } else if (fn == 1) {
                    if (dataListFinal != null && !dataListFinal.isEmpty()) {
                        var31 = dataListFinal.iterator();

                        while (var31.hasNext()) {
                            var32 = var31.next();
                            if (var32 instanceof List) {
                                var33 = (List) var32;
                                var33.set(19, terminalArchivesObject.getPowerUnitNumber());
                                var33.set(23, terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                                EventUtils.toDataHub(businessDataitemId, mpedIdStr, var33, (Object[]) null, listDataObj);
                            }
                        }
                    }
                } else if (fn != 19 && fn != 20 && fn != 4 && fn != 3) {
                    if (fn == 246) {
                        dataListFinal.set(2, terminalArchivesObject.getTerminalId());
                    } else if (fn == 210) {
                        dataListFinal.set(3, terminalArchivesObject.getPowerUnitNumber());
                    } else {
                        if (fn != 250 && fn != 251 && fn != 252) {
                            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());
                        } else {
                            dataListFinal.set(2, terminalArchivesObject.getPowerUnitNumber());
                        }

                        dataListFinal.add("00");
                        dataListFinal.add("93");
                    }

                    dataListFinal.add(new Date());
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    if (!VoluntrayDataUtils.FN_LIST.contains(Integer.valueOf(fn))) {
                        dataListFinal.add(dataListFinal.size(), dataListFinal.get(dataListFinal.size() - 6));
                        dataListFinal.remove(dataListFinal.size() - 7);
                    }

                    EventUtils.toDataHub(businessDataitemId, mpedIdStr, dataListFinal, (Object[]) null, listDataObj);
                } else {
                    for (int i = 0; i < dataListFinal.size(); ++i) {
                        List oj = (List) dataListFinal.get(i);
                        oj.set(2, terminalDataObject.getUpTime());
                        oj.add(terminalArchivesObject.getPowerUnitNumber());
                        oj.add("00");
                        oj.add("93");
                        oj.add(new Date());
                        oj.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                        oj.add(oj.size(), oj.get(oj.size() - 6));
                        oj.remove(oj.size() - 7);
                        DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
                        int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
                        String shardId = (String) dataHubTopic.getActiveShardList().get(index);
                        DataObject dataObj = new DataObject(oj, (Object[]) null, dataHubTopic.topic(), shardId);
                        listDataObj.add(dataObj);
                    }
                }
            } else {
                logger.error("鏃犳硶浠庣紦瀛樿幏鍙栨纭殑妗ｆ淇℃伅:" + areaCode + "_" + terminalAddr + "_P" + var30 + "_fn" + fn);
                if (fn == 250 || fn == 251 || fn == 252) {
                    logger.error(areaCode + "_" + terminalAddr + "_P" + var30 + "  afn_fn:" + afn + "_" + fn + "   " + businessDataitemId);
                }
            }
        }
        dataHubControl.insertion(DataHubProps.project, listDataObj, null);
        return null;
    }

    @Override
    protected void cacheException(Object o, Exception e) {

    }
}