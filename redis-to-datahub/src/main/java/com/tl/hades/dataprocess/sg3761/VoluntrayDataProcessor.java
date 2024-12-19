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
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class VoluntrayDataProcessor extends UnsettledMessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(VoluntrayDataProcessor.class);


    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET VoluntrayDataProcessor DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class clazz = ParamConstants.classMap.get(Integer.valueOf(protocolId));
            if (null == clazz) {
                throw new RuntimeException("规约ID[" + protocolId + "]类型不匹配");
            }

            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
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
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + var30);
                    if (terminalArchivesObject == null) {
                        continue;
                    }
                    String mpedIdStr = terminalArchivesObject.getID();
                    if (null != mpedIdStr && !"".equals(mpedIdStr)) {
                        List dataListFinal;
                        try {
                            dataListFinal = VoluntrayDataUtils.getDataList(dataList, afn, fn, mpedIdStr);
                            if (dataListFinal == null) {
                                continue;
                            }
                        } catch (Exception var28) {
                            logger.error("主动上报 afn_fn" + afn + "_" + fn + " 测量点:" + mpedIdStr + " " + dataList.toString(), var28);
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
                                if (fn <= 152 && fn >= 149) {//此处为费率曲线主动上报
                                    var33.set(var33.size() - 4, terminalArchivesObject.getPowerUnitNumber());
                                    CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, var33, null, listDataObj);
                                    continue;
                                }
                                var33.add(terminalArchivesObject.getPowerUnitNumber());
                                var33.add(new Date());
                                var33.add("93");
                                CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, var33, null, listDataObj);
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
                                        CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, var33, (Object[]) null, listDataObj);
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

                            CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataListFinal, (Object[]) null, listDataObj);
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
                                String shardId = dataHubTopic.getActiveShardList().get(index);
                                DataObject dataObj = new DataObject(oj, null, dataHubTopic.topic(), shardId);
                                listDataObj.add(dataObj);
                            }
                        }
                    } else {
                        logger.error("档案信息:" + areaCode + "_" + terminalAddr + "_P" + var30 + "_fn" + fn);
                        if (fn == 250 || fn == 251 || fn == 252) {
                            logger.error(areaCode + "_" + terminalAddr + "_P" + var30 + "  afn_fn:" + afn + "_" + fn + "   " + businessDataitemId);
                        }
                    }
                }

                PersistentObject var29 = new PersistentObject("athena", listDataObj);
                var29.setProject(DataHubProps.project);
                list.add(var29);
                context.setContent(list);
                return false;
            }
        }

        return true;
    }

}