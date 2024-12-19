package com.tl.hades.dataprocess.jjt;

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
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JjtEventDataProcessor extends UnsettledMessageProcessor {

    Logger logger = LoggerFactory.getLogger(JjtEventDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET JJT EVENT DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class<?> clazz = ParamConstants.classMap.get(protocolId);
            if (null == clazz) {
                throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
            }
            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
                int afn = terminalDataObject.getAFN();
                String areaCode = terminalDataObject.getAreaCode();
                int terminalAddr = terminalDataObject.getTerminalAddr();
                List<PersistentObject> list = new ArrayList<>();//持久类
                List<DataObject> listDataObj = new ArrayList<>();//数据类
                for (DataItemObject data : terminalDataObject.getList()) {
                    //TODO 2019-11-20 list is null 判断
                    List<DataItemObject> firstList = terminalDataObject.getList();
                    if (firstList == null || firstList.size() == 0) {
                        continue;
                    }

                    List dierList = firstList.get(0).getList();
                    if (dierList == null || dierList.size() == 0) {
                        continue;
                    }

                    List arrayList = (List) dierList.get(0);
                    int pn = data.getPn();
                    int fn = data.getFn();
                    logger.info("===MAKE FREEZE " + afn + "_" + fn + " DATA===");

                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    protocol3761ArchivesObject.setAfn(afn);
                    protocol3761ArchivesObject.setFn(fn);
                    //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
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
                            //149001
                            try {
                                if (dataList.get(6) == null || dataList.get(7) == null || dataList.get(8) == null || dataList.get(9) == null) {
                                    continue;
                                }
                                if(Integer.parseInt(dataList.get(0).toString())<0){
                                    continue;
                                }
                            } catch (Exception e) {
                                continue;
                            }
                            dataListFinal.add(null);//TERMINAL_NO:VARCHAR:费控终端区域号
                            dataListFinal.add(terminalId);//TERMINAL_ID:BIGINT:集中器标识
                            dataListFinal.add(dataList.get(0));//CONSUME_INDX:BIGINT:记录序号
                            dataListFinal.add(dataList.get(1));//DET_TYPE:VARCHAR:记录类型：0，远程控制用电；1，刷卡用电
                            String pump_no = dataList.get(5).toString();
                            if (pump_no.indexOf(".") > 0) {
                                pump_no = pump_no.substring(0, pump_no.indexOf("."));
                            }
                            dataListFinal.add(pump_no);//PUMP_NO:VARCHAR:机井编号
                            dataListFinal.add(dataList.get(2));//CARD_NO:VARCHAR:电卡编号
                            if (dataList.get(3) == null) {
                                continue;
                            }
                            dataListFinal.add(dataList.get(3));//BEGIN_TIME:DATETIME:本次刷卡开始时间
                            dataListFinal.add(dataList.get(4));//END_TIME:DATETIME:本次刷卡结束时间
                            dataListFinal.add(dataList.get(6));//BEGIN_REMAIN_AMT:VARCHAR:本次刷卡开始剩余金额
                            dataListFinal.add(dataList.get(8));//END_REMAIN_AMT:VARCHAR:本次刷卡结束剩余金额
                            dataListFinal.add(dataList.get(7));//LAST_MR_NUM:VARCHAR:刷卡开始用电时组合有功电能示值
                            dataListFinal.add(dataList.get(9));//THIS_MR_NUM:VARCHAR:刷卡结束用电时组合有功电能示值

                            dataListFinal.add(0);//IS_TS:INT:是否已经推送
                            //不等于0为过程用电
                            if (Integer.parseInt(dataList.get(0).toString()) != 0) {
                                dataListFinal.add(null);//LAST_WATER_LEVEL:VARCHAR:上m次用水阶梯
                                dataListFinal.add(null);//LAST_UPQ_BEGIN:DECIMAL:上m次用电开始用电时已用电量
                                dataListFinal.add(null);//LAST_UPQ_END:DECIMAL:上m次用电结束用电时已用电量
                                dataListFinal.add(null);//COMM_ADDR:VARCHAR:电表通讯地址
                                dataListFinal.add(null);//LAST_UPQ_CONS:DECIMAL:上m次用电用户额定用电量
                            } else {
                                businessDataitemId = businessDataitemIdEnd;
                            }
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
                        if (afn == 30 && fn == 5) {
                            //149001
                            //149001
                            try {
                                if (dataList.get(6) == null || dataList.get(7) == null || dataList.get(8) == null || dataList.get(9) == null) {
                                    continue;
                                }
                            } catch (Exception e) {
                                continue;
                            }
                            dataListFinal.add(null);//TERMINAL_NO:VARCHAR:费控终端区域号
                            dataListFinal.add(terminalId);//TERMINAL_ID:BIGINT:集中器标识
                            dataListFinal.add(dataList.get(0));//CONSUME_INDX:BIGINT:记录序号
                            dataListFinal.add(dataList.get(1));//DET_TYPE:VARCHAR:记录类型：0，远程控制用电；1，刷卡用电
                            String pump_no = dataList.get(5).toString();
                            if (pump_no.indexOf(".") > 0) {
                                pump_no = pump_no.substring(0, pump_no.indexOf("."));
                            }
                            dataListFinal.add(pump_no);//PUMP_NO:VARCHAR:机井编号
                            dataListFinal.add(dataList.get(2));//CARD_NO:VARCHAR:电卡编号
                            if (dataList.get(3) == null) {
                                continue;
                            }
                            dataListFinal.add(dataList.get(3));//BEGIN_TIME:DATETIME:本次刷卡开始时间
                            dataListFinal.add(dataList.get(4));//END_TIME:DATETIME:本次刷卡结束时间
                            dataListFinal.add(dataList.get(6));//BEGIN_REMAIN_AMT:VARCHAR:本次刷卡开始剩余金额
                            dataListFinal.add(dataList.get(8));//END_REMAIN_AMT:VARCHAR:本次刷卡结束剩余金额
                            dataListFinal.add(dataList.get(7));//LAST_MR_NUM:VARCHAR:刷卡开始用电时组合有功电能示值
                            dataListFinal.add(dataList.get(9));//THIS_MR_NUM:VARCHAR:刷卡结束用电时组合有功电能示值

                            dataListFinal.add(0);//IS_TS:INT:是否已经推送
                            dataListFinal.add(dataList.get(11));//上m次用水阶梯
                            dataListFinal.add(dataList.get(12));//上m次用电开始用电时已用电量
                            dataListFinal.add(dataList.get(13));//上m次用电结束用电时已用电量
                            dataListFinal.add(dataList.get(10));//表地址
                            //不等于0为过程用电
                            if (Integer.parseInt(dataList.get(0).toString()) != 0) {
                                dataListFinal.add(null);//上m次用电用户额定用电量
                            } else {
                                businessDataitemId = businessDataitemId + "_pro_dzs";
                            }
                        }
                        dataListFinal.add(new Date());//入库时间
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]

                        CommonUtils.putToDataHub(businessDataitemId, terminalId, dataListFinal, null, listDataObj);

                    }

                }
                PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                list.add(persistentObject);//这个完整报文添加到了持久化list了
                context.setContent(list);
                return false;
            }
        }
        return true;
    }

}