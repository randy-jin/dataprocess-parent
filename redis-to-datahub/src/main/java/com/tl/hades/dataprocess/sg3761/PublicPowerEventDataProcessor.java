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
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
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

/**
 * 共享电源
 *
 * @author easb
 */

public class PublicPowerEventDataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(PublicPowerEventDataProcessor.class);
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked", "unused"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET PublicPowerEvent DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {//双轨临时注掉
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
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类

                TerminalArchivesObject terminalArchivesObject;
                String terminalId;
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    List dataList = data.getList();//本测量点的数据项xxx.xx...
                    List datatoredis = new ArrayList<Object>();
                    int pn = data.getPn();
                    int fn = data.getFn();
                    logger.info("===MAKE " + afn + "_" + fn + " DATA===");
                    terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
                    String orgNo = terminalArchivesObject.getPowerUnitNumber();
                    if (terminalArchivesObject == null) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    terminalId = terminalArchivesObject.getTerminalId();
                    if (null == terminalId || "".equals(terminalId)) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_TMNLID");
                        continue;
                    }
                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    protocol3761ArchivesObject.setAfn(afn);
                    protocol3761ArchivesObject.setFn(fn);
                    String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
                    String eventId = idGenerator.next();
                    List dataListFinal = new ArrayList<Object>();
                    //实时用电数据上报
                    if (afn == 21 && fn == 7) {
                        List dataLista = (List) ((List) dataList.get(13)).get(0);
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(1));
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(2) == null ? new Date() : (Date) dataList.get(2), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(null);
                        dataListFinal.add(null);
                        dataListFinal.add(dataList.get(12));//总电量
                        for (int i = 0; i < dataLista.size(); i++) {//尖峰平谷
                            dataListFinal.add(dataLista.get(i));
                        }
                        for (int i = 0; i < 8; i++) {
                            dataListFinal.add(null);
                        }
                        for (int i = 3; i < 10; i++) {//Abc电压电流
                            dataListFinal.add(dataList.get(i));
                        }
                        dataListFinal.add(dataList.get(2));
                        dataListFinal.add(null);//充电开始时间
                        dataListFinal.add(dataList.get(10));//消费金额
                        if (dataListFinal.size() != 29) {
                            continue;
                        }
                    } else if (afn == 21 && fn == 8) {//窃电数据上报
                        List dataLista = (List) ((List) dataList.get(13)).get(0);
                        List dataListb = (List) ((List) dataList.get(15)).get(0);
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(0));//业务流水号
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(1) == null ? new Date() : (Date) dataList.get(2), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(terminalAddr);
                        dataListFinal.add(null);//标准代码表
                        dataListFinal.add(dataList.get(1));//开始充电时间
                        dataListFinal.add(dataList.get(2));//结束充电时间
                        for (int i = 3; i < 9; i++) {//充电时长-总尖峰平谷
                            dataListFinal.add(dataList.get(i));
                        }
                        for (int i = 0; i < dataLista.size(); i++) {
                            dataListFinal.add(dataLista.get(i));
                        }
                        for (int i = 0; i < dataListb.size(); i++) {
                            dataListFinal.add(dataListb.get(i));
                        }
                        dataListFinal.add(dataList.get(9));
                        if (dataListFinal.size() != 23) {
                            continue;
                        }
                    } else if (afn == 21 && fn == 80) {//终端停/上电事件
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(2));
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(0) == null ? new Date() : (Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(dataList.get(0));//停电发生时间
                        dataListFinal.add(dataList.get(1));//上点发生时间
                    } else if (afn == 21 && fn == 88) {//开/关用电小门事件记录
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(4));
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(0) == null ? (Date) dataList.get(1) : (Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(dataList.get(0));//开关用电小门发生时间
                        dataListFinal.add(dataList.get(1));
                        List<String> result = typeFlagF88(dataList.get(2));
                        if (result.isEmpty()) {
                            for (int i = 0; i < 4; i++) {
                                dataListFinal.add(null);
                            }
                        } else {
                            dataListFinal.add(result.get(1));//异常/正常
                            dataListFinal.add(result.get(0));//关闭/开启
                            dataListFinal.add(result.get(2));//本地/远程
                            dataListFinal.add(result.get(3));//空闲/使用中
                        }
                        dataListFinal.add("0");
                        dataListFinal.add(null);
                        dataListFinal.add(null);
                        if (dataListFinal.size() != 13) {
                            continue;
                        }
                    } else if (afn == 21 && fn == 89) {//主动上报窃电预警事件
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(2));//业务流水号
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(0) == null ? new Date() : (Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(dataList.get(0));//停电发生时间
                        dataListFinal.add(null);//标准代码表
                        dataListFinal.add(terminalAddr);
                        dataListFinal.add(null);
                    } else if (afn == 21 && fn == 90) {//异常断电事件
                        dataListFinal.add(eventId);
                        dataListFinal.add(dataList.get(2));
                        dataListFinal.add(terminalId);
                        dataListFinal.add(DateUtil.format(dataList.get(0) == null ? new Date() : (Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(dataList.get(0));//异常断电发生时间
                        dataListFinal.add(null);
                        dataListFinal.add(terminalAddr);
                    }
                    dataListFinal.add(orgNo);//供电单位编号
                    dataListFinal.add(orgNo.substring(0, 5));
                    dataListFinal.add(new Date());//入库时间3531778244

                    CommonUtils.putToDataHub(businessDataitemId,terminalId,dataListFinal,null,listDataObj);


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

    //f87解析
    private List typeFlagF87(Object flag) {
        String flagFinal = "";
        List list = new ArrayList();
        if (!"".equals(flag)) {
            flagFinal = Integer.toBinaryString((Integer) flag);
            if (flagFinal.length() < 2) {
                flagFinal = "00".substring(0, 2 - flagFinal.length()) + flagFinal;
            }
        }
        if (!"".equals(flagFinal)) {
            list.add("" + flagFinal.charAt(0));
            list.add("" + flagFinal.charAt(1));
        }
        return list;
    }

    //f88解析
    private List<String> typeFlagF88(Object flag) {
        String flagFinal = "";
        List<String> list = new ArrayList();
        if (!"".equals(flag) && null != flag) {
            flagFinal = Integer.toBinaryString((Integer) flag);
            if (flagFinal.length() < 4) {
                flagFinal = "0000".substring(0, 4 - flagFinal.length()) + flagFinal;
            }
        }
        if (!"".equals(flagFinal)) {
            for (char o : flagFinal.toCharArray()
                    ) {
                list.add("" + o);
            }
        }
        return list;
    }
}
