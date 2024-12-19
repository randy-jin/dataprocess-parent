package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.ls.pf.base.utils.tools.StringUtils;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import com.tl.hades.persist.Split645DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TerminalVersionProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(TerminalVersionProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET TERMINAL VERSION DATA===");
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
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    List dataList = data.getList();//本测量点的数据项xxx.xx...
                    //TODO 2019-11-20 list is null 判断
                    if (dataList == null || dataList.size() == 0) {
                        continue;
                    }
                    int pn = data.getPn();
                    int fn = data.getFn();
                    logger.info("===MAKE " + afn + "_" + fn + " DATA===");

                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    protocol3761ArchivesObject.setAfn(afn);
                    protocol3761ArchivesObject.setFn(fn);
                    String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();

                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
                    if (terminalArchivesObject == null) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }
                    String tmnlIdStr = terminalArchivesObject.getTerminalId();
                    String orgNo = terminalArchivesObject.getPowerUnitNumber();
                    if (orgNo == null) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr);
                        continue;
                    }

                    Object[] refreshKey = new Object[5];
                    refreshKey[0] = tmnlIdStr;
                    refreshKey[1] = tmnlIdStr;
                    refreshKey[2] = "00000000000000"; // 数据召测时间
                    refreshKey[3] = businessDataitemId;
                    refreshKey[4] = protocolId;

                    List dataListFinal = new ArrayList<>();
                    dataListFinal.add(BigDecimal.valueOf(Long.valueOf(tmnlIdStr)));
                    if (fn == 10) {//载波版本信息
                        dataListFinal.add(orgNo);
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
                        dataListFinal.add(dataList.get(1));//终端生产厂家
                        dataListFinal.add(dataList.get(2));//芯片代码
                        dataListFinal.add(dataList.get(6));//版本信息
                        try {
                            String date = "20" + dataList.get(5) + "-" + dataList.get(4) + "-" + dataList.get(3);
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            dataListFinal.add(DateUtil.format(formatter.parse(date), DateUtil.defaultDatePattern_YMD));//终端软件版本日期
                        } catch (Exception e) {
                            dataListFinal.add(null);
                        }
                        dataListFinal.add(dataList.get(0));//模块地址
                        dataListFinal.add(null);//模块ID长度
                        dataListFinal.add(null);//模块ID格式
                        dataListFinal.add(null);//模块ID
                        dataListFinal.add(null);//组合ID
                        dataListFinal.add(null);//波特率
                        dataListFinal.add(null);//校验位
                        dataListFinal.add(null);//数据位
                        dataListFinal.add(null);//停止位
                        dataListFinal.add(null);//流控
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型

                    } else if (fn == 9) {//远程通信模块版本信息
                        dataListFinal.add(orgNo);
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
                        dataListFinal.add(dataList.get(0));//厂商代号
                        dataListFinal.add(dataList.get(1));//模块型号

                        //软硬件版本信息及时间
                        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.defaultDatePattern_YMD);
                        dataListFinal.add(dataList.get(2));//软件版本信息
                        Object so = dataList.get(3);
                        Object ho = dataList.get(5);
                        String softDate = null;
                        String hardDate = null;
                        if (so instanceof Date) {
                            softDate = sdf.format(so);
                        }
                        if (ho instanceof Date) {
                            hardDate = sdf.format(ho);
                        }
                        dataListFinal.add(softDate);//软件版本日期
                        dataListFinal.add(dataList.get(4));//硬件版本信息
                        dataListFinal.add(hardDate);//硬件版本日期
                        dataListFinal.add(dataList.get(6));//Sim卡ICCID
                        dataListFinal.add(null);//Sim卡号
                        dataListFinal.add(null);//模块id长度
                        dataListFinal.add(null);//模块ID格式
                        dataListFinal.add(null);//模块ID
                        dataListFinal.add(null);//组合ID
                        dataListFinal.add(null);//运行网络类型
                    } else if (fn == 17) {//远程通信模块ID信息
                        dataListFinal.add(orgNo);
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
                        dataListFinal.add(null);//厂商代号
                        dataListFinal.add(null);//模块型号
                        dataListFinal.add(null);//软件版本信息
                        dataListFinal.add(null);//软件版本日期
                        dataListFinal.add(null);//硬件版本信息
                        dataListFinal.add(null);//硬件版本日期
                        dataListFinal.add(null);//Sim卡ICCID
                        dataListFinal.add(null);//Sim卡号
                        dataListFinal.add(dataList.get(0));//模块id长度
                        dataListFinal.add(dataList.get(1));//模块ID格式
                        dataListFinal.add(dataList.get(2));//模块ID
                        dataListFinal.add(null);//组合ID
                        dataListFinal.add(null);//运行网络类型
                    } else if (fn == 18) {
                        dataListFinal.add(orgNo);
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
                        dataListFinal.add(dataList.get(0));//终端生产厂家
                        dataListFinal.add(null);//芯片代码
                        dataListFinal.add(null);//版本信息
                        dataListFinal.add(null);//终端软件版本日期
                        dataListFinal.add(null);//模块地址
                        dataListFinal.add(dataList.get(1));//模块ID长度
                        dataListFinal.add(dataList.get(2));//模块ID格式
                        dataListFinal.add(dataList.get(3));//模块ID
                        dataListFinal.add(null);//组合ID
                        dataListFinal.add(null);//波特率
                        dataListFinal.add(null);//校验位
                        dataListFinal.add(null);//数据位
                        dataListFinal.add(null);//停止位
                        dataListFinal.add(null);//流控
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型
                        dataListFinal.add(null);//模块类型

                    } else if (fn == 19) {//电能表／采集器通信模块ID信息
                        List firstList = (List) dataList.get(0);
                        for (int i = 0; i < firstList.size(); i++) {
                            List dLists = (List) firstList.get(i);

                            String mpedIndex = dLists.get(0).toString();
                            TerminalArchivesObject terminalArchivesObject19 = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + mpedIndex);
                            String mpedId = terminalArchivesObject19.getID();
                            if (mpedId == null) {
                                continue;
                            }
                            List finalList = new ArrayList();
                            finalList.add(tmnlIdStr);
                            finalList.add(mpedId);// COLL_METER_ID:BIGINT
                            finalList.add(orgNo);//ORG_NO:STRING
                            finalList.add(dLists.get(6));//COMM_ADDR:STRING
                            finalList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:STRING
                            finalList.add(mpedIndex);//SUBNODE_SN:STRING,
                            finalList.add(null);//CHIP_CODE:STRING,
                            finalList.add(mpedIndex);//MPED_INDEX:STRING
                            finalList.add(null);//SUBNODE_TYPE:STRING
                            finalList.add(null);//SUBNODE_DISC:STRING
                            finalList.add(null);//FACTORY_CODE:STRING
                            finalList.add(dLists.get(1));//UNIT_TYPE:STRING
                            finalList.add(dLists.get(3));//ID_LENTH:STRING,
                            finalList.add(dLists.get(4));//ID_FORMAT:STRING,
                            finalList.add(dLists.get(5));//UNIT_ID:STRING,
                            finalList.add(null);///UPDATE_FLAG:STRING,
                            finalList.add(null);///CHIP_FACTORY_CODE:STRING,
                            finalList.add(null);///CHIP_TYPE:STRING,
                            finalList.add(null);///CHIP_ID:STRING,
                            finalList.add(dLists.get(2));///UNIT_FACTORY_CODE:STRING,
                            finalList.add(null);///CALL_TIME_LONG:BIGINT,
                            finalList.add(null);///LATEST_SUCC_TIME:TIMESTAMP,
                            finalList.add(orgNo.substring(0, 5));
                            finalList.add(new Date());

                            CommonUtils.putToDataHub(businessDataitemId, tmnlIdStr, finalList, refreshKey, listDataObj);

                        }
                        continue;
                    } else if (fn == 54) {//终端流量
                        Date date = (Date) dataList.get(0);
                        String dateTime = DateUtil.format(date, "yyyy-MM") + "-01";
                        dataListFinal.add(dateTime);
                        dataListFinal.add(dataList.get(1));
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());
                    } else if (fn == 11) {//获取终端ESAM信息

                        String esamIndex = StringUtils.encodeHex((byte[]) dataList.get(0));
                        String certIndex = StringUtils.encodeHex((byte[]) dataList.get(1));
                        String keyVersion = StringUtils.encodeHex((byte[]) dataList.get(4));
                        dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        dataListFinal.add(terminalAddr);
                        dataListFinal.add(areaCode);
                        dataListFinal.add(esamIndex);
                        dataListFinal.add(certIndex );
                        dataListFinal.add(String.valueOf(dataList.get(2)));
                        dataListFinal.add(String.valueOf(dataList.get(3)));
                        dataListFinal.add(keyVersion);

                    } else {//版本信息
                        dataListFinal.add(dataList.get(0));
                        dataListFinal.add(dataList.get(1));
                        dataListFinal.add(dataList.get(2));
                        dataListFinal.add(dataList.get(6));
                        dataListFinal.add(dataList.get(3));
                        dataListFinal.add(dataList.get(7));
                    }
                    if (fn == 9 || fn == 10 || fn == 17 || fn == 18) {
                        dataListFinal.add(orgNo.substring(0, 5));
                        dataListFinal.add(new Date());
                        if (fn == 9 || fn == 18) {
                            dataListFinal.add("03");
                        } else if (fn == 17) {
                            dataListFinal.add("04");
                        } else {
                            dataListFinal.add("02");
                        }
                    } else if (fn == 11) {
                        dataListFinal.add(orgNo.substring(0, 5));
                        dataListFinal.add(new Date());
                    } else {
                        dataListFinal.add(new Date());
                        dataListFinal.add(orgNo.substring(0, 5));
                    }
                    if (dataListFinal == null) {
                        continue;
                    }

                    CommonUtils.putToDataHub(businessDataitemId, tmnlIdStr, dataListFinal, refreshKey, listDataObj);

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