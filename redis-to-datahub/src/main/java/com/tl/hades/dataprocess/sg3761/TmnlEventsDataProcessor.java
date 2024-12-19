package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TmnlEventsDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(TmnlEventsDataProcessor.class);
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
    private final String erc = "erc";

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET EVENTS DATA===");
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
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();// 数据类
                // 多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    int fn = data.getFn();
                    List dataList = data.getList();
                    for (Object o : dataList) {
                        if (o instanceof List) {
                            List sublist = (List) o;
                            int ercCode = (Integer) (sublist.remove(0));
                            Date event_time;
                            try {
                                if (ercCode == 14) {
                                    if (sublist.size() == 4) {
                                        sublist.remove(0);
                                    }
                                    event_time = (Date) sublist.get(1);
                                } else {
                                    event_time = (Date) sublist.remove(0);
                                }
                            } catch (Exception e) {
                                continue;
                            }
                            if (event_time == null || "".equals(event_time)) {
                                continue;
                            }
                            String event_date = EventUtils.getEventDate(event_time);
                            if (ercCode == 14) {
                                if (event_time != null) {
                                    try {
                                        long c = (new Date().getTime() - event_time.getTime()) / 1000 / 60 / 60 / 24;
                                        if (Math.abs(c) > 180) {
                                            continue;
                                        }
                                    } catch (Exception e) {
                                        continue;
                                    }
                                }
                            }
                            Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                            protocol3761ArchivesObject.setAfn(afn);
                            protocol3761ArchivesObject.setFn(fn);
                            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
                            if (terminalArchivesObject == null) {
                                continue;
                            }
                            String terminalId = terminalArchivesObject.getTerminalId();
                            String orgNo = terminalArchivesObject.getPowerUnitNumber();
                            if (null == terminalId || "".equals(terminalId)) {
                                logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P0");
                                return false;
                            }
                            //TODO 如果是56需要特殊处理 hch
                            if (ercCode == 56) {
                                String eventId = idGenerator.next();
                                String flag = sublist.remove(0).toString();
                                List<String> addrList = sublist;
                                if (addrList == null) {
                                    continue;
                                }
                                Object dataFlag = sublist.get(0);
                                int node_num;
                                if (dataFlag instanceof Integer) {
                                    node_num = (int) sublist.remove(0);
                                } else {
                                    node_num = sublist.size();
                                }
                                for (String addrStr : addrList) {
                                    if (addrStr == null) {
                                        continue;
                                    }
                                    //56
                                    List eventErc56 = new ArrayList();

                                    eventErc56.add(eventId);
                                    eventErc56.add(terminalId);
                                    eventErc56.add(event_time);
                                    TerminalArchivesObject terminalArchivesObject56 = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + addrStr, "MI" + addrStr);
                                    String meterId = terminalArchivesObject56.getMeterId();
                                    eventErc56.add(flag);//停上电标识
                                    eventErc56.add(node_num);//节点数量
                                    if (meterId == null) {
                                        eventErc56.add(null);
                                    } else {
                                        eventErc56.add(BigDecimal.valueOf(Long.parseLong(meterId)));
                                    }
                                    eventErc56.add(addrStr);
                                    eventErc56.add(orgNo.substring(0, 5));//shard_no
                                    eventErc56.add(new Date());
                                    eventErc56.add(event_date);//event_date

                                    //事件子表
                                    String businessDataitemId = erc + ercCode;
                                    CommonUtils.putToDataHub(businessDataitemId, eventId, eventErc56, null, listDataObj);

                                    if (meterId == null) {
                                        continue;
                                    }

                                    //no_power

                                    List no_power = new ArrayList();
                                    no_power.add(BigDecimal.valueOf(Long.parseLong(terminalArchivesObject56.getMeterId())));
                                    no_power.add(new Date());
                                    no_power.add(orgNo);
                                    no_power.add("0".equals(flag) ? "1" : "0");

                                    if (flag.equals("1")) {//erc56停电标示为1 NO_power停电标示为0 所以此处判断0
                                        no_power.add(event_time);
                                        no_power.add(null);
                                    } else {
                                        no_power.add(null);
                                        no_power.add(event_time);
                                    }
                                    no_power.add(orgNo.substring(0, 5));//shard_no
                                    no_power.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                                    String dataitemID = "03110001";
                                    Object checkDate = no_power.get(no_power.size() - 1);
                                    boolean err = DateFilter.betweenDay(checkDate, 0, 180);
                                    if (!err) {
                                        dataitemID = "no_power_err";
                                    }

                                    //入nopower表
                                    CommonUtils.putToDataHub(dataitemID, eventId, no_power, null, listDataObj);
                                }

                            } else {

                                String eventId = idGenerator.next();


                                List eventErc = new ArrayList();
                                List lastData = new ArrayList();
                                if (ercCode == 43) {
                                    if (sublist.size() < 11) continue;

                                    eventErc.add(eventId);//EVENT_ID:BIGINT:事件标识
                                    eventErc.add(event_date);//EVENT_DATE:DATE:事件日期
                                    eventErc.add(event_time);//EVENT_TIME:DATETIME:事件发生时间
                                    eventErc.add(terminalId);//TERMINAL_ID:BIGINT:终端标识
                                    eventErc.add(new Date());//INPUT_TIME:DATETIME:入库时间
                                    eventErc.add(areaCode);//AREA_CODE:VARCHAR:终端区划码
                                    eventErc.add(terminalAddr);//TERMINAL_ADDR:VARCHAR:终端地址
                                    eventErc.add(orgNo);//ORG_NO:VARCHAR:供电单位
                                    eventErc.add("1");//REPORT_OR_COLLECT:VARCHAR:上报或召测标志0：上报，1：召测，2：分析产生
                                    Object f = sublist.get(0);
                                    if (f == null) continue;
                                    char[] flag = f.toString().toCharArray();
                                    String updateFlag = "0";
                                    if (flag[0] == '1') {
                                        updateFlag = "1";
                                    }
                                    if (flag[1] == '1') {
                                        updateFlag = "2";
                                    }
                                    if (flag[2] == '1') {
                                        updateFlag = "3";
                                    }
                                    eventErc.add(updateFlag);//UPDATE_FLAG:VARCHAR:变更模块分类标志1：远程通信模块，2：本地通信模块，3：从节点通信模块

                                    eventErc.add("1");//BF_UNIT_TYPE:VARCHAR:变更前模块类型1：PLC，2：WIRELESS
                                    eventErc.add(null);//BF_HPLC_FACTORY:VARCHAR:变更前芯片厂商代码
                                    eventErc.add(null);//BF_HPLC_IDFORMAT:VARCHAR:变更前芯片ID格式类型
                                    eventErc.add(sublist.get(3));//BF_MIDFORMAT:VARCHAR:变更前模块ID格式类型
                                    eventErc.add(sublist.get(1));//BF_FACTORY_CODE:VARCHAR:变更前模块厂商代码
                                    eventErc.add(sublist.get(4));//BF_COMB_ID:VARCHAR:变更前模块ID
                                    eventErc.add(null);//BF_UNIT_ID:VARCHAR:变更前芯片ID
                                    eventErc.add(sublist.get(5));//BF_COMM_ADDR:VARCHAR:变更前从节点通信地址


                                    eventErc.add("1");//AF_UNIT_TYPE:VARCHAR:变更后模块类型1：PLC，2：WIRELESS
                                    eventErc.add(null);//AF_HPLC_FACTORY:VARCHAR:变更后芯片厂商代码
                                    eventErc.add(null);//AF_HPLC_IDFORMAT:VARCHAR:变更后芯片ID格式类型
                                    eventErc.add(sublist.get(8));//AF_MIDFORMAT:VARCHAR:变更后模块ID格式类型
                                    eventErc.add(sublist.get(6));//AF_FACTORY_CODE:VARCHAR:变更后模块厂商代码
                                    eventErc.add(sublist.get(9));//AF_COMB_ID:VARCHAR:变更后模块ID
                                    eventErc.add(null);//AF_UNIT_ID:VARCHAR:变更后芯片ID

                                    eventErc.add("0");//REC_SOURCE:VARCHAR:记录来源0:事件上报，1：主站对比分析
                                    eventErc.add(sublist.get(10));
                                    eventErc.add(orgNo.substring(0, 5));
                                    eventErc.add(new Date());
                                } else if (ercCode == 63) {
                                    eventErc.add(eventId);
                                    eventErc.add(event_date);
                                    eventErc.add(terminalId);
                                    eventErc.add(null);
                                    eventErc.add(event_time);
                                    eventErc.add(null);
                                    eventErc.add(sublist.get(2));
                                    eventErc.add(sublist.get(3));
                                    eventErc.add(sublist.get(4));
                                    eventErc.add(null);
                                    eventErc.add(sublist.get(1));
                                    eventErc.add(orgNo.substring(0, 5));
                                    eventErc.add(new Date());
                                } else if (ercCode == 59) {
                                    eventErc.add(eventId);
                                    eventErc.add(event_date);
                                    eventErc.add(terminalId);
                                    eventErc.add(event_time);
                                    int count = sublist.size();
                                    eventErc.add(count);
                                    StringBuffer sb = new StringBuffer();
                                    String addr = null;
                                    for (int s = 0; s < sublist.size(); s++) {
                                        sb.append(sublist.get(s)).append(",");
                                    }
                                    if (sb.length() > 0) {
                                        addr = sb.toString().substring(0, sb.length() - 1);
                                    }
                                    eventErc.add(addr);
                                    eventErc.add(orgNo.substring(0, 5));
                                    eventErc.add(new Date());
                                } else {
                                    eventErc.add(eventId);
                                    eventErc.add(terminalId);
                                    eventErc.add(event_time);
                                    lastData = getDataList(sublist, ercCode, areaCode, terminalAddr);
                                    if (lastData == null) {
                                        continue;
                                    }
                                    eventErc.addAll(lastData);
                                    if (ercCode == 35) {
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                    } else if (ercCode == 12) {
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                        eventErc.add(null);
                                    }
                                    eventErc.add(orgNo.substring(0, 5));//shard_no
                                    eventErc.add(new Date());
                                    eventErc.add(event_date);
                                }


                                //事件子表
                                String businessDataitemId = erc + ercCode;

                                CommonUtils.putToDataHub(businessDataitemId, eventId, eventErc, null, listDataObj);

                                if (ercCode == 14) {
                                    List eventErcChecked = new ArrayList();
                                    eventErcChecked.add(eventId);
                                    eventErcChecked.add(terminalId);
                                    eventErcChecked.add(areaCode);
                                    eventErcChecked.add(terminalAddr + "");
                                    eventErcChecked.add(event_time);
                                    eventErcChecked.addAll(lastData);
                                    eventErcChecked.add(orgNo.substring(0, 5));//shard_no
                                    eventErcChecked.add(new Date());
                                    eventErcChecked.add(event_date);

                                    CommonUtils.putToDataHub("erc14_checked_source", eventId, eventErcChecked, null, listDataObj);

                                }
                            }
                        } else {
                            //如果不是list 在这里处理
                        }
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

    private static List<Object> getDataList(List<Object> dataList, int erc, String areaCode, int terminalAddr) {
        if (dataList.size() < 1) return null;
        List<Object> finaldataList = new ArrayList<Object>();
        TerminalArchivesObject terminalArchivesObject;
        if (erc == 17 || erc == 18 || erc == 39 || (erc >= 8 && erc <= 13) || erc == 15 || (erc >= 24 && erc <= 31) || erc == 33 || erc == 34 || (erc >= 37 && erc <= 39) || erc == 41) {
            if (erc == 17 || (erc >= 9 && erc <= 12) || erc == 13 || erc == 15 || erc == 18 || (erc >= 24 && erc <= 31) || erc == 34 || erc == 40) {
                finaldataList.add(dataList.remove(0));//qizhibiaoshi
            }
            if (erc == 39) dataList.remove(0);
            int pn = (Integer) dataList.remove(0);
            terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
            if (null == terminalArchivesObject.getID() || "".equals(terminalArchivesObject.getID())) {
                logger.error("无法从缓存获取正确的档案信息！！！！" + "areaCode=" + areaCode + "terminalAddr=" + terminalAddr);
                return null;
            }
            finaldataList.add(terminalArchivesObject.getID());
        } else if (erc == 6 || erc == 7 || erc == 19 || erc == 23) {
            int tn = (Integer) dataList.remove(0);
            terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "T" + tn);
            if (null == terminalArchivesObject.getID() || "".equals(terminalArchivesObject.getID())) {
                logger.error("无法从缓存获取正确的档案信息！！！！" + "areaCode=" + areaCode + "terminalAddr=" + terminalAddr);
                return null;
            }
            finaldataList.add(terminalArchivesObject.getID());
        }

        if (erc == 17) {
            finaldataList.add(null);//yichangbiaoshi
            finaldataList.add(null);//dianya %
            finaldataList.add(null);//dianliu %
        }
        if (erc == 14) {
            finaldataList.add(dataList.get(2));
            Object ztz = dataList.get(0);
            if (ztz == null) {
                finaldataList.add(null);
                finaldataList.add(null);
            } else {
                finaldataList.add(ztz.toString().substring(0, 1));//事件正常标志
                finaldataList.add(ztz.toString().substring(1, 2));//事件有效标志
            }
            //			finaldataList.add(dataList.get(1));//11000110
            //			finaldataList.add(dataList.get(0));//11000110
            finaldataList.add("1");
            finaldataList.add("9");
            finaldataList.add(ztz);//
            dataList.clear();
        }

        if (erc == 45) dataList.clear();

        for (Object o : dataList) {
            finaldataList.add(o);
        }
        return finaldataList;
    }
}