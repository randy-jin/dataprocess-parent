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
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.EventUtils;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OtherTmnlEventsDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OtherTmnlEventsDataProcessor.class);
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
                        if(o==null){
                            logger.error("无法数据信息:" + areaCode + "_" + terminalAddr + "  data is null");
                            continue;
                        }
                        List sublist = (List) o;
                        int ercCode = (Integer) (sublist.remove(0));
                        Date event_time=null;
                        try {
                            if (ercCode == 14) {
                                if(sublist.size()==4) {
                                    sublist.remove(0);
                                }
                                event_time = (Date) sublist.get(1);
                            }
                        } catch (Exception e) {
                            logger.error("erc14",e);
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
                        String terminalId = terminalArchivesObject.getTerminalId();
                        String orgNo = terminalArchivesObject.getPowerUnitNumber();
                        if (null == terminalId || "".equals(terminalId)) {
                            logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P0");
                            return false;
                        }

                        String eventId = idGenerator.next();



                        List eventErc = new ArrayList();
                        eventErc.add(eventId);
                        eventErc.add(new BigDecimal(terminalId));
                        eventErc.add(event_time);
                        List lastData = getDataList(sublist, ercCode);
                        if (lastData == null) {
                            continue;
                        }
                        eventErc.addAll(lastData);
                        eventErc.add(orgNo.substring(0, 5));//shard_no
                        eventErc.add(new Date());
                        eventErc.add(event_date);



                        //事件子表
                        String businessDataitemId = erc + ercCode;

                        CommonUtils.putToDataHub(businessDataitemId,eventId,eventErc, null,listDataObj);

                        if (ercCode == 14) {
                            List eventErcChecked = new ArrayList();
                            eventErcChecked.add(eventId);
                            eventErcChecked.add(new BigDecimal(terminalId));
                            eventErcChecked.add(areaCode);
                            eventErcChecked.add(terminalAddr + "");
                            eventErcChecked.add(event_time);
                            eventErcChecked.addAll(lastData);
                            eventErcChecked.add(orgNo.substring(0, 5));//shard_no
                            eventErcChecked.add(new Date());
                            eventErcChecked.add(event_date);

                            CommonUtils.putToDataHub("erc14_checked_source",eventId,eventErcChecked, null,listDataObj);

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

private static List<Object> getDataList(List<Object> dataList, int erc) {
    if (dataList.size() < 1) return null;
    List<Object> finaldataList = new ArrayList<Object>();
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
        finaldataList.add("1");//PROTOCOL_TYPE
        finaldataList.add("9");//ON_OFF_TYPE
        finaldataList.add(ztz);//EVENT_REMARK
        dataList.clear();
    }
    for (Object o : dataList) {
        finaldataList.add(o);
    }
    return finaldataList;
}
}