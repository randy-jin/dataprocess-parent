package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TmnlHeartbeatDataProcessor extends UnsettledMessageProcessor {

private final static Logger logger = LoggerFactory.getLogger(TmnlHeartbeatDataProcessor.class);
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();

@Override
protected boolean doCanProcess(MessageContext arg0) throws Exception {
    return true;
}

@SuppressWarnings({"rawtypes", "unchecked"})
@Override
protected boolean doCanFinish(MessageContext context) throws Exception {
    //对获取的redis冻结类数据进行处理
    logger.info("===GET HEARTBEAT DATA===");
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
            for (DataItemObject data : terminalDataObject.getList()) {
                List dataList = data.getList();//本测量点的数据项xxx.xx...
                if (dataList == null || dataList.size() < 5) {
                    continue;
                }
                int fn = data.getFn();
                logger.info("===MAKE " + afn + "_" + fn + " DATA===");
//                Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
//                protocol3761ArchivesObject.setAfn(afn);
//                protocol3761ArchivesObject.setFn(fn);
                String businessDataitemId ="tmnl_heartbeat";
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
                List dataListFinal = new ArrayList<>();

                //MONITOR_ID:VARCHAR:监测记录ID.监测记录唯一标识
               //TERMINAL_ID:BIGINT:终端ID
                //CREATE_TIME:DATETIME:生成时间.入库时间
                //HEART_RATE:DECIMAL:监测心跳次数,不超过终端时钟校时周期监测次数
                //IS_CHECK:DECIMAL:是否需要校时.1:是 0:否
                //TMNL_TIME:DATETIME:终端时间
                //SERVER_TIME:DATETIME:主站时间
                //OFFSET_TIME:DECIMAL:时钟偏差（s）
                dataListFinal.add(idGenerator.next());
                dataListFinal.add(tmnlIdStr);
                dataListFinal.add(((TerminalDataObject) tmnlMessageResult).getUpTime());
                dataListFinal.add(dataList.get(2));
                dataListFinal.add(dataList.get(0));
                dataListFinal.add(dataList.get(3));
                dataListFinal.add(dataList.get(4));
                Long l= (Long) dataList.get(1);
                dataListFinal.add(l.intValue());
                CommonUtils.putToDataHub(businessDataitemId, tmnlIdStr, dataListFinal, null, listDataObj);
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