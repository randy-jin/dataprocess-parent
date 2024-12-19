package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.OopEventConcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 电表事件--前台
 */
public class OopEventFrontDataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(OopEventFrontDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET OOP EVENT FRONT DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            int protocolId = ternData.getCommandType();
            if (protocolId != 8) {
                protocolId = 9;
            }
            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<>();//持久类
            List<com.tl.hades.persist.DataObject> listDataObj = new ArrayList<>();//数据类
            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                try{
                    OopEventConcat.getDataList(meterData,protocolId,areaCode,termAddr,listDataObj,4,ternData.getOadSign());
                }catch (Exception e){
                    logger.error("EVENTFRONT",e);
                }

            }
            com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
            persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
            list.add(persistentObject);//这个完整报文添加到了持久化list了
            context.setContent(list);
            return false;
        }
        return true;
    }
}
