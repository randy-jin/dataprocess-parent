package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.OopCurveConcat;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 前台曲线入库
 *
 * @author easb
 * oop-curve-front-dataprocessor.xml
 */
public class OopCurveFrontDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopCurveFrontDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET OOP CURVE FRONT DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            int dar=ternData.getDar();
            int protocolId = ternData.getCommandType();
            if (protocolId != 8) {
                protocolId = 9;
            }
            List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
            List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类

            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                try{
                    OopCurveConcat.getDataList(meterData,protocolId,areaCode,termAddr,dar,listDataObj,"4");
                }catch (Exception e){
                    logger.error("oop curve front",e);
                }
            }
            PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
            persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
            list.add(persistentObject);//这个完整报文添加到了持久化list了
            context.setContent(list);
            return false;
        }

        return true;
    }

}
