package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量曲线
 *
 * @author easb
 * dateMap1.put("00100200", "231005");	//日测量点总电能示值曲线		1
 * dateMap1.put("20000200", "232013");//日测量点电压曲线 1 2 3
 * dateMap1.put("20010200", "232016");//日测量点电流曲线 1 2 3
 * dateMap1.put("200A0200", "232020");//日测量点功率因数曲线 0 1 2 3
 * dateMap1.put("20040200", "232005");//日测量点功率曲线 1 2 3 4
 * dateMap1.put("20050200", "232009");//日测量点无功功率曲线 wu 5 6 7 8
 * oop-curve-dataprocessor.xml
 */
public class OopCurveDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopCurveDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET OOP CURVE DATA===");
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
            List<PersistentObject> list = new ArrayList<>();//持久类
            List<DataObject> listDataObj = new ArrayList<>();//数据类

            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                try{
                    OopCurveConcat.getDataList(meterData,protocolId,areaCode,termAddr,dar,listDataObj,"0");
                }catch (Exception e){
                    logger.error("oop curve:",e);
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
