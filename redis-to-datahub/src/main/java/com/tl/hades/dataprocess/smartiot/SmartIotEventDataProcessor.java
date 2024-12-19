package com.tl.hades.dataprocess.smartiot;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.DataObject;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.OopTmnlEventConcat;
import com.tl.hades.persist.SmartIotEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 智能物联网表事件
 * @author  wjj
 * @date 2022/8/16 9:08
 */
public class SmartIotEventDataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(SmartIotEventDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext context) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        logger.info("===GET Smart Iot Event Data DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            String businessDataitemId = null;
            List oad = ternData.getDataList();
            if (oad.size() != 0) {
                businessDataitemId = (String) oad.get(0);
            }
            if (businessDataitemId == null || "".equals(businessDataitemId)) {
                businessDataitemId = ternData.getOadSign();
            }
            if (businessDataitemId == null || "".equals(businessDataitemId)) {
                if (ternData.getMeterDataList().size()>0 && ternData.getMeterDataList().get(0).getMeterData().size()>0) {
                    businessDataitemId = ternData.getMeterDataList().get(0).getMeterData().get(1).getDataItem();
                }
            }

            List<com.tl.hades.persist.DataObject> listDataObj = new ArrayList<>();//数据类
            List<MeterData> meterList = ternData.getMeterDataList();
            SmartIotEventUtils.smartIotDataHandler(meterList, areaCode, termAddr, businessDataitemId, listDataObj);

            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<>();//持久类
            com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
            persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
            list.add(persistentObject);//这个完整报文添加到了持久化list了
            context.setContent(list);
            return false;
        }

        return true;
    }

}