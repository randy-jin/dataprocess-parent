package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.OopEvent645Concat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

/**
 * 面向对象645电表全事件
 *
 * @author easb
 */
public class OopEvent645DataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopEvent645DataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET OOP EVENT  DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            String businessDataitemId;
            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<>();//持久类
            List<DataObject> listDataObj = new ArrayList<>();//数据类
            List<Object> meterList = ternData.getDataList();
            for (Object meterData : meterList) {
                String meterAddr;
                ArrayList<Object> arrayList = (ArrayList<Object>) meterData;
                businessDataitemId = (String) arrayList.get(1);
                meterAddr = (String) arrayList.remove(arrayList.size() - 1);
                //拼接终端地址用于后续查找档案

                //获取档案信息
                TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                String tmnlId = terminalArchivesObject.getTerminalId();

                if (null == tmnlId || "".equals(tmnlId)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                    continue;
                }
                if ("03300D00".equals(businessDataitemId)) {
                    businessDataitemId = "71501";
                }
                List<Object> dataListFinal;
                dataListFinal = OopEvent645Concat.getEventDateList(businessDataitemId, terminalArchivesObject, arrayList);
                if (dataListFinal == null) {
                    continue;
                }
                if (dataListFinal.get(0) instanceof List) {
                    for (Object o : dataListFinal) {
                        List dtList = (List) o;
                        businessDataitemId = (String) dtList.remove(dtList.size() - 1);
                        CommonUtils.putToDataHub(businessDataitemId, tmnlId, dtList, null, listDataObj);
                    }
                    continue;
                }
                CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
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