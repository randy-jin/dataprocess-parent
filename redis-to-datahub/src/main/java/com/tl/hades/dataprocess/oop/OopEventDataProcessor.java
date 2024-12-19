package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.DateFilter;
import com.tl.hades.persist.OopTmnlEventConcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 面向对象电表全事件
 *
 * @author easb
 */
public class OopEventDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopEventDataProcessor.class);

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
            String businessDataitemId = null;
            List oad = ternData.getDataList();
            if (oad.size() != 0) {
                businessDataitemId = (String) oad.get(0);
            }
            if ("".equals(businessDataitemId) || businessDataitemId == null) {
                businessDataitemId = ternData.getOadSign();
            }
            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<>();//持久类
            List<DataObject> listDataObj = new ArrayList<>();//数据类
            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                Map<Object, Object> map = new HashMap<>();
                String meterAddr = null;

                List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
                if (doList != null && doList.size() > 0) {
                    if (OopTmnlEventConcat.isAllNull(doList)) {
                        continue;
                    }
                    for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                        Object objectData = dataObject.getData();
                        Object objectDataItem = dataObject.getDataItem();
                        map.put(objectDataItem, objectData);
                        if (objectData == null || "null".equals(objectData)) {
                            continue;
                        }
                        if ("202A0200".equals(objectDataItem)) {
                            //表地址
                            meterAddr = String.valueOf(objectData);
                        }
                    }
                    if (businessDataitemId == null) {
                        continue;
                    }
                    //erc14事件过滤掉
//                    if ("31060200".equals(businessDataitemId)) {
//                        continue;
//                    }
                    if (meterAddr == null) {
                        Object ob = map.get("20240200");
                        if (ob != null && !"null".equals(ob)) {
                            meterAddr = map.get("20240200").toString();
                        }
                    }

                    //获取档案信息
                    TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                    String tmnlId = terminalArchivesObject.getTerminalId();

                    if (null == tmnlId || "".equals(tmnlId)) {
                        logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                        continue;
                    }
                    //拼接终端地址用于后续查找档案
                    String realAc=areaCode;
                    String realTd=termAddr;
                    if(ParamConstants.startWith.equals("41")){
                        String full = areaCode + termAddr;
                        String realAc1 = full.substring(3, 7);
                        String realTd1 = full.substring(7, 12);
                        realAc = String.valueOf(Integer.parseInt(realAc1));
                        realTd = String.valueOf(Integer.parseInt(realTd1));
                    }
                    List<Object> dataListFinal = OopTmnlEventConcat.getEventDateList(businessDataitemId, map, tmnlId, terminalArchivesObject, realAc, realTd, meterAddr);
                    if (dataListFinal == null) {
                        continue;
                    }
                    if (dataListFinal.get(0) instanceof List) {
                        for (int i = 0; i < dataListFinal.size(); i++) {
                            List listOne = (List) dataListFinal.get(i);
                            String busisne = (String) listOne.remove(listOne.size() - 1);
                            if (busisne.equals("30110200")) {
                                Object checkDate = listOne.get(listOne.size() - 1);
                                boolean err = DateFilter.betweenDay(checkDate, 0, 180);
                                if (!err) {
                                    busisne = "no_power_err";
                                }
                            }

                            CommonUtils.putToDataHub(busisne, tmnlId, listOne, null, listDataObj);
                        }
                    } else {
                        CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
                    }
                    if (businessDataitemId.equals("31060200")&&terminalArchivesObject.getPowerUnitNumber().startsWith("41")) {//ERC14
                        List eventErcChecked = new ArrayList();
                        String eventId14 = dataListFinal.get(0).toString();
                        eventErcChecked.add(eventId14);
                        eventErcChecked.add(dataListFinal.get(1));
                        eventErcChecked.add(realAc);
                        eventErcChecked.add(realTd);
                        List endList = dataListFinal.subList(2, dataListFinal.size());
                        eventErcChecked.addAll(endList);
                        CommonUtils.putToDataHub("erc14_checked_source", eventId14, eventErcChecked, null, listDataObj);
                    }
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