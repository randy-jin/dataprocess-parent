package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.DateFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 面向对象电表全事件
 *
 * @author easb
 */
public class OopEventErc14DataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(OopEventErc14DataProcessor.class);
    public static final Map<String, String> codeMap = new HashMap<String, String>();
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
    static {//设置businessDataitemId
        codeMap.put("31060200", "E_EVENT_ERC14");//终端停上电事件
    }

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET OOP EVENT ERC14  DATA===");
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
            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<com.tl.hades.persist.PersistentObject>();//持久类
            List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
            List<MeterData> meterList = ternData.getMeterDataList();
            for (MeterData meterData : meterList) {
                Map<Object, Object> map = new HashMap<>();
                List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
                if (doList != null && doList.size() > 0) {
                    if (isAllNull(doList)) {
                        continue;
                    }
                    for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                        Object objectData = dataObject.getData();
                        Object objectDataItem = dataObject.getDataItem();
                        map.put(objectDataItem, objectData);
                        if (objectData == null || "null".equals(objectData)) {
                            continue;
                        }

                    }
                    if (businessDataitemId == null) {
                        continue;
                    }



                    //获取终端档案信息
                    TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode,termAddr,null);
                    String tmnlId = terminalArchivesObject.getTerminalId();

                    if (null == tmnlId || "".equals(tmnlId)) {
                        logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr );
                        continue;
                    }
                    List<Object> dataListFinal;
                    dataListFinal = getEventDateList(businessDataitemId, map, tmnlId, terminalArchivesObject);
                    if (dataListFinal == null) {
                        continue;
                    }
                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal,null, listDataObj);
                    if(ParamConstants.startWith.equals("41")){
                        String codeVal1 = codeMap.get(businessDataitemId);
                        if(codeVal1.equals("E_EVENT_ERC14")){
                            //拼接终端地址用于后续查找档案
                            String full = areaCode + termAddr;
                            String realAc1 = full.substring(3, 7);
                            String realTd1 = full.substring(7, 12);
                            String realAc = String.valueOf(Integer.parseInt(realAc1));
                            String realTd = String.valueOf(Integer.parseInt(realTd1));

                            List eventErcChecked = new ArrayList();
                            String eventId14=dataListFinal.get(0).toString();
                            eventErcChecked.add(eventId14);
                            eventErcChecked.add(dataListFinal.get(1));
                            eventErcChecked.add(realAc);
                            eventErcChecked.add(realTd);
                            List endList=dataListFinal.subList(2,dataListFinal.size());
                            eventErcChecked.addAll(endList);

                            CommonUtils.putToDataHub("erc14_checked_source", eventId14, eventErcChecked,null, listDataObj);
                        }
                    }

                }
                com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                list.add(persistentObject);//这个完整报文添加到了持久化list了
                context.setContent(list);
                return false;
            }
        }
        return true;
    }

    /**
     * 全事件
     *
     * @param dataitemID
     * @param map
     * @param tmnlId
     * @param terminalArchivesObject
     * @return
     */
    private static List<Object> getEventDateList(String dataitemID, Map<Object, Object> map, String tmnlId, TerminalArchivesObject terminalArchivesObject) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String codeVal = codeMap.get(dataitemID);
        if (codeVal == null) return null;
        List<Object> dataList = new ArrayList<Object>();
        Date starTime = null;
        Date endTime = null;
        try {
            if (isNull(map.get("201E0200"))) {
                starTime = sdf.parse(map.get("201E0200").toString());
            }
            if (isNull(map.get("20200200"))) {
                endTime = sdf.parse(map.get("20200200").toString());
            }
        } catch (Exception e) {
            logger.error("无法转换成时间格式:" + e.getMessage());
        }
        //终端停上电事件//TODO
        if(codeVal.equals("E_EVENT_ERC14")) {
            List dL = (List) map.get("33090206");
            if (dL == null || dL.size() == 0) {
                return null;
            }
            Object btzd = dL.get(0);
            String bt = null;
            if (btzd != null) {
                bt = btzd.toString();
            }
            dataList.add(idGenerator.next());
            dataList.add(tmnlId);
            Date event_date;
            if (DateFilter.isLateWeek(starTime, -7)) {
                return null;
            } else {
                event_date = starTime;
            }
            if (event_date == null || "".equals(event_date)) {
                event_date = new Date();
            }
            dataList.add(starTime);//停电发生时间
            dataList.add(endTime);//上电发生时间
            if (bt == null) {
                dataList.add(null);
                dataList.add(null);
            } else {
                dataList.add(bt.substring(0, 1));//事件正常标志
                dataList.add(bt.substring(1, 2));//事件有效标志
            }

            dataList.add("2");//规约类型
            dataList.add("9");//事件类型 停电或复电
            dataList.add(bt);//事件备用字段
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//分库字段
            dataList.add(new Date());//插入时间
            dataList.add(DateUtil.format(event_date, DateUtil.defaultDatePattern_YMD));//事件日期
        }
        return dataList;
    }

    /**
     * 判断对象不为全空
     *
     * @param dataObject
     * @return
     */
    private static boolean isAllNull(List<com.tl.hades.objpro.api.beans.DataObject> dataObject) {
        for (com.tl.hades.objpro.api.beans.DataObject data : dataObject) {
            if (data.getData() != null && !"null".equals(data.getData())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断对象不为空
     *
     * @param obj
     * @return
     */
    private static boolean isNull(Object obj) {
        if (obj == null || "null".equals(obj)) {
            return false;
        }
        return true;
    }

}