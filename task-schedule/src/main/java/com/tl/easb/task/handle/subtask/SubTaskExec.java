package com.tl.easb.task.handle.subtask;

import com.ls.athena.callmessage.multi.batch.MpedDataSet;
import com.ls.athena.callmessage.multi.batch.TmnlMessageSet;
import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import com.tl.easb.cache.dataitem.DataItemCache;
import com.tl.easb.cache.dataitem.DataItemView;
import com.tl.easb.coll.api.ZcDataManager;
import com.tl.easb.coll.api.ZcTaskManager;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.manage.AutoTaskManage;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.utils.CacheUtil;

import com.tl.easb.utils.PropertyUtils;
import com.tl.easb.utils.SpringUtils;
import com.tl.easb.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class SubTaskExec {
    private static Logger log = LoggerFactory.getLogger(SubTaskExec.class);

    private static long NOTASK_SLEEP_LONG = ParamConstants.TASK_SUBTASK_NOTASK_SLEEP;
    public static int REDIS_GAIN_SUBTASK_STEP_LENGTH = ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH;
    public static String GET_COLL_INFO_FROM_HISTORY_SQL = " SELECT MPED_ID, BUSINESS_DATAITEM_ID, TERMINAL_ID, PROTOCOL_ID FROM R_AUTOTASK_HISTORY WHERE AUTOTASK_ID = ? AND DATA_DATE = STR_TO_DATE(?, '%Y%m%d%H%i%s') AND TERMINAL_ID = ? ";
    private final static String SQRbusinessDataitemId = PropertyUtils.getProperValue("SQR_BUSINESS_DATAITEM_ID");
    private static IOperateBzData operateBzData;

    static {
        operateBzData = (IOperateBzData) SpringUtils.getBean("operateBzData");
    }

    public static void execute() {
        while (true) {
            try {
                // 从缓存检索任务状态，一个键值对，一个任务id对应一个任务状态
                Map<String, String> taskStatusMap = ZcDataManager.getAllTaskStatus();
                // 如果没有任务
                if (taskStatusMap == null || taskStatusMap.size() == 0) {
                    continue;
                }

                // 获取在执行的并且优先级最高的任务
                Task task = getTask(taskStatusMap);

                while (true) {
                    // 判断获取的任务是否为空，即判断taskStatusMap中是否有键值对
                    if (task == null || task.getTaskId() == null) {
                        break;
                    }
                    // 从缓存中获取任务
                    AutoTaskConfig autoTaskConfig = AutoTaskManage.findTaskConfig(task.getTaskId());
                    if (null == autoTaskConfig) {
                        break;
                    }

                    // 设置内存中的对象autoTaskConfig的采集数据日期跟缓存中的日期保持一致--added by
                    // JinZhiQiang at 2015/9/18
                    autoTaskConfig.setDataDate(task.getStatusParamsCollDataDate());
                    // 是否从缓存获取采集信息
                    if (task.getStatusParamsCollInfoProdMethod() == SubTaskDefine.STATUS_COLLINFO_FROM_CACHE) {
                        // 从缓存获取采集信息
                        Map<String, TmnlMessageSet> sendData = gainCpsFromCache(autoTaskConfig);
                        if (sendData == null || sendData.size() == 0) {
                            break;
                        }
                        log.info("任务【" + autoTaskConfig.getAutoTaskId() + "】从缓存获取采集信息下发数：" + sendData.size());
                        TransferToPrepositiveMachine.send(sendData, autoTaskConfig);
                        continue;
                    }

                    String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(task.getTaskId(), null);
                    if (wrapAutoTaskId.indexOf("null") > 0) {
                        break;
                    }
                    // 检索任务队列
                    List<String> subTaskIds = ZcTaskManager.drawSubtasks(wrapAutoTaskId, REDIS_GAIN_SUBTASK_STEP_LENGTH);
                    // 是否有终端信息
                    if (subTaskIds == null || subTaskIds.size() == 0) {
                        TimeUnit.SECONDS.sleep(5);
                        subTaskIds = ZcTaskManager.drawSubtasks(wrapAutoTaskId, REDIS_GAIN_SUBTASK_STEP_LENGTH);
                        if (subTaskIds == null || subTaskIds.size() == 0) {
                            CacheUtil.setTaskPause(task.getTaskId());
                            break;
                        }
                    }

                    Map<String, TmnlMessageSet> sendData = null;
                    switch (task.getStatusParamsCollInfoProdMethod()) {
                        case SubTaskDefine.STATUS_COLLINFO_FROM_CONFIG:
                            log.info("任务【" + task.getTaskId() + "】从任务配置中获取采集信息");
                            if (autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)) {// 普通任务
                                if (autoTaskConfig.getrScope() == MainTaskDefine.R_SCOPE_CP) {// 采集范围测量点
                                    if (autoTaskConfig.getCpSql().toLowerCase().indexOf("mped_id") > -1) {// 需要临时缓存
                                        sendData = gainCpsFromSql(task, autoTaskConfig, subTaskIds);
                                    } else {// 不需要临时缓存
                                        sendData = gainCpsFromConfig(task, autoTaskConfig, subTaskIds);
                                    }
                                } else if (autoTaskConfig.getrScope() == MainTaskDefine.R_SCOPE_TMNL) {// 采集范围终端
                                    sendData = gainCpsFromConfig(task, autoTaskConfig, subTaskIds);
                                } else if (autoTaskConfig.getrScope() == MainTaskDefine.R_SCOPE_TOTAL) {// 采集范围总加组
                                    sendData = gainCpsFromConfig(task, autoTaskConfig, subTaskIds);
                                }
                            } else if (autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {// 透传任务
                                if (autoTaskConfig.getCpSql().toLowerCase().indexOf("mped_id") > -1) {// 需要临时缓存
                                    sendData = gainCpsFromRedis(task, autoTaskConfig, subTaskIds);
                                } else {// 不需要临时缓存
                                    sendData = gainCpsFromConfig(task, autoTaskConfig, subTaskIds);
                                }
                            }

                            break;
                        case SubTaskDefine.STATUS_COLLINFO_FROM_HISTORY:
                            log.info("任务【" + task.getTaskId() + "】从历史表中获取采集信息");
                            sendData = gainCpsFromHistory(autoTaskConfig, subTaskIds);
                            break;
                        default:
                            ;
                    }
                    if (sendData == null || sendData.size() == 0) {
                        break;
                    }
                    // 发送数据给前置机队列
                    TransferToPrepositiveMachine.send(sendData, autoTaskConfig);
                }
            } catch (Exception e) {
                log.error("子任务执行异常：", e);
            } catch (Throwable t) {
                log.error("子任务执行异常：", t);
            } finally {
                try {
                    Thread.sleep(NOTASK_SLEEP_LONG);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }
    }


    /**
     * 报文传递的参数
     *
     * @param taskConfig
     * @return
     * @throws ParseException
     */
    private static List<Object> getDataItems(AutoTaskConfig taskConfig) throws ParseException {
        // 获取自动任务配置
        int itemsScope = taskConfig.getItemsScope();
        List<Object> list = new ArrayList<Object>();
        if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)) {// 普通任务
            switch (itemsScope) {
                case SubTaskDefine.ITEMS_SCOPE_REALTIME_DATA:
                    list.add(taskConfig.getDataDateByDate());
                    break;
                case SubTaskDefine.ITEMS_SCOPE_FREEZE_DATA:
                    list.add(taskConfig.getDataDateByDate());
                    break;
                // 月冻结修改
                case SubTaskDefine.ITEMS_SCOPE_MON_FREEZE_DATA:
                    list.add(taskConfig.getDataDateByDate());
                    break;
                case SubTaskDefine.ITEMS_SCOPE_CURVE_DATA:
                    list.add(taskConfig.getDataDateByDate());
                    list.add(taskConfig.getCurveType());
                    list.add(taskConfig.getCurveCondition());
                    break;
                case SubTaskDefine.ITEMS_SCOPE_CURVE_DATA_DAY:
                    list.add(taskConfig.getDataDateByDate());
                    list.add(taskConfig.getCurveType());
                    list.add(taskConfig.getCurveCondition());
                    break;
                default:
            }
        } else if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {// 透传
            switch (itemsScope) {
                case SubTaskDefine.ITEMS_SCOPE_TC_CURVE_DATA:
                    list.add(taskConfig.getDataDate());
                    list.add(taskConfig.getCurveCondition());
                    break;
                default:
            }
//			 list.add("");
        }

        return list;
    }

    /**
     * 组装key值
     *
     * @param areaCode
     * @param terminalAddr
     * @param afn
     * @return
     */
    public static String getKey(String areaCode, String terminalAddr, String afn) {
        return StringUtil.arrToStr(SubTaskDefine.SEPARATOR, areaCode, terminalAddr, afn);
    }

    /**
     * 获取afn
     *
     * @param key
     * @return
     */
    public static int getAfn(String key) {
        return Integer.parseInt(key.split(SubTaskDefine.SEPARATOR)[2], 16);
    }

    /**
     * 将cp集合组装成发送的数据格式
     *
     * @param cpSet
     * @param autoTaskConfig
     * @return
     * @throws ParseException
     */
    @SuppressWarnings("unchecked")
    public static Map<String, TmnlMessageSet> assembleSendData(Set<String> cpSet, AutoTaskConfig autoTaskConfig)
            throws ParseException {
        // 获取时间时标数据
        List<Object> list = getDataItems(autoTaskConfig);// 需要修改********************************************

        // 存放终端对应的mpedId
        Map<String, List<MpedDataSet>> mpedIdMap = new HashMap<String, List<MpedDataSet>>();
        // terminalId为key，TmnlMessage对象为value
        Map<String, TmnlMessageSet> sendData = new HashMap<String, TmnlMessageSet>();
        // 组装发送数据
        for (String cpData : cpSet) {
            // terminalId_测量点标识_采集日期（yyyymmddhhmmss）_数据项标识_规约类型；
            String[] cpDataParams = cpData.split(SubTaskDefine.SEPARATOR);
            // 终端标识terminalId
            String terminalId = cpDataParams[SubTaskDefine.CP_TERMINAL_ID];
            // 测量点标识
            String mpedId = cpDataParams[SubTaskDefine.CP_MPED_ID];
            // 业务数据项标识
            String bussiDataItemId = cpDataParams[SubTaskDefine.CP_DATA_ITEM_SIGN];
            // 规约类型
            String protocolId = cpDataParams[SubTaskDefine.CP_DATA_PROTOCOL_ID];
            String areaCode = cpDataParams[SubTaskDefine.CP_DATA_AREA_CODE];
            String terminalAddr = cpDataParams[SubTaskDefine.CP_DATA_TERMINAL_ADDR];
            String mpedIndex = cpDataParams[SubTaskDefine.CP_DATA_MPED_INDEX];
            String afn = cpDataParams[SubTaskDefine.CP_DATA_AFN];
            String fn = cpDataParams[SubTaskDefine.CP_DATA_FN];
            String addr = cpDataParams[SubTaskDefine.CP_DATA_ADDR];

            DataItemView dataItemView = DataItemCache.getDataItemViewById(bussiDataItemId + "_" + protocolId);

            String objFlag = null;
            String dataFlag = null;
            if (null != dataItemView) {
                objFlag = dataItemView.getObjFlag();
                if (null == objFlag) {
                    log.error("根据[" + bussiDataItemId + "]无法找出对应的OBJ_FLAG");
                    continue;
                }
                dataFlag = dataItemView.getDataFlag();
                if (null == dataFlag) {
                    log.error("根据[" + bussiDataItemId + "]无法找出对应的DATA_FLAG");
                    continue;
                }
            } else {
                log.error("根据[" + bussiDataItemId + "]无法找出对应的DataItem");
                continue;
            }

            // terminalId_bussiDataItemId 定义一条子任务，一个子任务 对应一个TmnlMessage
            String key = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, terminalId, bussiDataItemId);

            TmnlMessageSet term = sendData.get(key);
            List<MpedDataSet> mpedList = mpedIdMap.get(key);
            MpedDataSet mpedData = null;

            /********************分支箱对象组装start***********************/
            if ("230302".equals(bussiDataItemId) || "230303".equals(bussiDataItemId)) {//分支箱数据项
                Map<String, String> fzxMap = operateBzData.getFZX(areaCode, terminalAddr);
                String num = "";
                if ("230302".equals(bussiDataItemId)) {
                    num = fzxMap.get("ZCFZX");//注册分支箱数量F60
                } else if ("230303".equals(bussiDataItemId)) {
                    num = fzxMap.get("ZCDB");//注册电表数量F61
                }
                int total = 0;
                try {
                    total = Integer.parseInt(num);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                int mod = total % 8;
                int stepNo = total / 8;
                term = new TmnlMessageSet(terminalId, dataFlag);
                // 设置terminalId
                term.setTmnlId(terminalId);
                // 设置规约类型
                term.setProtocolId(protocolId);
                // 设置objFlag
                term.setDataObj(objFlag);
                // 设置dataFlag
                term.setDataFlag(dataFlag);
                term.setAreaCode(areaCode);
                term.setTmnlAddr(terminalAddr);
                // 设置是否写入数据库
                term.setWriteDatabase(true);
                // 设置是否写入内存库
                term.setWriteHbase(true);
                // 设置优先级
                term.setPriority(autoTaskConfig.getPri());

                mpedList = new ArrayList<MpedDataSet>();
                if (stepNo > 0) {
                    for (int i = 0; i < stepNo; i++) {
                        // 设置MpedData**************
                        mpedData = new MpedDataSet(String.valueOf(i + 1), dataItemView.getDataItemId());
                        mpedData.setMpedType(objFlag);
                        List paramList = new ArrayList();

                        List paramList1 = new ArrayList();
                        List paramList2 = new ArrayList();
                        List paramList3 = new ArrayList();
                        List paramList4 = new ArrayList();
                        List paramList5 = new ArrayList();
                        List paramList6 = new ArrayList();
                        List paramList7 = new ArrayList();
                        List paramList8 = new ArrayList();
                        List paramList9 = new ArrayList();
                        List paramList10 = new ArrayList();
                        int n = i * 8;
                        paramList2.add(paramList3);
                        paramList3.add(1 + n);
                        paramList2.add(paramList4);
                        paramList4.add(2 + n);
                        paramList2.add(paramList5);
                        paramList5.add(3 + n);
                        paramList2.add(paramList6);
                        paramList6.add(4 + n);
                        paramList2.add(paramList7);
                        paramList7.add(5 + n);
                        paramList2.add(paramList8);
                        paramList8.add(6 + n);
                        paramList2.add(paramList9);
                        paramList9.add(7 + n);
                        paramList2.add(paramList10);
                        paramList10.add(8 + n);

                        paramList1.add(paramList2);
                        paramList = paramList1;

                        mpedData.setExtraList(paramList);//测点参数list
                        mpedData.setAfn(afn);
                        mpedData.setFn(fn);
                        mpedData.setPn("0");
                        mpedList.add(mpedData);
                        term.setMpedDataList(mpedList);
                    }
                }
                if (mod > 0) {
                    // 设置MpedData**************
                    mpedData = new MpedDataSet(String.valueOf(stepNo + 1), dataItemView.getDataItemId());
                    mpedData.setMpedType(objFlag);
                    List paramList = new ArrayList();

                    List paramList1 = new ArrayList();
                    List paramList2 = new ArrayList();
                    List paramList3 = new ArrayList();
                    List paramList4 = new ArrayList();
                    List paramList5 = new ArrayList();
                    List paramList6 = new ArrayList();
                    List paramList7 = new ArrayList();
                    List paramList8 = new ArrayList();
                    List paramList9 = new ArrayList();
                    List paramList10 = new ArrayList();
                    int n = stepNo * 8;
                    paramList2.add(paramList3);
                    paramList3.add(1 + n);
                    paramList2.add(paramList4);
                    paramList4.add(2 + n);
                    paramList2.add(paramList5);
                    paramList5.add(3 + n);
                    paramList2.add(paramList6);
                    paramList6.add(4 + n);
                    paramList2.add(paramList7);
                    paramList7.add(5 + n);
                    paramList2.add(paramList8);
                    paramList8.add(6 + n);
                    paramList2.add(paramList9);
                    paramList9.add(7 + n);
                    paramList2.add(paramList10);
                    paramList10.add(8 + n);

                    for (int i = 7; i > mod - 1; i--) {
                        paramList2.remove(i);
                    }

                    paramList1.add(paramList2);
                    paramList = paramList1;

                    mpedData.setExtraList(paramList);//测点参数list
                    mpedData.setAfn(afn);
                    mpedData.setFn(fn);
                    mpedData.setPn("0");
                    mpedList.add(mpedData);
                    term.setMpedDataList(mpedList);
                }
                sendData.put(key, term);
                continue;
            }
            /********************分支箱对象组装end***********************/

            if (term == null) { // 如果key返回为空
                // 重新构造TmnlMessage
                term = new TmnlMessageSet(terminalId, dataFlag);
                // 设置terminalId
                term.setTmnlId(terminalId);
                // 设置规约类型
                term.setProtocolId(protocolId);
                // 设置objFlag
                term.setDataObj(objFlag);
                // 设置dataFlag
                term.setDataFlag(dataFlag);
                term.setAreaCode(areaCode);
                term.setTmnlAddr(terminalAddr);
                // 判断任务类型，特殊任务传不同dataFlag****************************
                if (dataFlag.equals(MainTaskDefine.ITEMS_SCOPE_CURVE_READ + "")) {// 如果是曲线数据
                    if (autoTaskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ) {// 当日曲线
                        // term.setDataFlag("");
                    }
                    if (autoTaskConfig.getCurveType() == MainTaskDefine.CURVE_TYPE_15) {// 96曲线
                        term.setDataFlag(MainTaskDefine.ITEMS_SCOPE_CURVE_READ_96);
                    }
                    if (autoTaskConfig.getCurveType() == MainTaskDefine.CURVE_TYPE_5) {// 288曲线
                        term.setDataFlag(MainTaskDefine.ITEMS_SCOPE_CURVE_READ_288);
                    }
                }
                // ******************************************************
                // 设置是否写入数据库
                term.setWriteDatabase(true);
                // 设置是否写入内存库
                term.setWriteHbase(true);
                // 设置优先级
                term.setPriority(autoTaskConfig.getPri());
                mpedList = new ArrayList<MpedDataSet>();
            }

            // 设置MpedData**************
            mpedData = new MpedDataSet(mpedId, dataItemView.getDataItemId());
            mpedData.setMpedId(mpedId);
            mpedData.setMpedType(objFlag);
            mpedData.setDataItemId(bussiDataItemId);
            // sg3761
            mpedData.setAfn(afn);
            mpedData.setFn(fn);
            mpedData.setPn(mpedIndex);
            mpedData.setMpedAddress(addr);
            if (autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {// 透传任务
                Map<String, String> pMap = operateBzData.getAllPMpedFromMP(mpedId);
                // 取到的测点为空，则跳过此次循环
                if (pMap == null || pMap.size() == 0) {
                    log.error("根据mpedId：" + mpedId + "从redis中获取的测量点档案信息为空");
                    continue;
                }
                String meterProtocolId = pMap.get("PTL");//电表规约
                if (meterProtocolId.equals("8") && protocolId.equals("1") && autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {
                    dataItemView = DataItemCache.getDataItemViewById(bussiDataItemId + "_698" + "_" + protocolId);
                } else {
                    dataItemView = DataItemCache.getDataItemViewById(bussiDataItemId + "_" + protocolId);
                }
                if (null == dataItemView) {
                    log.error("根据[" + bussiDataItemId + "_" + protocolId + "]无法找出对应的dataItemView");
                    continue;
                }
                String PARAM = pMap.get("PARAM");
                mpedData.setPn("0");
                mpedData.setMpedAddress(pMap.get("ADDR"));
                mpedData.setPort(pMap.get("PORT"));
                String[] cmd = dataItemView.getCmd().split("-");
                if (cmd.length == 5) {
                    mpedData.setCtrl(cmd[3]);
                    mpedData.setDI(cmd[4]);
                } else if (cmd.length == 6) {
                    mpedData.setCtrl(cmd[4]);
                    mpedData.setDI(cmd[5]);
                } else {
                    log.info("透传任务cmd长度不够5位,cmd==" + cmd);
                }
                String[] mpedParam = PARAM.replace("|", "-").split("-");
                mpedData.setBaud(Integer.parseInt(mpedParam[0]) + "");
                mpedData.setStopBit("0");// mpedParam[1]测试用写死
                mpedData.setNocheckOut(mpedParam[2]);
                mpedData.setCheckOut("0");// mpedParam[3]测试用写死
                mpedData.setNumber(mpedParam[4]);
                mpedData.setTimeOutUnit(mpedParam[5]);
                // mpedData.setTimeOut(mpedParam[6]);
                mpedData.setTimeOut("90");
                mpedData.setTimeOutDelay(mpedParam[7]);
            }
            //TODO 关于终端下发时包含测量点序号集合 2020-06-03 hch 新增  采集范围为"终端" 采集数据项范围为"实时数据" "参数查询"
            if (autoTaskConfig.getrScope() == MainTaskDefine.R_SCOPE_TMNL && (autoTaskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_TIME || autoTaskConfig.getItemsScope() == 2)) {//所有终端召测都传入pnlist
                Map<String, String> tmnlMap = operateBzData.getObjectByPn(areaCode, terminalAddr);
                // 终端参数需要传pn用
                List<Object> pnList = new ArrayList<Object>();
                for (Entry<String, String> entry : tmnlMap.entrySet()) {
                    if (entry.getKey().startsWith("P")) {
                        String pIndex = entry.getKey().toString().substring(1, entry.getKey().toString().length());
                        pnList.add(pIndex);
                    }
                }
                mpedData.setExtraList(pnList);
//					term.setDataFlag(MainTaskDefine.DATA_FLAG_PARAM_QUERY_SG3761_0AF10);
            } else {
                mpedData.setExtraList(list);
            }

            mpedList.add(mpedData);
            // 设置mpedDataList
            term.setMpedDataList(mpedList);

            mpedIdMap.put(key, mpedList);
            sendData.put(key, term);
        }
        return sendData;
    }

    /**
     * 从缓存中取CPS信息
     *
     * @param autoTaskConfig
     * @throws Exception
     */
    private static Map<String, TmnlMessageSet> gainCpsFromCache(AutoTaskConfig autoTaskConfig) throws Exception {
        String autotaskId = autoTaskConfig.getAutoTaskId();
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, null);
        Set<String> cpSet = new HashSet<String>();
        /***********补采直到获取完下发终端信息跳出 qin add 2018 11 07*************/
        while (true) {
            // 获取redis里面的task对应的子任务
            List<String> subTaskIds = ZcTaskManager.drawSubtasks(wrapAutoTaskId, REDIS_GAIN_SUBTASK_STEP_LENGTH);
            // 如果获取子任务id的链表为空或者获取的list大小为0，则返回不执行
            if (subTaskIds == null || subTaskIds.size() == 0) {
                CacheUtil.setTaskPause(autotaskId);
                return null;
            }
            cpSet = ZcTaskManager.getCps(subTaskIds);
            if (cpSet.size() > 0) {
                break;
            }
        }

        Set<String> allCpSetLong = new HashSet<String>();
        CacheUtil.incrTaskCounter(autotaskId, cpSet.size());
        for (String cp : cpSet) {
            String[] cpDataParams = cp.split(SubTaskDefine.SEPARATOR);
            // 终端标识terminalId
//            String terminalId = cpDataParams[SubTaskDefine.CP_TERMINAL_ID];
            String mpedId = cpDataParams[SubTaskDefine.CP_MPED_ID];
            String dataitemId = cpDataParams[SubTaskDefine.CP_DATA_ITEM_SIGN];// BUSINESS_DATAITEM_ID
            String protocolId = cpDataParams[SubTaskDefine.CP_DATA_PROTOCOL_ID];
//            String areaCode = cpDataParams[SubTaskDefine.CP_DATA_AREA_CODE];
//            String terminalAddr = cpDataParams[SubTaskDefine.CP_DATA_TERMINAL_ADDR];

//            String areaTerminalAdd = operateBzData.getTmnl(terminalId,"ADDR");
//            String[] areaTerminal = areaTerminalAdd.split("\\|");
//            String areaCode = areaTerminal[0];
//            String terminalAddr = areaTerminal[1];

            DataItemView dataItemView = DataItemCache.getDataItemViewById(dataitemId + "_" + protocolId);
            Map<String, String> pMap = operateBzData.getAllPMpedFromMP(mpedId);
            String pn = pMap.get("MPINDEX");// 表地址
            String addr = pMap.get("ADDR");// 表地址
            String cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, pn,
                    dataItemView.getAFN_FN(), addr);
            allCpSetLong.add(cpLong);

//            DataItemView dataItemView = DataItemCache.getDataItemViewById(dataitemId + "_" + protocolId);
//            List<Object> list = operateBzData.hmget("MP$" + mpedId, new String[]{"MPINDEX", "ADDR"});
//            if (null == list || list.isEmpty() || null == list.get(0)) {
//                continue;
//            }
//            String pn = (String) list.get(0);
//            String addr = (String) list.get(1);// 表地址
//            String cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, pn,
//                    dataItemView.getAFN_FN(), addr);
//            allCpSetLong.add(cpLong);
//            String pn = (String) operateBzData.getAllPMpedFromMP(mpedId).get("MPINDEX");
//            Map<String, String> pMap = operateBzData.getAllPMped(areaCode, terminalAddr, pn);
//            String addr = pMap.get("ADDR");// 表地址
        }
        // if
        // (autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED))
        // {////透传任务
        // return assembleSendData(cpSet, autoTaskConfig);
        // }
        return assembleSendData(allCpSetLong, autoTaskConfig);
    }

    /**
     * 透传任务从临时缓存中取测点信息
     *
     * @param task
     * @param autoTaskConfig
     * @param subTaskIds
     * @throws SQLException
     */
    private static Map<String, TmnlMessageSet> gainCpsFromRedis(Task task, AutoTaskConfig autoTaskConfig,
                                                                List<String> subTaskIds) throws Exception {
        String autotaskId = autoTaskConfig.getAutoTaskId();
        // 获取采集范围
        int scope = autoTaskConfig.getrScope();
        // 该任务的数据项
        Map<String, List<String>> dataitems = DataItemCache.getDataitemIdByAutotaskId(autotaskId);
        List<String> dataitemSigns = dataitems.get(autotaskId);
        if (null == dataitemSigns) {
            throw new Exception("任务【" + autotaskId + "】对应的数据项配置信息为空");
        }
        // 测点集合
        Set<String> allCpSet = new HashSet<String>();
        // 用于参数传递
        Set<String> allCpSetLong = new HashSet<String>();

        // 计算分发数
        long cpOfNum = 0;
        // 子任务对应的测点
        Map<String, Set<String>> taskCpData = new HashMap<String, Set<String>>();
        for (String subtaskId : subTaskIds) {
            String[] subtaskParams = subtaskId.split(SubTaskDefine.SEPARATOR);
            // String autoTaskId =
            // subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TASKID];
            String areaCode = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_AREA];
            String terminalAddr = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ADDR];
            String terminalId = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ID];

            /*
             * 通过行政区码和终端地址向临时缓存取测量点信息
             */
            Map<String, String> map = CacheUtil.getTaskScope(autotaskId, areaCode, terminalAddr);// 取临时缓存

            // 取到的测点为空，则跳过此次循环
            if (map == null || map.size() == 0) {
//                log.error("任务【" + autotaskId + "】的子任务【" + subtaskId + "】根据" + areaCode + "和" + terminalAddr
//                        + "从临时缓存中获取的档案信息为空");
                continue;
            }

            Set<String> cpSet = new HashSet<String>();

            Iterator<Entry<String, String>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, String> entry = (Entry<String, String>) iter.next();
                String mpedId = entry.getValue();
                // 存储的pn前面带有一个字母的前缀
                String pn = entry.getKey();
                pn = pn.substring(1, pn.length());
                Map<String, String> pMap = operateBzData.getAllPMped(areaCode, terminalAddr, pn);
                //TODO 不需要电表规约
                String meterProtocolId = pMap.get("MPID");// 电表规约规约类型
                String addr = pMap.get("ADDR");// 表地址
                String protocolId = (String) operateBzData.getObjectByPn(areaCode, terminalAddr).get("TPID");
                for (String dataitemSign : dataitemSigns) {
                    //TODO 3761下698特殊处理
                    DataItemView dataItemView = null;
                    if (meterProtocolId.equals("8") && protocolId.equals("1") && autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {
                        dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_698" + "_" + protocolId);
                    } else {
                        dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_" + protocolId);
                    }
                    if (null == dataItemView) {
                        log.error("根据[" + dataitemSign + "_" + protocolId + "]无法找出对应的dataItemView");
                        continue;
                    }
                    // terminalId_测量点标识_采集日期（yyyymmddhhmmss）_数据项标识_终端规约类型
                    String cp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, terminalId, mpedId,
                            task.getStatusParamsCollDataDate(), dataitemSign, protocolId);
                    String cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, areaCode, terminalAddr, pn,
                            dataItemView.getAFN_FN(), addr, meterProtocolId);
                    cpSet.add(cp);
                    allCpSet.add(cp);
                    allCpSetLong.add(cpLong);
                    cpOfNum++;
                }
            }
            if (cpSet.size() > 0) {
                taskCpData.put(subtaskId, cpSet);
            }
        }

        if (taskCpData.size() == 0) {
            throw new Exception("任务【" + task.getTaskId() + "】对应的测点信息为空");
        }

        /*
         * ZcDataManager.initCpAndSubtaskDs()方法的返回值应该返回是的重复的信息，
         * 因此在接下来的操作需要给subtask2Cps去掉重复的数据 此方法不保存重复的测点信息
         */
        log.info("任务【" + autotaskId + "】本次发送的测点信息数：" + taskCpData.size());
        List<String> existedCps = ZcDataManager.initCpAndSubtaskDs(taskCpData);
        log.info("任务【" + autotaskId + "】对应的缓存中已存在的测点信息数：" + existedCps.size());
        // 发送测点需要去除重复
        delExistedCps(allCpSet, allCpSetLong, existedCps);
        log.info("任务【" + autotaskId + "】去重后发送给前置机的测点信息数：" + allCpSet.size());
        CacheUtil.incrTaskCounter(autotaskId, cpOfNum);

        return assembleSendData(allCpSetLong, autoTaskConfig);
    }

    /**
     * 普通任务采集测量点从临时缓存取测点信息
     *
     * @param task
     * @param autoTaskConfig
     * @param subTaskIds
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private static Map<String, TmnlMessageSet> gainCpsFromSql(Task task, AutoTaskConfig autoTaskConfig,
                                                              List<String> subTaskIds) throws Exception {
        String autotaskId = autoTaskConfig.getAutoTaskId();
        // 获取采集范围
        int scope = autoTaskConfig.getrScope();
        // 该任务的数据项业务id
        Map<String, List<String>> dataitems = DataItemCache.getDataitemIdByAutotaskId(autotaskId);
        List<String> dataitemSigns = dataitems.get(autotaskId);
        if (null == dataitemSigns) {
            throw new Exception("任务【" + autotaskId + "】对应的数据项配置信息为空");
        }
        // 测点集合
        Set<String> allCpSet = new HashSet<String>();
        Set<String> allCpSetLong = new HashSet<String>();

        // 计算分发数
        long cpOfNum = 0;
        // 子任务对应的测点
        Map<String, Set<String>> taskCpData = new HashMap<String, Set<String>>();
        for (String subtaskId : subTaskIds) {
            String[] subtaskParams = subtaskId.split(SubTaskDefine.SEPARATOR);
            String areaCode = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_AREA];
            String terminalAddr = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ADDR];
            String terminalId = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ID];

            /*
             * 通过行政区码和终端地址向临时缓存取档案信息
             */
            Map<String, String> map = CacheUtil.getTaskScope(autotaskId, areaCode, terminalAddr);// 取临时缓存

            // 取到的测点为空，则跳过此次循环
            if (map == null || map.size() == 0) {
                log.error("任务【" + autotaskId + "】的子任务【" + subtaskId + "】根据" + areaCode + "和" + terminalAddr
                        + "从临时缓存中获取的档案信息为空");
                continue;
            }

            Set<String> cpSet = new HashSet<String>();
            Iterator<Entry<String, String>> iter = map.entrySet().iterator();
            String protocolId = "";
            if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
                protocolId = (String) operateBzData.getObjectByPnSQR(areaCode, terminalAddr).get("TPID");
            } else {
                protocolId = (String) operateBzData.getObjectByPn(areaCode, terminalAddr).get("TPID");
            }
            if (null == protocolId) {
                log.error("根据行政区码[" + areaCode + "]和终端地址[" + terminalAddr + "]无法从缓存中获取TPID，map的size为：" + map.size());
                continue;
            }
            while (iter.hasNext()) {
                Entry<String, String> entry = (Entry<String, String>) iter.next();
                // 存储的pn前面带有一个字母的前缀
                String pn = entry.getKey();
                pn = pn.substring(1, pn.length());
                String mpedId = entry.getValue();
                Map<String, String> pMap = operateBzData.getAllPMped(areaCode, terminalAddr, pn);
                String addr = pMap.get("ADDR");// 表地址
                for (String dataitemSign : dataitemSigns) {
                    DataItemView dataItemView = null;
                    String meterProtocolId = pMap.get("MPID");// 电表规约规约类型
                    //3761下698特殊处理
                    if (meterProtocolId.equals("8") && protocolId.equals("1") && autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {
                        dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_698" + "_" + protocolId);
                    } else {
                        dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_" + protocolId);
                    }
                    if (null == dataItemView) {
                        log.error("根据[" + dataitemSign + "_" + protocolId + "]无法找出对应的dataItemView");
                        continue;
                    }
                    String cp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, terminalId, mpedId,
                            task.getStatusParamsCollDataDate(), dataitemSign, protocolId);
                    // terminalId_测量点标识_采集日期（yyyymmddhhmmss）_数据项业务标识(BUSINESS_DATAITEM_ID)_规约类型
                    String cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, areaCode, terminalAddr, pn,
                            dataItemView.getAFN_FN(), addr);
                    cpSet.add(cp);
                    allCpSet.add(cp);
                    allCpSetLong.add(cpLong);
                    cpOfNum++;
                }

            }
            if (cpSet.size() > 0) {
                taskCpData.put(subtaskId, cpSet);
            }
        }

        if (taskCpData.size() == 0) {
            throw new Exception("任务【" + task.getTaskId() + "】对应的测点信息为空");
        }

        /*
         * ZcDataManager.initCpAndSubtaskDs()方法的返回值应该返回是的重复的信息，
         * 因此在接下来的操作需要给subtask2Cps去掉重复的数据 此方法不保存重复的测点信息
         */
        log.info("任务【" + autotaskId + "】本次发送的测点信息数：" + taskCpData.size());
        List<String> existedCps = ZcDataManager.initCpAndSubtaskDs(taskCpData);
        log.info("任务【" + autotaskId + "】对应的缓存中已存在的测点信息数：" + existedCps.size());
        // 发送测点需要去除重复
        delExistedCps(allCpSet, allCpSetLong, existedCps);
        log.info("任务【" + autotaskId + "】去重后发送给前置机的测点信息数：" + allCpSet.size());
        CacheUtil.incrTaskCounter(autotaskId, cpOfNum);

        return assembleSendData(allCpSetLong, autoTaskConfig);
    }

    /**
     * 从配置表中取测点信息
     *
     * @param task
     * @param autoTaskConfig
     * @param subTaskIds
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private static Map<String, TmnlMessageSet> gainCpsFromConfig(Task task, AutoTaskConfig autoTaskConfig,
                                                                 List<String> subTaskIds) throws Exception {
        String autotaskId = autoTaskConfig.getAutoTaskId();
        // 获取采集范围
        int scope = autoTaskConfig.getrScope();
        // 该任务的数据项
        Map<String, List<String>> dataitems = DataItemCache.getDataitemIdByAutotaskId(autotaskId);
        List<String> dataitemSigns = dataitems.get(autotaskId);
        if (null == dataitemSigns) {
            throw new Exception("任务【" + autotaskId + "】对应的数据项配置信息为空");
        }
        // 测点集合，用于初始化缓存
        Set<String> allCpSet = new HashSet<String>();
        // 用于参数传递
        Set<String> allCpSetLong = new HashSet<String>();

        // 计算分发数
        long cpOfNum = 0;
        // 子任务对应的测点
        Map<String, Set<String>> taskCpData = new HashMap<String, Set<String>>();
        for (String subtaskId : subTaskIds) {
            String[] subtaskParams = subtaskId.split(SubTaskDefine.SEPARATOR);
            String areaCode = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_AREA];
            String terminalAddr = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ADDR];
            String terminalId = subtaskParams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ID];
            String protocolId = "";

            /*
             * 通过行政区码和终端地址向redis取档案信息
             */
            Map<String, String> map = null;
            if (autoTaskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURVE_READ
                    && autoTaskConfig.getCurveType() == MainTaskDefine.CURVE_TYPE_5) {
                map = new HashMap<String, String>();
                map.put("P1", "0");
            } else {
                switch (scope) {
                    case MainTaskDefine.R_SCOPE_CP:// 测量点
                        // 获取测点编号
                        if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
                            map = operateBzData.getAllPointNumSQR(areaCode, terminalAddr);// map.put("Pn", mpedId);
                        } else {
                            map = operateBzData.getAllPointNum(areaCode, terminalAddr);// map.put("Pn", mpedId);
                        }
                        break;
                    case MainTaskDefine.R_SCOPE_TMNL:// 终端
                        map = new HashMap<String, String>();
                        map.put("P0", terminalId);
                        break;
                    case MainTaskDefine.R_SCOPE_TOTAL:// 总加组
                        // 获取总加组号
                        if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
                            map = operateBzData.getAlltotalNumberSQR(areaCode, terminalAddr);
                        } else {
                            map = operateBzData.getAlltotalNumber(areaCode, terminalAddr);
                        }
                        map.put("P0", "");// 方法待定
                        break;
                    default:
                        if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
                            map = operateBzData.getAllPointNumSQR(areaCode, terminalAddr);
                        } else {
                            // 获取该终端下的所有测量点信息
                            map = operateBzData.getAllPointNum(areaCode, terminalAddr);
                        }
                        break;
                }
                // 取到的测点为空，则跳过此次循环
                if (map == null || map.size() == 0) {
//                    log.error("任务【" + autotaskId + "】的子任务【" + subtaskId + "】根据" + areaCode + "和" + terminalAddr
//                            + "从redis中获取的档案信息为空");
                    continue;
                }
            }

            Set<String> cpSet = new HashSet<String>();
            Iterator<Entry<String, String>> iter = map.entrySet().iterator();
//            String protocolId = "";
//            if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
//                protocolId = (String) operateBzData.getObjectByPnSQR(areaCode, terminalAddr).get("TPID");
//            } else {
//                protocolId = (String) operateBzData.getObjectByPn(areaCode, terminalAddr).get("TPID");
//            }

            protocolId = map.get("TPID");
            if (null == protocolId) {
                if (dataitemSigns.get(0).equals(SQRbusinessDataitemId) && dataitemSigns.size() == 1) {
                    protocolId = (String) operateBzData.getObjectByPnSQR(areaCode, terminalAddr).get("TPID");
                } else {
                    protocolId = (String) operateBzData.getObjectByPn(areaCode, terminalAddr).get("TPID");
                }
            }
            if(null == protocolId){
                log.error("根据行政区码[" + areaCode + "]和终端地址[" + terminalAddr + "]无法从缓存中获取TPID，map的size为：" + map.size());
                continue;
            }
            while (iter.hasNext()) {
                Entry<String, String> entry = (Entry<String, String>) iter.next();
                if (entry.getKey().startsWith("P")) {
                    String mpedId = entry.getValue();
                    String pn = entry.getKey();
                    pn = pn.substring(1, pn.length());
                    String addr = map.get(mpedId);// 表地址
                    for (String dataitemSign : dataitemSigns) {
                        DataItemView dataItemView = null;
                        String cp = "";
                        String cpLong = "";
//                        Map<String, String> pMap = operateBzData.getAllPMped(areaCode, terminalAddr, pn);
//                        String addr = pMap.get("ADDR");// 表地址
                        if (autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {// 透传任务需要追加电表规约
                            List<Object> list = operateBzData.hmget("P$" + areaCode + "#" + terminalAddr + "#" + pn, new String[]{"MPID"});
                            if (null == list || list.isEmpty() || null == list.get(0)) {
                                continue;
                            }
//                            addr = (String) list.get(0);// 表地址
//                            String meterProtocolId = pMap.get("MPID");// 电表规约规约类型
                            String meterProtocolId = (String) list.get(0);// 电表规约规约类型
//                            String meterProtocolId = pMap.get("MPID");// 电表规约规约类型
                            //3761下698特殊处理
                            if (meterProtocolId.equals("8") && protocolId.equals("1") && autoTaskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {
                                dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_698" + "_" + meterProtocolId);
                            } else {
                                dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_" + meterProtocolId);
                            }

                            if (null == dataItemView) {
                                log.error("根据[" + dataitemSign + "_" + meterProtocolId + "]无法找出对应的dataItemView");
                                continue;
                            }
                            cp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, terminalId, mpedId,
                                    task.getStatusParamsCollDataDate(), dataitemSign, protocolId);
                            cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, areaCode, terminalAddr, pn,
                                    dataItemView.getAFN_FN(), addr, meterProtocolId);
                            // terminalId_测量点序号_采集日期（yyyymmddhhmmss）_数据项标识_规约类型
                        } else {
                            dataItemView = DataItemCache.getDataItemViewById(dataitemSign + "_" + protocolId);
                            if (null == dataItemView) {
                                log.error("根据[" + dataitemSign + "_" + protocolId + "]无法找出对应的dataItemView");
                                continue;
                            }
                            cp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, terminalId, mpedId,
                                    task.getStatusParamsCollDataDate(), dataitemSign, protocolId);
                            cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp, areaCode, terminalAddr, pn,
                                    dataItemView.getAFN_FN(), addr);
                            // terminalId_测量点序号_采集日期（yyyymmddhhmmss）_数据项标识_规约类型
                        }
                        cpSet.add(cp);
                        allCpSet.add(cp);
                        allCpSetLong.add(cpLong);
                        cpOfNum++;
                    }
                }
            }
            if (cpSet.size() > 0) {
                taskCpData.put(subtaskId, cpSet);
            }
        }

        if (taskCpData.size() == 0) {
            throw new Exception("任务【" + task.getTaskId() + "】对应的测点信息为空");
        }

        /*
         * ZcDataManager.initCpAndSubtaskDs()方法的返回值应该返回是的重复的信息，
         * 因此在接下来的操作需要给subtask2Cps去掉重复的数据 此方法不保存重复的测点信息
         */
//		log.info("任务【" + autotaskId + "】本次发送的测点信息数：" + taskCpData.size());
        List<String> existedCps = ZcDataManager.initCpAndSubtaskDs(taskCpData);
//		log.info("任务【" + autotaskId + "】对应的缓存中已存在的测点信息数：" + existedCps.size());
        // 发送测点需要去除重复
        delExistedCps(allCpSet, allCpSetLong, existedCps);
        log.info("任务【" + autotaskId + "】去重后发送给前置机的测点信息数：" + allCpSet.size());
        CacheUtil.incrTaskCounter(autotaskId, cpOfNum);

        return assembleSendData(allCpSetLong, autoTaskConfig);
    }

    /**
     * 判断当前测量点类型是否匹配该任务制定的任务类型
     *
     * @param userFlagInTaskList
     * @param userFlag
     * @return true：匹配，false：不匹配
     */
    private static boolean isUserFlagInTask(List<String> userFlagInTaskList, String userFlag) {
        if ("0".equals(userFlag)) {
            return true;
        }
        if (null == userFlag) {
            return false;
        }
        if (userFlag.length() == 1) {
            userFlag = "0" + userFlag;
        }
        for (String _temp : userFlagInTaskList) {
            if (null != _temp && _temp.equals(userFlag)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 从历史表中取测点信息
     *
     * @param autoTaskConfig
     * @param subTaskIds
     * @throws Exception
     */
    private static Map<String, TmnlMessageSet> gainCpsFromHistory(AutoTaskConfig autoTaskConfig,
                                                                  List<String> subTaskIds) throws Exception {
        long cpOfNum = 0;
        Set<String> allCpSet = new HashSet<String>();
        // 用于参数传递
        Set<String> allCpSetLong = new HashSet<String>();
        Map<String, Set<String>> taskCpData = new HashMap<String, Set<String>>();
        for (String subtaskId : subTaskIds) {
            String[] subtaskparams = subtaskId.split(SubTaskDefine.SEPARATOR);
            ResultSet rs = DBUtils.executeQuery(GET_COLL_INFO_FROM_HISTORY_SQL,
                    new String[]{autoTaskConfig.getAutoTaskId(), autoTaskConfig.getDataDate(),
                            subtaskparams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ID]});

            Set<String> cpSet = new HashSet<String>();
            while (rs.next()) {
                // TERMINAL_ID
                String terminalId = rs.getString("TERMINAL_ID");
                // 测量点序号
                String mpedId = rs.getString("MPED_ID");
                // 业务数据项标识
                String dataitemId = rs.getString("BUSINESS_DATAITEM_ID");
                // 规约类型
                String protocolId = rs.getString("PROTOCOL_ID");
                String areaTerminalAdd = operateBzData.getTmnl(terminalId, "ADDR");
                DataItemView dataItemView = DataItemCache.getDataItemViewById(dataitemId + "_" + protocolId);
                String[] areaTerminal = areaTerminalAdd.split("\\|");
                String areaCode = areaTerminal[0];
                String terminalAddr = areaTerminal[1];
                String pn = (String) operateBzData.getAllPMpedFromMP(mpedId).get("MPINDEX");
                String addr = (String) operateBzData.getAllPMped(areaCode, terminalAddr, pn).get("ADDR");
                // terminalId_测量点序号_采集日期（yyyymmddhhmmss）_数据项标识_规约类型
                String cpInfo = StringUtil.arrToStr(SubTaskDefine.SEPARATOR,
                        subtaskparams[SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ID], mpedId, autoTaskConfig.getDataDate(),
                        dataitemId, protocolId);
                String cpLong = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cpInfo, areaCode, terminalAddr, pn,
                        dataItemView.getAFN_FN(), addr);
                cpSet.add(cpInfo);
                allCpSetLong.add(cpLong);
                allCpSet.add(cpInfo);
                cpOfNum++;
            }
            taskCpData.put(subtaskId, cpSet);
        }

        if (taskCpData.size() == 0) {
            throw new Exception("任务【" + autoTaskConfig.getAutoTaskId() + "】对应的测点信息为空");
        }

        /*
         * ZcDataManager.initCpAndSubtaskDs()方法的返回值返回是采集点(key)和子任务(value)
         * 表中已经存在的数据， 已经存在的数据不需要重复插入而且重复的数据不需要重复发送给前置
         * 因此在接下来的操作需要给subtask2Cps去掉重复的数据
         */
        log.info("任务【" + autoTaskConfig.getAutoTaskId() + "】对应的测点信息数：" + taskCpData.size());
        List<String> existedCps = ZcDataManager.initCpAndSubtaskDs(taskCpData);
        log.info("任务【" + autoTaskConfig.getAutoTaskId() + "】对应的缓存中已存在的测点信息数：" + existedCps.size());
        CacheUtil.incrTaskCounter(autoTaskConfig.getAutoTaskId(), cpOfNum);
        // 去除重复的数据--mod by jinzhiqiang 采集历史冻结数据的情况下，不再去重
        // delExistedCps(allCpSet, existedCps);
        log.info("任务【" + autoTaskConfig.getAutoTaskId() + "】采集历史冻结数据不再去重，发送给前置机的测点数：" + allCpSet.size());
        return assembleSendData(allCpSetLong, autoTaskConfig);
    }

    private static void delExistedCps(Set<String> allCpSet, Set<String> allCpSetLong, List<String> existedCps) {
        if (existedCps == null || existedCps.size() == 0) { // 判断已经存在的监测点是否为空
            log.info("没有需要去掉的重复数据。");
            return;
        }
        Set<String> retSet = new HashSet<>();
        // 把为值为空的map删除
        for (String cp : existedCps) {
            allCpSet.remove(cp);
            for (String cpLong : allCpSetLong) {
                if (!cpLong.contains(cp)) {
                    retSet.add(cpLong);
                }
            }
        }
        allCpSetLong = retSet;
    }

    /**
     * 从任务状态块中获取优先级最高的正在执行的任务
     *
     * @param taskStatusMap
     * @return
     */
    private static Task getTask(final Map<String, String> taskStatusMap) {
        Task task = new Task();

        Iterator<Entry<String, String>> iter = taskStatusMap.entrySet().iterator();

        while (iter.hasNext()) {
            Entry<String, String> entry = (Entry<String, String>) iter.next();
            String taskId = entry.getKey();
            String[] taskStatusParams = entry.getValue().split(SubTaskDefine.SEPARATOR);

            // 如果任务不在执行中，那么跳过
            if (SubTaskDefine.STATUS_EXEC_FLAG_STOP.equals(taskStatusParams[SubTaskDefine.STATUS_PARAMS_EXEC_FLAG])) {
                continue;
            }
            // 如果任务的优先级是最高的，则设置subTask并跳出循环
            if (SubTaskDefine.STATUS_PRIORITY_MAX == Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_PRIORITY])) {
                task.setTaskId(taskId);
                task.setTaskStatusParams(taskStatusParams);
                break;
            }

            // 如果老的优先级低于taskStatusParams的优先级，把优先级高的子任务信息存储到subtask对象中，1为最高，9为最低
            if (task.getStatusParamsPriority() > Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_PRIORITY])) {
                task.setTaskId(taskId);
                task.setTaskStatusParams(taskStatusParams);
            }
        }
        return task;
    }

    /**
     * 透传任务报文解析 qinsong
     *
     * @param pMap
     * @param control
     * @param dataId
     * @return
     */
    public static List<Object> transMessage(Map pMap, String control, String dataId) {
        List<Object> list = new ArrayList<Object>();
        list.add(pMap.get("PORT"));// 终端通信端口号 数据范围1～31
        list.add(pMap.get("BAUDRATE"));// 透明转发通信控制字 Baud（bps）：0～7依次表示
        list.add(pMap.get("STOPBITS"));// 透明转发通信控制字 1/ 2停止位:0/1
        list.add("1");// 透明转发通信控制字 无/有校验:0/1
        list.add(pMap.get("PARITY"));// 透明转发通信控制字 偶/奇校验:0/1
        list.add("3");// 透明转发通信控制字 5-8位数 0～3
        list.add("1");// 透明转发接收等待报文超时时间
        // 按位表示本字节D0～D6编码组成的数据的单位，置“0”：10ms，置“1”：s
        list.add("120");// 透明转发接收等待报文超时时间
        // 编码表示透明转发接收等待报文超时时间的数值，数值范围0～127
        list.add("12000");// 透明转发接收等待字节超时时间
        list.add(pMap.get("ADDR"));// 终端通信地址 测试电表
        list.add(dataId);// 数据项 当前正向有功
        if ("14".equals(control)) {
            Date date = new Date();
            String sysdate = date.getHours() + ":" + date.getSeconds() + ":" + date.getMinutes();
            list.add(sysdate);
        }
        return list;
    }

    /**
     * 通过终端预抄电能表事件，需要的数据
     *
     * @param pMap
     * @param dataIds
     * @return
     */
    private static List<Object> transMessageForWholeEvent(Map<String, String> pMap, List<Object> dataIds) {
        List<Object> list = new ArrayList<Object>();
        list.add(pMap.get("PORT"));// 终端通信端口号 数据范围1～31
        list.add(new ArrayList<Object>());// 没有中继站
        list.add(pMap.get("ADDR"));// 终端通信地址 测试电表
        int protocolId = pMap.get("MPID") == null ? -1 : Integer.valueOf(pMap.get("MPID").toString());// 规约ID
        switch (protocolId) {// 规约数据类型判断 1：2007规约，0:97规约
            case 2:
                list.add(0);
                break;
            case 3:
                list.add(1);
                break;
            case 5:
                list.add(1);
                break;
            case 7:
                list.add(0);
                break;
        }
        list.add(dataIds);
        return list;
    }
}