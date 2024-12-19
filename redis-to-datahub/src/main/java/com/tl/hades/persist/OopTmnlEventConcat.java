package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OopTmnlEventConcat {
    public static final Map<String, String> codeMap = new HashMap<String, String>();
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
    static {//设置businessDataitemId
        //全事件30110200
        codeMap.put("301B0200", "E_METER_EVENT_OPEN_LID");//开表盖事件
        codeMap.put("30130200", "E_METER_EVENT_CLEAR");//电能表清零事件记录记录
        codeMap.put("31060200", "E_EVENT_ERC14");//终端停上电事件
        codeMap.put("31050200", "E_EVENT_ERC12");//时间超差事件
        codeMap.put("30110200", "E_MTER_EVENT_NO_POWER_SOURCE");//电能表停上电事件
        codeMap.put("31120200", "E_EVENT_ERC63_SOURCE");//跨台区电能表事件
        codeMap.put("31110200", "E_EVENT_ERC35_SOURCE");//搜表事件
        codeMap.put("30300200", "E_EVENT_ERC43_SOURCE");//通信模块变更事件单元
        codeMap.put("31200300", "R_TMNL_PARA_240000N_SOURCE");//负荷识别异同数据上报  负荷识别设备启停数据上报
        codeMap.put("erc56", "E_EVENT_ERC56_SOURCE");//电表停上电事件
        codeMap.put("31230200", "E_TMNL_EVENT_LOAD_CHANGE_SOURCE");//用户负荷异动事件
        codeMap.put("31220200", "E_TMNL_EVENT_EQUIP_SWITCH_SOURCE");//用户设备启停事件
        codeMap.put("3E200200", "E_TMNL_EVENT_BREAKER_PROTECT_SOURCE");//断路器保护动作事件
        codeMap.put("3E210200", "E_TMNL_EVENT_BREAKER_GATE_SOURCE");//断路器闸位变化事件
        codeMap.put("3E220200", "E_TMNL_EVENT_BREAKER_PROTECT_RETIRE_SOURCE");//断路器保护功能投退事件
        codeMap.put("3E230200", "E_TMNL_EVENT_BREAKER_WARNING_SOURCE");//断路器告警事件
        codeMap.put("3E2B0200", "E_TMNL_EVENT_STEAL_RECORD_SOURCE");//窃电事件记录单元
        codeMap.put("3E020200", "E_TMNL_EVENT_DOOR_SWITCH_SOURCE");//门开关事件
        codeMap.put("3E030200", "E_TMNL_EVENT_LOCK_SWITCH_SOURCE");//锁开关事件
        codeMap.put("3E400200", "E_TMNL_EVENT_DOUBT_STEAL_SOURCE");//疑似窃电事件单元
        codeMap.put("3E420200", "E_TMNL_EVENT_MAGNETIC_FIELD_SOURCE");//强磁场事件单元
        codeMap.put("32040200", "E_TMNL_EVENT_ALARM_TAH_RECORD_SOURCE");//温湿度告警事件记录单元
        codeMap.put("32060200", "E_TMNL_EVENT_ALARM_SMOKE_SOURCE");//烟雾告警事件记录单元
    }
    private final static Logger logger = LoggerFactory.getLogger(OopTmnlEventConcat.class);
    /**
     * 全事件
     *
     * @param dataitemID
     * @param map
     * @param tmnlId
     * @param terminalArchivesObject
     * @param cj
     * @return
     */
    public static List<Object> getEventDateList(String dataitemID, Map<Object, Object> map, String tmnlId, TerminalArchivesObject terminalArchivesObject, Object... cj) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String codeVal = codeMap.get(dataitemID);
        if (codeVal == null) return null;
        List<Object> dataList = new ArrayList<>();
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
        if (codeVal.equals("E_METER_EVENT_OPEN_LID")) {//开表盖事件
            String meterId = terminalArchivesObject.getMeterId();
            if (meterId == null) {
                return null;
            }
            dataList.add(meterId);
            dataList.add(new Date());
            dataList.add(terminalArchivesObject.getPowerUnitNumber());
            dataList.add(starTime);//事件开始失
            dataList.add(endTime);//事件结束时间
            Object v1 = map.get("00102201");
            Object v2 = map.get("00202201");
            if ((v1 != null && v1.toString().contains("F")) || "null".equals(v1)) {
                v1 = null;
            }
            if ((v2 != null && v2.toString().contains("F")) || "null".equals(v2)) {
                v2 = null;
            }
            dataList.add(v1);//正向有功
            dataList.add(v2);//反向有功
            Object v3 = map.get("00108201");
            Object v4 = map.get("00208201");
            if ((v3 != null && v3.toString().contains("F")) || "null".equals(v3)) {
                v3 = null;
            }
            if ((v4 != null && v4.toString().contains("F")) || "null".equals(v4)) {
                v4 = null;
            }
            for (int i = 0; i < 10; i++) {
                if (i == 4) {
                    dataList.add(v3);//正向有功
                } else if (i == 5) {
                    dataList.add(v4);//反向有功
                } else {
                    dataList.add(null);
                }
            }
            dataList.add(map.get("20220200"));//事件序号
            dataList.add(null);
        } else if (codeVal.equals("E_METER_EVENT_OVER_I_SOURCE")) {//电能表过流
            return null;
        } else if (codeVal.equals("E_METER_EVENT_SWITCH_ERR")) {//电能表负荷开关误动拒动
            return null;
        } else if (codeVal.equals("E_METER_EVENT_CLEAR")) {//电能表清零事件记录记录
            return null;
        } else if (codeVal.equals("E_MTER_EVENT_NO_POWER_SOURCE")) {//电能表掉电事件
            if (DateFilter.isLateWeek(starTime, -180)) {
                return null;
            }
            String flag56 = "0";
            List<Object> resultList = new ArrayList<>();
            String meterId = terminalArchivesObject.getMeterId();
            dataList.add(meterId);
            dataList.add(new Date());
            dataList.add(terminalArchivesObject.getPowerUnitNumber());
            String obj = (String) map.get("20200200");
            if (obj == null || "".equals(obj) || "null".equals(obj)) {
                dataList.add("0");//电能表事件类型 1上电  0掉电
                flag56 = "1";
            } else {
                dataList.add("1");//电能表事件类型
                flag56 = "0";
            }
            dataList.add(starTime);//事件发生时刻
            dataList.add(endTime);//事件结束时刻
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataList.add(DateUtil.format(starTime, DateUtil.defaultDatePattern_YMD));
            dataList.add(dataitemID);
            resultList.add(dataList);//停电事件

            List erc56 = new ArrayList();

            Date evenDate;

            erc56.add(idGenerator.next());
            erc56.add(tmnlId);
            if (flag56 == "0") {//此处flag56已经为正常，跟NO_power已经换位
                evenDate = endTime;
            } else {
                evenDate = starTime;
            }
            erc56.add(evenDate);
            erc56.add(flag56);
            erc56.add(null);
            erc56.add(meterId);
            erc56.add(cj[2]);
            erc56.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            erc56.add(new Date());
            erc56.add(DateUtil.format(evenDate, DateUtil.defaultDatePattern_YMD));
            erc56.add("erc56");
            resultList.add(erc56);
            return resultList;
        } else if (codeVal.equals("E_METER_EVENT_MAG_INTERF")) {//恒定磁场干扰事件
            return null;
        } else if (codeVal.equals("E_METER_EVENT_POWER_ABN")) {//电源异常事件
            return null;
        } else if (codeVal.equals("E_METER_EVENT_OPEN_TERM_LID")) {//开端钮盒事件
            return null;
        } else if (codeVal.equals("E_METER_EVENT_VOL_UNBALANCE")) {//电压不平衡事件
            return null;
        } else if (codeVal.equals("E_EVENT_ERC14")) {//终端停上电事件//TODO
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
            return dataList;
        } else if (codeVal.equals("E_EVENT_ERC12")) {//时钟超差
            String mpedId = terminalArchivesObject.getID();
            Object dnsz;
            Object zdsz;
            List<Object> obgDate = new ArrayList<>();
            if (isNull(map.get("33130206"))) {
                dnsz = map.get("33130206");
                obgDate.add(dnsz);
            }
            if (isNull(map.get("33130207"))) {
                zdsz = map.get("33130207");
                obgDate.add(zdsz);
            }
            StringBuffer status = new StringBuffer();
            Object obj = map.get("33000200");
            if (obj instanceof List) {
                List mapList = (List) obj;
                if (mapList.size() > 0) {
                    for (int b = 0; b < mapList.size(); b++) {
                        String[] lis = mapList.get(b).toString().substring(1, mapList.get(b).toString().length() - 1).split(",");
                        status.append(lis[1]);
                    }
                }
            }

            dataList.add(idGenerator.next());
            dataList.add(tmnlId);
            dataList.add(starTime);
            if (endTime != null) {
                dataList.add("0");//起止标志 恢复
            } else {
                dataList.add("1");//起止标志 停止
            }
            dataList.add(mpedId);//测量点标识
            dataList.add(map.get("20240200").toString());//事件发生源
            dataList.add(status.toString().substring(1, status.toString().length()).replace(' ', '-'));//事件上报状态
            dataList.add(endTime);//事件结束时间
            dataList.add(EventUtils.getDate(obgDate, 0));//电能表时钟
            dataList.add(EventUtils.getDate(obgDate, 1));//终端当前时钟
            dataList.add(map.get("20220200"));//event_index
            dataList.add(cj[2]);//comm_addr1
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataList.add(new Date());
            dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//事件日期
            return dataList;
        } else if (codeVal.equals("E_EVENT_ERC63_SOURCE")) {//跨台区电能表事件
            List dlist = (List) map.get("33040206");
            List fullList;
            for (Object o : dlist) {
                fullList = new ArrayList();
                String flist = o.toString();
                String[] fdataList = flist.split(",");
                Date sdsj = null;
                try {
                    sdsj = DateUtil.parse(fdataList[2].substring(0, fdataList[2].length() - 1), "yyyy-MM-dd HH:mm:ss");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                fullList.add(idGenerator.next());
                fullList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                fullList.add(tmnlId);
                fullList.add(map.get("20220200"));
                fullList.add(starTime);
                fullList.add(endTime);
                fullList.add(fdataList[0].substring(1));//通讯地址
                fullList.add(fdataList[1]);//主节点地址
                fullList.add(sdsj);//变更时间
                fullList.add(null);//相序
                fullList.add(null);
                fullList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                fullList.add(new Date());
                fullList.add(dataitemID);
                dataList.add(fullList);
            }
            return dataList;
        } else if (codeVal.equals("E_EVENT_ERC35_SOURCE")) {//搜表事件
            List allList = new ArrayList();
            List dlist = (List) map.get("33030206");
            for (int a = 0; a < dlist.size(); a++) {

                String flist = dlist.get(a).toString();
                String[] fdataList = flist.split(",");
                if (fdataList.length < 5) {
                    continue;
                }
                Date sdsj = null;
                try {
                    sdsj = DateUtil.parse(fdataList[5], "yyyy-MM-dd HH:mm:ss");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                List aList = new ArrayList();
                aList.add(idGenerator.next());
                aList.add(tmnlId);
                aList.add(starTime);//发生时间
                aList.add(null);//通讯端口号
                aList.add(fdataList[0].substring(1));//未知电能表通信地址
                aList.add(fdataList[4].trim());//未知电表所在相别
                aList.add(fdataList[2].trim());//未知电表通讯协议
                aList.add(map.get("20220200"));//事件记录序号
                aList.add(endTime);//事件结束时间
                aList.add(sdsj);//搜到的时间
                aList.add(fdataList[1]);//采集器地址
                aList.add(fdataList[2]);//电表规约
                aList.add(null);//上报内容
                aList.add(fdataList[3]);//相序
                aList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                aList.add(new Date());
                aList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//事件日期
                aList.add("erc35");
                allList.add(aList);
            }

            return allList;
        } else if (codeVal.equals("E_EVENT_ERC43_SOURCE")) {//通信模块变更事件单元
            dataList.add(idGenerator.next());
            dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//事件日期
            dataList.add(starTime);//发生时间
            dataList.add(tmnlId);
            dataList.add(new Date());//入库时间
            dataList.add(cj[0]);//行政区码
            dataList.add(cj[1]);//终端地址码
            dataList.add(terminalArchivesObject.getPowerUnitNumber());
            dataList.add("0");//上报或召测状态
            dataList.add(null);//变更模块分类标志

            String befor = map.get("33120207").toString();
            String[] bstr = befor.split(";");
            String bmklx = null;
            String bxpcsdm = null;
            String bxpidf = null;
            String bxpid = null;
            String bmkcsdm = null;
            String bmkidf = null;
            String bmkid = null;
            for (int i = 0; i < bstr.length; i++) {
                String obj = bstr[i];
                String val = obj.substring(obj.indexOf("=") + 1, obj.length());
                if (i == 0) {
                    bmklx = val;//变更前模块类型
                }
                if (i == 1) {
                    bxpcsdm = val;//变更前芯片厂商代码
                }
                if (i == 2) {
                    bxpidf = val;//变更前芯片ID格式类型
                }
                if (i == 3) {
                    bxpid = val;//变更前芯片ID
                }
                if (i == 4) {
                    bmkcsdm = val;//变更前模块厂商代码
                }
                if (i == 5) {
                    bmkidf = val;//变更前模块ID格式类型
                }
                if (i == 6) {
                    bmkid = val;//变更前模块ID
                }
            }
            dataList.add(bmklx);//变更前模块类型
            dataList.add(bxpcsdm);//变更前芯片厂商代码
            dataList.add(bxpidf);//变更前芯片ID格式类型
            dataList.add(bmkidf);//变更前模块ID格式类型
            dataList.add(bmkcsdm);//变更前模块厂商代码
            dataList.add(bmkid);//变更前模块ID
            dataList.add(bxpid);//变更前芯片ID
            dataList.add(null);//变更前从节点通信地址


            String after = map.get("33120208").toString();
            String[] astr = after.split(";");
            String amklx = null;
            String axpcsdm = null;
            String axpidf = null;
            String axpid = null;
            String amkcsdm = null;
            String amkidf = null;
            String amkid = null;
            for (int i = 0; i < astr.length; i++) {
                String obj = astr[i];
                String val = obj.substring(obj.indexOf("=") + 1, obj.length());
                if (i == 0) {
                    amklx = val;//变更后模块类型
                }
                if (i == 1) {
                    axpcsdm = val;//变更后芯片厂商代码
                }
                if (i == 2) {
                    axpidf = val;//变更后芯片ID格式类型
                }
                if (i == 3) {
                    axpid = val;//变更后芯片ID
                }
                if (i == 4) {
                    amkcsdm = val;//变更后模块厂商代码
                }
                if (i == 5) {
                    amkidf = val;//变更后模块ID格式类型
                }
                if (i == 6) {
                    amkid = val;//变更后模块ID
                }
            }
            dataList.add(amklx);//AF_UNIT_TYPE:VARCHAR:变更后模块类型1：PLC，2：WIRELESS
            dataList.add(axpcsdm);//AF_HPLC_FACTORY:VARCHAR:变更后芯片厂商代码
            dataList.add(axpidf);//AF_HPLC_IDFORMAT:VARCHAR:变更后芯片ID格式类型
            dataList.add(amkidf);//AF_MIDFORMAT:VARCHAR:变更后模块ID格式类型
            dataList.add(amkcsdm);//AF_FACTORY_CODE:VARCHAR:变更后模块厂商代码
            dataList.add(amkid);//AF_COMB_ID:VARCHAR:变更后模块ID
            dataList.add(axpid);//AF_UNIT_ID:VARCHAR:变更后芯片ID


            Object addr = map.get("33120206");
            dataList.add(addr);//变更后从节点通信地址
            dataList.add("0");//记录来源 0 主动上报
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataList.add(new Date());

            return dataList;
        } else if ("R_TMNL_PARA_240000N_SOURCE".equals(codeVal)) {//巡检仪 负荷识别异同数据上报
            List datas;
            int i;
            if (map.containsKey("48000600")) {
                datas = (List) map.get("48000600");
                i = 1;
                dataitemID = "48000600";
            } else {
                datas = (List) map.get("48000700");
                i = 0;
                dataitemID = "48000700";
            }
            for (Object data : datas
            ) {
                if (i == 0) {
                    List finalList = (List) data;
                    List prefix = new ArrayList();
                    prefix.add(tmnlId);//TERMINAL_ID:BIGINT:终端ID
                    List load = (List) finalList.remove(6);
                    for (int j = 0; j < 6; j++) {
                        if ("0 1 2 5".contains(String.valueOf(j))) {
                            prefix.add(String.valueOf(finalList.get(j)));//DEVICE_NO:VARCHAR:设备名称号
                        } else {
                            prefix.add(finalList.get(j));//DEVICE_NO:VARCHAR:设备名称号
                        }
                    }
                    for (Object lo : load
                    ) {
                        List sufix = new ArrayList();
                        sufix.addAll(prefix);
                        sufix.addAll((Collection) lo);
                        sufix.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                        sufix.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                        sufix.add(dataitemID);
                        dataList.add(sufix);
                    }
                    continue;
                }
                for (Object os : (List) data
                ) {
                    List finalList = (List) os;
                    List fullList = new ArrayList();
                    fullList.add(tmnlId);//TERMINAL_ID:BIGINT:终端ID
                    Date dataDate = (Date) finalList.remove(0);
                    fullList.add(dataDate);//DATA_DATE:DATETIME:数据时间
                    fullList.addAll(finalList);
                    fullList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    fullList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    fullList.add(dataitemID);
                    dataList.add(fullList);
                }
            }

            return dataList;
        }else if("E_TMNL_EVENT_LOAD_CHANGE_SOURCE".equals(codeVal)){//用户设备启停事件
            List<List> sjdyLists= (List<List>) map.get("331B0206");
            if(sjdyLists==null){
                return  null;
            }
            for (List sjdyList:sjdyLists) {

                if(sjdyList==null)continue;
                StringBuffer sb=new StringBuffer();
                List dList=new ArrayList();
                dList.add(idGenerator.next());//EVENT_ID:BIGINT:事件标识
                dList.add(tmnlId);//TERMINAL_ID:BIGINT:终端标识
                dList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
                dList.add(Integer.parseInt(map.get("20220200").toString()));//EVENT_INDEX:INT:事件记录序号
                dList.add(map.get("201E0200"));//EVENT_ST:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
                if(isNull(map.get("20200200"))){
                    dList.add(map.get("20200200").toString());//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
                }else{
                    dList.add(null);//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
                }
                if(isNull(map.get("20240200"))){
                    dList.add(map.get("20240200").toString());//EVENT_SOURCE:VARCHAR:事件发生源
                }else{
                    dList.add(null);//EVENT_SOURCE:VARCHAR:事件发生源
                }
                if(isNull(map.get("33000200"))){
                    dList.add(map.get("33000200").toString());//EVENT_STATUS:VARCHAR:事件上报状态
                }else{
                    dList.add(null);//EVENT_SOURCE:VARCHAR:事件发生源
                }
                dList.add(new Date());//INPUT_TIME:DATETIME:入库时间
                dList.add(Integer.parseInt(sjdyList.get(0).toString()));//FACTORY_CODE:BIGINT:算法厂家编号
                dList.add(Integer.parseInt(sjdyList.get(1).toString()));//SCENES_CODE:BIGINT:算法场景编号
                dList.add(sjdyList.get(2));//DATA_TIME:DATETIME:数据时间
                List fxsjList= (List) sjdyList.get(3);
                for (int m=0;m<fxsjList.size();m++) {
                    sb.append(fxsjList.get(m)).append(",");
                }
                dList.add(sb.toString().substring(0,sb.toString().length()-1));//ANALYZED_DATA:VARCHAR:负荷辨识分析数据
                dList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位
                dList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                dList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                dList.add(dataitemID);
                dataList.add(dList);
            }
            return dataList;
        }else if("E_TMNL_EVENT_EQUIP_SWITCH_SOURCE".equals(codeVal)){//用户负荷异动事件
            dataList.add(idGenerator.next());//EVENT_ID:BIGINT:事件标识
            dataList.add(tmnlId);//TERMINAL_ID:BIGINT:终端标识
            dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
            dataList.add(Integer.parseInt(map.get("20220200").toString()));//EVENT_INDEX:INT:事件记录序号
            dataList.add(map.get("201E0200"));//EVENT_ST:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
            if(isNull(map.get("20200200"))){
                dataList.add(map.get("20200200").toString());//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
            }else{
                dataList.add(null);//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
            }
            if(isNull(map.get("20240200"))){
                dataList.add(map.get("20240200").toString());//EVENT_SOURCE:VARCHAR:事件发生源
            }else{
                dataList.add(null);//EVENT_SOURCE:VARCHAR:事件发生源
            }
            if(isNull(map.get("33000200"))){
                dataList.add(map.get("33000200").toString());//EVENT_STATUS:VARCHAR:事件上报状态
            }else{
                dataList.add(null);//EVENT_SOURCE:VARCHAR:事件发生源
            }
            dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
            if(isNull(map.get("331D0206"))){
                dataList.add(map.get("331D0206").toString());//EQUIP_NAME:VARCHAR:设备名称号
            }else{
                dataList.add(null);//EQUIP_NAME:VARCHAR:设备名称号
            }
            if(isNull(map.get("331D0207"))){
                dataList.add(map.get("331D0207").toString());//EQUIP_NUMBER:VARCHAR:设备类号
            }else{
                dataList.add(null);//EQUIP_NUMBER:VARCHAR:设备类号
            }
            dataList.add(map.get("331D0208").toString());//EQUIP_ATTRIBUTES:VARCHAR:设备属性：阻性负载	（0），容性负载（1），感性负载（2），混合（3），其他（4）
            dataList.add(map.get("331D0209").toString());//EQUIP_STATUS:VARCHAR:设备状态：停机（0），启动（1），负载上升（2），负载下降（3），负载波动	（4），其他（5）
            dataList.add(Integer.parseInt(map.get("331D020A").toString()));//FACTORY_CODE:BIGINT:算法厂家编号
            dataList.add(Integer.parseInt(map.get("331D020B").toString()));//SCENES_CODE:BIGINT:算法场景编号
            List fhbsfxsjList= (List)map.get("331D020C");
            StringBuffer sb=new StringBuffer();
            for (int m=0;m<fhbsfxsjList.size();m++) {
                sb.append(fhbsfxsjList.get(m)).append(",");
            }
            dataList.add(sb.toString().substring(0,sb.toString().length()-1));//ANALYZED_DATA:VARCHAR:负荷辨识分析数据
            dataList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            return dataList;
        }else if("E_TMNL_EVENT_STEAL_RECORD_SOURCE".equals(codeVal)){//窃电事件记录单元
            try {
                List meterList= (List) map.get("3EAB020C");
                for (Object meterObj:meterList) {
                    String [] m= meterObj.toString().replace("[","").replace("]","").trim().split(",");
                    terminalArchivesObject = CommonUtils.getTerminalArchives(cj[0].toString(), cj[1].toString(), m[0]);
                    List mList=new ArrayList();
                    mList.add(idGenerator.next());//EVENT_ID:BIGINT:事件标识
                    mList.add(terminalArchivesObject.getMeterId());//METER_ID:BIGINT:电能表ID
                    mList.add(tmnlId);//TERMINAL_ID:BIGINT:终端标识
                    mList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
                    mList.add(starTime);//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
                    mList.add(endTime);//EVENT_ST:DATETIME:事件结束时间，yyyy-mm-ddhh24:mi:ss
                    mList.add(new Date());//INPUT_TIME:DATETIME:入库时间，yyyy-mm-ddhh24：mi：ss
                    List sbList= (List) map.get("33000200");
                    String sbzt= sbList.get(0).toString().substring(1,sbList.get(0).toString().indexOf(","));
                    mList.add(sbzt);//EVENT_STATUS:VARCHAR:事件上报状态
                    mList.add(map.get("202A0200").toString());//EVENT_SOURCE:VARCHAR:事件发生源

                    mList.add(map.get("3EAB0206").toString());//STEAL_STATUS:VARCHAR:窃电状态：00：发生；01：恢复；
                    mList.add(map.get("3EAB0207").toString());//STEAL_REASON:VARCHAR:窃电原因：00：表箱窃电；01：电表窃电；02：表计自身计量超差；03：其他未知窃电原因；其他值：保留
                    mList.add(Double.parseDouble(map.get("3EAB0208").toString()));//INCOME_CURRENT:DECIMAL:窃电发生时进线侧电流
                    mList.add(Double.parseDouble(map.get("3EAB0209").toString()));//USER_CURRENT:DECIMAL:窃电发生时用户侧电流
                    mList.add(Double.parseDouble(map.get("3EAB020A").toString()));//INCOME_POWER:DECIMAL:窃电发生时进线侧功率
                    mList.add(Double.parseDouble(map.get("3EAB020B").toString()));//USER_POWER:DECIMAL:窃电发生时用户侧功率
                    mList.add(m[0]);//COMM_ADDR:VARCHAR:电表地址
                    if(m[1].trim().equals("null")){
                        mList.add(null);//METER_CLOCK:DATETIME:电表时钟
                    }else{
                        mList.add(sdf.parse(m[1]));//METER_CLOCK:DATETIME:电表时钟
                    }
                    //VOLTAGE_A:DECIMAL:A相电压
                    //VOLTAGE_B:DECIMAL:B相电压
                    //VOLTAGE_C:DECIMAL:C相电压
                    //CURRENT_A:DECIMAL:A相电流
                    //CURRENT_B:DECIMAL:B相电流
                    //CURRENT_C:DECIMAL:C相电流
                    //CURRENT_ZERO:DECIMAL:零线电流
                    //ACTIVE_POWER:DECIMAL:总有功功率
                    for(int indexx=2;indexx<m.length-2;indexx++){
                        if(m[indexx].trim().equals("null")){
                            mList.add(null);
                        }else{
                            mList.add(Double.parseDouble(m[indexx].trim()));
                        }
                    }
                    mList.add(Integer.parseInt(m[10].trim()));//OPEN_TIMES:INT:开盖次数
                    mList.add(m[11].trim());//ATTRIBUTE:VARCHAR:属性标志：00：电表未窃电；01：电表窃电；02：计量超差；
                    mList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位
                    mList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    mList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    mList.add(dataitemID);
                    dataList.add(mList);
                }
            }catch (Exception e){
                return null;
            }
            return dataList;
        }else if((dataitemID.startsWith("320")||dataitemID.startsWith("3E"))&&!"E_TMNL_EVENT_STEAL_RECORD_SOURCE".equals(codeVal)){//断路器
            dataList.add(idGenerator.next());//EVENT_ID:BIGINT:事件标识
            dataList.add(tmnlId);//TERMINAL_ID:BIGINT:终端标识
            dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
            dataList.add( starTime);//EVENT_ST:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
            dataList.add(endTime);//EVENT_ET:DATETIME:事件发生时间，yyyy-mm-ddhh24:mi:ss
            if(dataitemID.startsWith("3E2")){
                dataList.add(map.get("202A0200").toString());//EVENT_SOURCE:VARCHAR:事件发生源
                List sbList= (List) map.get("33000200");
                String sbzt= sbList.get(0).toString().substring(1,sbList.get(0).toString().indexOf(","));
                dataList.add(sbzt);//EVENT_STATUS:VARCHAR:事件上报状态
                dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
                if("E_TMNL_EVENT_BREAKER_PROTECT_SOURCE".equals(codeVal)){//断路器保护动作事件
                    try{
                        dataList.add(map.get("3EA00206").toString());//ERROR_REASON:VARCHAR:故障原因：剩余电流预警（1），剩余电流告警（2），缺零（4），过载（5），短路短延时（6），缺相（7），欠压（8），过压（9），接地（10），互感器故障（16），短路瞬时（22），合闸失败（17），跳闸失败（23）
                        List gzxbList= (List) map.get("3EA00207");
                        int gzxbIndex=gzxbList.get(0).toString().indexOf("1");
                        dataList.add(String.valueOf(gzxbIndex+1));//ERROR_PHASE:VARCHAR:故障相别
                        dataList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位
                        dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                        dataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                    return dataList;
                }else if("E_TMNL_EVENT_BREAKER_GATE_SOURCE".equals(codeVal)){//断路器闸位变化事件
                    try{
                        dataList.add(map.get("3EA10206").toString());//GATE_STATUS:VARCHAR:闸位变化状态：分合（1），合分（2）
                        dataList.add(map.get("3EA10207").toString());//GATE_REASON:VARCHAR:闸位变化原因：剩余电流（2），缺零（4），过载（5），短路短延时（6），缺相（7），欠压（8），过压（9），接地（10），远程试验（13），按键试验（14），闭锁（15），手动（18），短路瞬时（22），重合闸（24），硬遥控（29），软遥控（30）
                        List gzxbList= (List) map.get("3EA10208");
                        int gzxbIndex=gzxbList.get(0).toString().indexOf("1");
                        dataList.add(String.valueOf(gzxbIndex+1));//ERROR_PHASE:VARCHAR:故障相别
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                    return dataList;
                }else if("E_TMNL_EVENT_BREAKER_PROTECT_RETIRE_SOURCE".equals(codeVal)){//断路器闸位变化事件
                    try{
                        dataList.add(map.get("3EA20206").toString());//ACTION:VARCHAR:动作：剩余电流保护（1），短路瞬时保护（2），短路短延时保护（3），过载保护（4），过压保护（5），欠压保护（6），缺相保护（7），缺零保护（8）
                        List syctr= (List) map.get("3EA20207");
                        int syctyIndex=syctr.get(0).toString().indexOf("1");
                        dataList.add(String.valueOf(syctyIndex+1));//LAST_VALUE:VARCHAR:上一次投入使用的定值
                        List dqtr= (List) map.get("3EA20208");
                        int dqtyIndex=dqtr.get(0).toString().indexOf("1");
                        dataList.add(String.valueOf(dqtyIndex+1));//PRESENT_VALUE:VARCHAR:当前要投入使用的定值
                        dataList.add(map.get("3EA20209").toString());//RETIRE_TYPE:VARCHAR:投退方式：就地（0），远方（1）
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                }else if("E_TMNL_EVENT_BREAKER_WARNING_SOURCE".equals(codeVal)){//断路器告警事件
                    try{
                        dataList.add(map.get("3EA30206").toString());//WARN_STATUS:VARCHAR:告警状态：告警恢复（0），告警发生（1）
                        dataList.add(map.get("3EA30207").toString());//WARN_REASON:VARCHAR:告警原因：剩余电流预警（1），剩余电流告警（2），缺零（4），过载（5），短路短延时（6），缺相（7），欠压（8），过压（9），接地（10），互感器故障（16），短路瞬时（22），合闸失败（17），跳闸失败（23）
                        List gzxbList= (List) map.get("3EA30208");
                        int gzxbIndex=gzxbList.get(0).toString().indexOf("1");
                        dataList.add(String.valueOf(gzxbIndex+1));//ERROR_PHASE:VARCHAR:故障相别
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                }
            }else  if(dataitemID.startsWith("3E0")){
                //无需特殊处理事件
                //E_TMNL_EVENT_DOOR_SWITCH_SOURCE 门开关事件
                //E_TMNL_EVENT_LOCK_SWITCH_SOURCE 锁开关事件
                dataList.add(map.get("202A0200").toString());//EVENT_SOURCE:VARCHAR:事件发生源
                dataList.add(map.get("20220200").toString());//EVENT_STATUS:VARCHAR:事件上报状态
                dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
            }else if(dataitemID.startsWith("3E4")){
                if("E_TMNL_EVENT_DOUBT_STEAL_SOURCE".equals(codeVal)){//疑似窃电事件单元
                    try{
                        dataList.add(map.get("3E700204").toString());//EVENT_SOURCE:VARCHAR:事件发生源：门状态（0），锁状态（1），摄像头（2），门和摄像头（3），锁和摄像头（4），门和锁及摄像头（5）
                        dataList.add(map.get("20220200").toString());//EVENT_STATUS:VARCHAR:事件上报状态
                        dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
                        dataList.add(map.get("3E700206").toString());//CIRCUIT:VARCHAR:发生回路
                        dataList.add("");//GATE_REASON:VARCHAR:闸位变化原因
                        dataList.add("");//ERROR_PHASE:VARCHAR:故障相别
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                }else if("E_TMNL_EVENT_MAGNETIC_FIELD_SOURCE".equals(codeVal)){//强磁场事件单元
                    try {
                        dataList.add(map.get("202A0200").toString());//EVENT_SOURCE:VARCHAR:事件发生源
                        dataList.add(map.get("20220200").toString());//EVENT_STATUS:VARCHAR:事件上报状态
                        dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
                        List<String> ccqdList= (List) map.get("3E720206");
                        //MAGNETIC_X:VARCHAR:X方向磁场强度
                        //MAGNETIC_Y:VARCHAR:Y方向磁场强度
                        //MAGNETIC_Z:VARCHAR:Z方向磁场强度
                        if(ccqdList==null || ccqdList.size()<1){
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                        }else {
                            for (String qd:ccqdList){
                                dataList.add(qd);
                            }
                        }
                    }catch (Exception e){
                        logger.error("",e);
                        return null;
                    }
                }
            }else if(dataitemID.startsWith("320")){
                try {
                    dataList.add(null);//EVENT_SOURCE:VARCHAR:事件发生源
                    dataList.add(null);//EVENT_STATUS:VARCHAR:事件上报状态
                    dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间
                    if("E_TMNL_EVENT_ALARM_SMOKE_SOURCE".equals(codeVal)){//烟雾告警事件记录单元
                        List<String> strList= (List) map.get("35030206");
                        //ASSET_NO:VARCHAR:资产编号
                        //BEF_SMOKE_STATUS:VARCHAR:告警前烟雾状态
                        //SMOKE_STATUS:VARCHAR:告警时烟雾状态
                        if(strList==null || strList.size()<1){
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                        }else {
                            for (String str:strList){
                                dataList.add(str);
                            }
                        }
                    }else if ("E_TMNL_EVENT_ALARM_TAH_RECORD_SOURCE".equals(codeVal)){//温湿度告警事件记录单元
                        List<String> strList= (List) map.get("35010206");
                        //ASSET_NO:VARCHAR:资产编号
                        //TEMPERATURE:DECIMAL:告警时温度
                        //HUMIDITY:DECIMAL:告警时湿度
                        //TEMP_STATUS:VARCHAR:告警时温度传感器状态
                        //HUMIDITY_STATUS:VARCHAR:告警时湿度传感器状态
                        //BEF_TEMPERATURE:DECIMAL:告警前温度
                        //BEF_HUMIDITY:DECIMAL:告警前湿度
                        //BEF_TEMP_STATUS:VARCHAR:告警前温度传感器状态
                        //BEF_HUMIDITY_STATUS:VARCHAR:告警前湿度传感器状态
                        if(strList==null || strList.size()<1){
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                        }else {
                            for (int fk=0;fk<strList.size();fk++){
                                if(fk == 1 || fk == 2 || fk == 5 || fk == 6){
                                    dataList.add(Double.parseDouble(strList.get(fk)));
                                }else {
                                    dataList.add(strList.get(fk));
                                }
                            }
                        }
                    }else{
                        return null;
                    }

                }catch (Exception e){
                    return null;
                }
            }
            dataList.add(terminalArchivesObject.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位
            dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            return dataList;
        }
        dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
        return dataList;
    }

    /**
     * 判断对象不为全空
     *
     * @param dataObject
     * @return
     */
    public static boolean isAllNull(List<com.tl.hades.objpro.api.beans.DataObject> dataObject) {
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


    /**
     * 根据传入的String解取数据
     *
     * @param obj
     * @return
     */
    private static List<String> getDateByString(Object obj) {
        String bf;
        List<String> dataList = new ArrayList<>();
        List<String> finalList = new ArrayList<>();
        if (obj != null && !"".equals(obj)) {
            bf = (String) obj;
            if (bf.contains(";")) {
                for (String st : bf.split(";")) {
                    dataList.add(st);
                }
            }
            if (!dataList.isEmpty() && dataList.size() != 0) {
                for (String st : dataList) {
                    if (bf.contains("=")) {
                        String[] str = st.split("=");
                        if (str.length > 1) {
                            finalList.add(st.split("=")[1]);
                        }
                    }
                }
            }
        }
        return finalList;
    }

}
