package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.MeterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OopCurrentConcat {
    private final static Logger logger = LoggerFactory.getLogger(OopCurrentConcat.class);
    private final static String redisName = "Q_BASIC_DATA_50040200_R";

    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
    private static List<String> itemList = new ArrayList<>();

    private static List<String> eventList = new ArrayList<>();

    private static Map<String, Integer> eedcAll = new HashMap<String, Integer>();
    private static Map<String, String> eedc = new HashMap<String, String>();

    private static List<String> realList = new ArrayList<>();//预抄 power vol cur factor
    private static Map<String, String> adjustMap = new HashMap<String, String>();

    /**
     * hplc新增
     */
    static {
        itemList.add("F2090700");
        itemList.add("F2090200");
        itemList.add("45000500");
        itemList.add("F2090500");
        itemList.add("FF100200");
        itemList.add("45000700");
        itemList.add("43000300");//终端版本信息
        itemList.add("F1000200");//ESAM信息
        itemList.add("45000200");//通信配置698
        itemList.add("60320200");
        itemList.add("60150200");
        itemList.add("31060800");
        itemList.add("31060900");
        itemList.add("F2090800");
        itemList.add("F2091400");
        itemList.add("20120200");
        itemList.add("F2090A00");//节点相位信息单元
        itemList.add("F2090C00");//节点版本信息
        itemList.add("48000800");//设备状态数据
        itemList.add("29000200");//能源控制器（当前温度）
        itemList.add("29010200");//能源控制器（当前湿度）
        itemList.add("28000200");//能源控制器（当前剩余电流最大相及剩余电流值）
        itemList.add("28070200");//能源控制器（断路器闸位状态）
        itemList.add("29090200");//能源控制器（电容器数据）
        itemList.add("290A0200");//能源控制器（电容器投切状态）

        eventList.add("03110001");
        eventList.add("03110002");
        eventList.add("03110003");
        eventList.add("03110004");
        eventList.add("03110005");
        eventList.add("03110006");
        eventList.add("03110007");
        eventList.add("03110008");
        eventList.add("03110009");
        eventList.add("0311000A");

        //三相e_edc一起返回
        eedcAll.put("70201", 0);//电压
        eedcAll.put("70301", 0);//电流
        eedcAll.put("70501", 0);//相角
        eedcAll.put("70505", 0);//相角（698）
        eedcAll.put("71401", -1);//功率因数
        eedcAll.put("70401", 0);//瞬时总有功功率645 (瞬时有功功率698)
        eedcAll.put("70402", 4);//瞬时总无功功率645 (瞬时无功功率698)

        //时区、时段
        adjustMap.put("04010000", "01");//时区 第一套
        adjustMap.put("04020000", "01");//时区
        adjustMap.put("04010001", "02");//时段第一套
        adjustMap.put("04020001", "02");//时段

        //单独e_edc数据返回
        eedc.put("70202", "1");
        eedc.put("70203", "2");
        eedc.put("70204", "3");
        eedc.put("70302", "1");
        eedc.put("70303", "2");
        eedc.put("70304", "3");

        eedc.put("70502", "1");
        eedc.put("70503", "2");
        eedc.put("70504", "3");

        eedc.put("71402", "1");
        eedc.put("71403", "2");
        eedc.put("71404", "3");

        eedc.put("70405", "1");
        eedc.put("70406", "2");
        eedc.put("70407", "3");
        eedc.put("70408", "4");
        eedc.put("70409", "5");
        eedc.put("70410", "6");
        eedc.put("70411", "7");
        eedc.put("70412", "8");


        eedc.put("70506", "1");// A相电压电流相角（698）

        realList.add("1802001");//645 vol
        realList.add("1803001");//645 cur
        realList.add("1805001");//645 factor
        realList.add("1804001");//645 power you
        realList.add("1804002");//645 power wu


    }


    public static void getDataList(MeterData meterData, int protocolId, String areaCode, String termAddr, String oopDataItemId, List<DataObject> listDataObj, int src, int... index) throws Exception {
        List dataList = null;
        String meterAddr = null;
        String businessDataitemId = null;
        Date dates = null;//存储时标
        Date collDate = null;//采集成功时标

        List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
        if (doList != null && doList.size() > 0) {
            for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                if ("60420200".equals(dataObject.getDataItem())) {//抄表时间
                    dates = (Date) dataObject.getData();
                    //抄表时间
                } else if ("202A0200".equals(dataObject.getDataItem())) {
                    //表地址
                    meterAddr = String.valueOf(dataObject.getData());
                } else if ("60410200".equals(dataObject.getDataItem())) {
                    collDate = (Date) dataObject.getData();//采集成功时标
                } else {
                    if ("30110200".equals(oopDataItemId)) {
                        if (dataList == null) {
                            dataList = new ArrayList();
                        }
                        if (dataObject.getData() != null) {
                            dataList.add(String.valueOf(dataObject.getData()));
                        }
                    } else {
                        oopDataItemId = dataObject.getDataItem();//数据项
                        if (dataObject.getData() != null) {
                            if (dataObject.getData() instanceof List) {
                                dataList = (List) dataObject.getData();
                            } else if ("202D0200".equals(oopDataItemId)) {
                                dataList = new ArrayList();
                                //透支金额
                                dataList.add(String.valueOf(dataObject.getData()));
                                com.tl.hades.objpro.api.beans.DataObject dataObject1 = doList.get(5);
                                List data = (List) dataObject1.getData();
                                //剩余金额
                                dataList.addAll(data);
                                break;
                            } else {
                                dataList = new ArrayList();
                                dataList.add(String.valueOf(dataObject.getData()));
                            }
                        }
                    }
                }
            }//20040201   0400010C
            if (dates == null) {
                dates = new Date();
            }
        }
        if (!itemList.contains(oopDataItemId)) {
            if (meterAddr == null) {
                meterAddr = meterData.getMeterAddr();
            }
            if (oopDataItemId == null || meterAddr == null || dataList == null || dates == null) {
                return;
            }
        }


        //当前areaCode, termAddr, meterAddr并非真实数据，需要查询面向对象缓存后再查询档案缓存
        TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);

        String orgNo = terminalArchivesObject.getPowerUnitNumber();
        if (orgNo == null || "".equals(orgNo)) {
            logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + " COMMADDR" + meterAddr);
            return;
        }
        String tmnlId = null;
        String mpedIdStr = null;
        String meterId = null;
        int mptl = 0;
        if (itemList.contains(oopDataItemId)) {
            tmnlId = terminalArchivesObject.getTerminalId();
            if (null == tmnlId || "".equals(tmnlId)) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + " COMMADDR" + meterAddr);
                return;
            }
        } else {
            mpedIdStr = terminalArchivesObject.getID();
            meterId = terminalArchivesObject.getMeterId();
            if (null == mpedIdStr || "".equals(mpedIdStr)) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + " COMMADDR" + meterAddr);
                return;
            }
            mptl = ProtocolArchives.getInstance().getMeterPtl(mpedIdStr);
            if (mptl == 3 && protocolId == 8) {
                if (oopDataItemId.startsWith("00")) {
                    if (dataList.size() == 6) {
                        dataList.remove(dataList.size() - 1);
                    }
                } else {
                    dataList.remove(dataList.size() - 1);
                }

            }
        }
        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
        businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
        if (businessDataitemId == null) {
            logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + oopDataItemId);
            return;
        }
        String indexKey = mpedIdStr;
        List<Object> dataListFinal = new ArrayList<>();

        Object[] refreshKey = null;
        if (src == 0) {
            refreshKey = new Object[5];
            refreshKey[0] = terminalArchivesObject.getTerminalId();
            refreshKey[1] = mpedIdStr;
            refreshKey[2] = "00000000000000"; // 数据召测时间
            refreshKey[3] = businessDataitemId;
            refreshKey[4] = protocolId;
        }

        if (oopDataItemId.startsWith("10")) {//698
            dataListFinal.add(mpedIdStr);
            if (oopDataItemId.equals("10100200")) {//正向有功需量
                dataListFinal.add("1");//flag
            } else if (oopDataItemId.equals("10200200")) {//反向有功需量
                dataListFinal.add("5");//flag
            } else if (oopDataItemId.equals("10300200")) {//组合无功1
                dataListFinal.add("2");//flag
            } else if (oopDataItemId.equals("10400200")) {//组合无功2
                dataListFinal.add("6");//flag
            }
            dataListFinal.add(dates);//抄表时间
            if (dataList == null) return;

            if(ParamConstants.startWith.equals("41")){//河南
                //去掉需量时间判断
                Object demand_v = ((List) dataList.get(0)).get(0);
                if (demand_v == null) return;
                dataListFinal.add(demand_v);
                Object demand_time = null;//demandtime
                try {
                    demand_time = ((List) dataList.get(0)).get(1);
                } catch (Exception e) {
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                Date d = sdf.parse(demand_time.toString());
                dataListFinal.add(d);
            } else if(ParamConstants.startWith.equals("11")) {  //北京
                for (int v = 0; v < dataList.size(); v = v + 1) {
                    //去掉需量时间判断
                    Object demand_v = dataList.get(v);
                    if (demand_v == null && v == 0) return;
                    dataListFinal.add(demand_v);
                    try {
                        Object demand_time = dataList.get(v + 1);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        Date d = sdf.parse(demand_time.toString());
                        dataListFinal.add(d);
                    } catch (Exception e) {
                        dataListFinal.add(null);
                    }
                }
            }

            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
            dataListFinal.add("00");//未分析
            if (src == 0) {
                if (protocolId == 8) {
                    dataListFinal.add("30");
                } else {
                    dataListFinal.add("10");
                }
            } else {
                if (protocolId == 8) {
                    dataListFinal.add("34");
                } else {
                    dataListFinal.add("14");
                }
            }

            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(DateUtil.format(OopDataUtils.timeAddOrMinus(dates, -1), DateUtil.defaultDatePattern_YMD));

        } else if (oopDataItemId.startsWith("010")) {//645
            dataListFinal.add(mpedIdStr);
            if (oopDataItemId.equals("01010000")) {//正向有功需量
                dataListFinal.add("1");//flag
            } else if (oopDataItemId.equals("01020000")) {//反向有功需量
                dataListFinal.add("5");//flag
            } else if (oopDataItemId.equals("01030000")) {//组合无功1
                dataListFinal.add("2");//flag
            } else if (oopDataItemId.equals("01040000")) {//组合无功2
                dataListFinal.add("6");//flag
            }
            dataListFinal.add(dates);//抄表时间
            //去掉需量时间判断
            Object demand_v = dataList.get(0);
            if (demand_v == null) return;
            dataListFinal.add(demand_v);
            Object demand_time = null;//demandtime
            Date d = null;
            if(ParamConstants.startWith.equals("41")) {//河南
                try {
                    demand_time = dataList.get(1);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String ftime = demand_time.toString();
                    d = sdf.parse(ftime.replaceFirst(ftime.substring(0, ftime.indexOf("-")), ftime.substring(0, ftime.indexOf("-")).trim()) + ":00");
                } catch (Exception e) {
                    return;
                }
                dataListFinal.add(d);
            } else if(ParamConstants.startWith.equals("11")){//北京
                try {
                    demand_time = dataList.get(1);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String ftime = demand_time.toString();
                    d = sdf.parse(ftime.replaceFirst(ftime.substring(0, ftime.indexOf("-")), ftime.substring(0, ftime.indexOf("-")).trim()) + ":00");
                } catch (Exception e) {
                    logger.error("需量日期为空");
                }
                dataListFinal.add(d);
                //增加费率需量及发生时间
                for (int n = 0; n < 8; n++) {
                    dataListFinal.add(null);
                }
            }
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
            dataListFinal.add("00");//未分析
            if (src == 0) {
                if (protocolId == 8) {
                    dataListFinal.add("30");
                } else {
                    dataListFinal.add("10");
                }
            } else {
                if (protocolId == 8) {
                    dataListFinal.add("34");
                } else {
                    dataListFinal.add("14");
                }
            }

            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(DateUtil.format(OopDataUtils.timeAddOrMinus(dates, -1), DateUtil.defaultDatePattern_YMD));

        } else if (realList.contains(businessDataitemId)) {
            dataListFinal.add(mpedIdStr);//ID:BIGINT:测量点标识（MPED_ID）
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
            dataListFinal.add(dates);//COL_TIME:DATETIME:终端抄表日期时间
            dataListFinal.add(orgNo);//ORG_NO:VARCHAR:供电单位编号
            for (int r = 0; r < dataList.size(); r++) {
                dataListFinal.add(dataList.get(r));
            }
            if (businessDataitemId.equals("1803001")) {
                dataListFinal.add(null);
            }
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
        } else if (eventList.contains(oopDataItemId) && src == 0) {//电表停上电事件召测--645
            if (meterId == null) {
                return;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = null;
            Date endDate = null;
            try {
                startDate = sdf.parse(String.valueOf(dataList.get(0)));
                endDate = sdf.parse(String.valueOf(dataList.get(1)));
            } catch (ParseException e) {
                return;
            }
            dataListFinal.add(meterId);
            dataListFinal.add(new Date());
            dataListFinal.add(orgNo);
            dataListFinal.add("0");//停上电类型
            dataListFinal.add(startDate);
            dataListFinal.add(endDate);
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(DateUtil.format(endDate, DateUtil.defaultDatePattern_YMD));
            indexKey = meterId;
        } else if (adjustMap.containsKey(oopDataItemId) && src == 0) {
            if (meterId == null) {
                return;
            }
            dataListFinal.add(new BigDecimal(Long.parseLong(meterId)));
            dataListFinal.add(mpedIdStr);
            dataListFinal.add(adjustMap.get(oopDataItemId));
            List dlist = (List) dataList.get(0);
            String contents = null;
            StringBuffer sb = new StringBuffer();
            for (int a = 0; a < dlist.size(); a = a + 2) {
                sb.append(dlist.get(a)).append(",");
            }
            contents = sb.substring(0, sb.length() - 1);
            dataListFinal.add(contents);
            dataListFinal.add(new Date());
            dataListFinal.add(orgNo.substring(0, 5));
            indexKey = meterId;
        } else if ("30110200".contains(oopDataItemId) && src == 0) {//电表停上电事件召测--698
            if (meterId == null) {
                return;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = null;
            Date endDate = null;
            try {
                startDate = sdf.parse(String.valueOf(dataList.get(0)));
                endDate = sdf.parse(String.valueOf(dataList.get(1)));
            } catch (ParseException e) {
                return;
            }
            dataListFinal.add(meterId);
            dataListFinal.add(new Date());
            dataListFinal.add(orgNo);
            dataListFinal.add("0");//停上电类型
            dataListFinal.add(startDate);
            dataListFinal.add(endDate);
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(DateUtil.format(endDate, DateUtil.defaultDatePattern_YMD));
            indexKey = meterId;
        } else if ("60150200".equals(oopDataItemId)) {//重点用户参数
            List list1 = (List) dataList.get(4);
            if (list1 == null) {
                return;
            }
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
            for (int i = 0; i < list1.size(); i++) {
                meterAddr = ((String) list1.get(i)).substring(2);
                //获取前置写入redis中的真实表地址
                //获取档案信息
                terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                mpedIdStr = terminalArchivesObject.getID();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + " COMMADDR" + meterAddr);
                    continue;
                }
                List alllList = new ArrayList();
                alllList.add(mpedIdStr);
                alllList.add(terminalArchivesObject.getTerminalId());
                alllList.add(meterAddr);
                alllList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                alllList.add(new Date());
                CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, alllList, refreshKey, listDataObj);
            }
        } else if (eedcAll.containsKey(businessDataitemId)) { //a、b、c三相 一起返回
            for (int i = 0; i < dataList.size(); i++) {
                List alllList = new ArrayList();
                alllList.add(mpedIdStr);
                alllList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                alllList.add(eedcAll.get(businessDataitemId) + 1 + i);
                alllList.add(orgNo);//供电单位
                alllList.add(dataList.get(i));
                alllList.add(dates);//电压时间点
                alllList.add("00");//未分析
                if (src == 0) {
                    if (protocolId == 8) {
                        alllList.add("30");
                    } else {
                        alllList.add("10");
                    }
                } else {
                    if (protocolId == 8) {
                        alllList.add("34");
                    } else {
                        alllList.add("14");
                    }
                }
                alllList.add(new Date());//入库时间
                alllList.add(orgNo.substring(0, 5));
                CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, alllList, refreshKey, listDataObj);
            }
        } else if (eedc.containsKey(businessDataitemId)) {//a、b、c三相 单独返回
            dataListFinal.add(mpedIdStr);
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            dataListFinal.add(eedc.get(businessDataitemId));
            dataListFinal.add(orgNo);//供电单位
            dataListFinal.add(dataList.get(0));
            dataListFinal.add(dates);//电压时间点
            dataListFinal.add("00");//未分析
            if (src == 0) {
                if (protocolId == 8) {
                    dataListFinal.add("30");
                } else {
                    dataListFinal.add("10");
                }
            } else {
                if (protocolId == 8) {
                    dataListFinal.add("34");
                } else {
                    dataListFinal.add("14");
                }
            }
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(orgNo.substring(0, 5));
            indexKey = mpedIdStr;
        } else if ("70001".equals(businessDataitemId)) {//状态字1入库
            dataListFinal.add(mpedIdStr);
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            dataListFinal.add(orgNo);//供电单位

            String statusStr = "";
            if (mptl == 8) {//698
                List l = (List) dataList.get(0);
                statusStr = l.get(0).toString();
            } else {//645
                statusStr = dataList.get(0).toString();
            }


            List list = new ArrayList();
            list.add(statusStr.charAt(15));//0外置开关控制
            list.add(statusStr.charAt(14));//1需量积算方式
            list.add(statusStr.charAt(13));//2时钟电池 (0正常，1欠压),
            list.add(statusStr.charAt(12));//3停电抄表电池 (0正常，1欠压),
            list.add(statusStr.charAt(11));//4有功功率方向 (0正向、1反向),
            list.add(statusStr.charAt(10));// 5:无功功率方向 (0正向、1反向),
            list.add(0);//6:保留,
            list.add(0);//7:保留
            list.add(statusStr.charAt(7));//8:控制回路错误
            list.add(statusStr.charAt(6));//9:ESAM错误
            list.add(0);//10:保留
            list.add(0);//11:保留
            list.add(statusStr.charAt(3));//12:内部程序错误
            list.add(statusStr.charAt(2));// 13:存储器故障或损坏
            list.add(statusStr.charAt(1));//14:透支状态
            list.add(statusStr.charAt(0));//15:时钟故障

            StringBuffer sb = new StringBuffer();
            for (int s = 0; s < list.size(); s++) {
                sb.append(list.get(s));
            }

            dataListFinal.add(sb.toString());
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());//入库时间
        } else if ("70003".equals(businessDataitemId)) {//状态字3入库
            dataListFinal.add(mpedIdStr);
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            dataListFinal.add(orgNo);//供电单位

            String statusStr = "";
            if (mptl == 8) {//698
                List l = (List) dataList.get(0);
                statusStr = l.get(0).toString();
            } else {//645
                statusStr = dataList.get(0).toString();
            }

            List list = new ArrayList();
            list.add(statusStr.charAt(15));
            list.add(statusStr.charAt(14) + "" + statusStr.charAt(13) + "");
            list.add(statusStr.charAt(12));
            list.add(statusStr.charAt(11));
            list.add(statusStr.charAt(10));
            list.add(statusStr.charAt(9));
            list.add(statusStr.charAt(8));
            list.add(statusStr.charAt(7) + "" + statusStr.charAt(6) + "");
            list.add(statusStr.charAt(5));
            list.add(statusStr.charAt(4));
            list.add(statusStr.charAt(3));
            list.add(statusStr.charAt(2));
            list.add(statusStr.charAt(1));
            list.add(statusStr.charAt(0));
            StringBuffer sb = new StringBuffer();
            for (int s = 0; s < list.size(); s++) {
                sb.append(list.get(s));
            }

            dataListFinal.add(sb.toString());
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());//入库时间
            if(ParamConstants.startWith.equals("11")) {
                //北京 保电状态添加
                String otherDi = "switch-status";
                List switchStatusList = new ArrayList();
                switchStatusList.add(mpedIdStr);
                switchStatusList.add(tmnlId);
                switchStatusList.add(0);//mped_index
                switchStatusList.add(list.get(3).toString());
                switchStatusList.add(list.get(6).toString());
                switchStatusList.add(list.get(10).toString());
                switchStatusList.add(orgNo.substring(0, 5));
                switchStatusList.add(new Date());

                CommonUtils.putToDataHub(otherDi, mpedIdStr, switchStatusList, null, listDataObj);
            }
        } else if ("1603002".equals(businessDataitemId)) {//节点信息
            for (int i = 0; i < dataList.size(); i++) {
                String taskid = idGenerator.next();
                List dlist = (List) dataList.get(i);
                String nodeList = (String) dlist.get(4);
                String[] data2 = {null, null};
                if (nodeList.contains("[")) {
                    String nodes = nodeList.substring(1, nodeList.length() - 1);
                    data2 = nodes.split(",");
                }
                String nodeAddr = dlist.get(0).toString();
                if (nodeAddr.contains("F")) {
                    continue;
                }

                //拼接终端地址用于后续查找档案
                String full = areaCode + termAddr;
                String realAc1 = full.substring(3, 7);
                String realTd1 = full.substring(7, 12);
                String realAc = String.valueOf(Integer.parseInt(realAc1));
                String realTd = String.valueOf(Integer.parseInt(realTd1));

                List alllList = new ArrayList();
                alllList.add(tmnlId);//终端id
                alllList.add(taskid);
                alllList.add(nodeAddr);
                alllList.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                alllList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//采集日期
                alllList.add(orgNo);
                alllList.add(null);//采集点编号CP_NO
                alllList.add(realAc);
                alllList.add(realTd);
                alllList.add(dataList.size());//节点总数
                alllList.add(dataList.size());//应答数量
                alllList.add(i + 1);//节点序号
                alllList.add(1);///起始序号
                alllList.add(new BigDecimal(dlist.get(2).toString()));//节点标识
                alllList.add(new BigDecimal(dlist.get(3).toString()));//代理节点标识
                alllList.add(data2[0]);//节点层级
                alllList.add(String.valueOf(data2[1]).trim());//节点角色
                alllList.add(dlist.get(1));//节点类型
                alllList.add(null);//相位信息
                alllList.add(null);//线路异常识别码
                alllList.add(null);
                alllList.add(null);
                alllList.add(null);
                alllList.add(null);
                alllList.add(null);
                alllList.add(orgNo.substring(0, 5));
                alllList.add(new Date());

                CommonUtils.putToDataHub(businessDataitemId, tmnlId, alllList, refreshKey, listDataObj);
            }
        } else if ("207010".equals(businessDataitemId)) {//终端本地通信模块信息
            dataList = (List) dataList.get(0);
            if (dataList.size() != 3) {
                return;
            }
            String str = (String) dataList.get(0);
            List datList = (List) dataList.get(1);
            List cpList = (List) dataList.get(2);
            boolean type = str.toUpperCase().contains("PLC");
            //模块格式
            String modId = null;
            List<String> dt = new ArrayList<>();
            if (str.contains(";")) {
                String[] stSplit = str.split(";");
                if (stSplit.length == 7) {
                    for (int i = 0; i < stSplit.length; i++) {
                        String[] m = stSplit[i].split("=");
                        if (m.length < 2) {
                            dt.add(null);
                        } else {
                            dt.add(m[1]);
                        }
                    }
                }
            }
            modId = "0" + dt.get(5);
//          type=PLC;mfrs=TX;idformat=2;id=01029C01C1FB02SCA100010944733A16033384780CA8;mmfrs=TX;midformat=2;mid=4130054201901000002699
            dataListFinal.add(tmnlId);
            dataListFinal.add(orgNo);
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//数据日期
            dataListFinal.add(cpList.get(0));//厂商代码
            dataListFinal.add(cpList.get(1));
            dataListFinal.add(String.valueOf(cpList.get(3)));//版本
            dataListFinal.add(DateUtil.format((Date) cpList.get(2), DateUtil.defaultDatePattern_YMD));//版本日期
            dataListFinal.add(null);//模块地址
            dataListFinal.add(null);//模块ID长度
            dataListFinal.add(modId);//模块ID格式
            dataListFinal.add(dt.get(6));//模块ID
            dataListFinal.add(null);//组合ID
            dataListFinal.add(datList.get(0));//波特率
            dataListFinal.add(datList.get(1));//校验位
            dataListFinal.add(datList.get(2));//数据位
            dataListFinal.add(datList.get(3));//停止位
            dataListFinal.add(datList.get(4));//流控
            if (type) {
                dataListFinal.add("1");//模块类型
            } else {
                dataListFinal.add("2");//模块类型
            }
            dataListFinal.add(dt.get(1));//芯片厂商代码
            dataListFinal.add(dt.get(2));//芯片Id格式类型
            dataListFinal.add(dt.get(3));//芯片id信息
            dataListFinal.add(dt.get(4));//模块厂商代码
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());
            dataListFinal.add("01");//标志位
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if ("207009".equals(businessDataitemId)) {//终端远程通信模块信息 end
            //获取档案信息
            dataList = (List) dataList.get(0);
            if (dataList.size() == 0) {
                return;
            }
            dataListFinal.add(tmnlId);
            dataListFinal.add(orgNo);
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
            dataListFinal.add(dataList.get(0));//厂商编码
            dataListFinal.add(String.valueOf(dataList.get(5)).trim());//模块型号
            dataListFinal.add(dataList.get(1));//主用软件版本
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dataListFinal.add(sdf.format(OopDataUtils.versionDate(dataList.get(2))));//终端软件发布日期
            dataListFinal.add(dataList.get(3));//硬件版本
            dataListFinal.add(sdf.format(OopDataUtils.versionDate(dataList.get(4))));//终端硬件发布日期
            dataListFinal.add(null);//sim卡 CCID号
            dataListFinal.add(null);//sim卡号
            dataListFinal.add(null);//模块ID长度
            dataListFinal.add(null);//模块ID格式
            dataListFinal.add(null);//模块ID
            dataListFinal.add(null);//组合ID
            dataListFinal.add(null);//运行网络类型
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());
            dataListFinal.add("01");
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if ("2004207011".equals(businessDataitemId)) {//集中器HPLC模块芯片召测
            if (dataList == null || dataList.size() == 0) return;

            for (Object datas : dataList) {
                List dt = (List) datas;
                if (dt.size() == 0) {
                    continue;
                }
                meterAddr = (String) dt.get(1);
                //获取档案信息
                terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                String tmnlIdEnd = terminalArchivesObject.getTerminalId();
                String mpedId = terminalArchivesObject.getID();
                orgNo = terminalArchivesObject.getPowerUnitNumber();
                if (null == tmnlId || "".equals(tmnlId) || null == mpedId || "".equals(mpedId)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
                    continue;
                }
                List finalList = new ArrayList<>();
                //type=3;mfrs=TC;idformat=2;id=01029C01C1FB0254435632000007B3F52AE92C1C5CC952EC;mmfrs=TC;midformat=1;mid=4130054000000046836176
                String longStr = dt.get(2).toString();
                finalList.add(tmnlIdEnd);
                finalList.add(mpedId);//测量点编号
                finalList.add(orgNo);//所属单位
                finalList.add(meterAddr);//设备地址
                finalList.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                finalList.add(dt.get(0));//从节点序号
                finalList.add(null);//芯片代码
                finalList.add(dt.get(0));//测量点号
                finalList.add(OopDataUtils.splitCore(longStr, 0));//leixing
                finalList.add(null);//组合ID
                finalList.add(null);//生产厂商
                finalList.add(null);//模块类型
                finalList.add(OopDataUtils.splitCore(longStr, 6) == null ? null : OopDataUtils.splitCore(longStr, 6).length());//模块id长度
                finalList.add(OopDataUtils.splitCore(longStr, 5));//模块ID格式
                finalList.add(OopDataUtils.splitCore(longStr, 6));//模块ID
                finalList.add(null);//更新标志
                finalList.add(OopDataUtils.splitCore(longStr, 1));//芯片厂商
                finalList.add(OopDataUtils.splitCore(longStr, 2));//芯片id格式类型
                finalList.add(OopDataUtils.splitCore(longStr, 3));//芯片id信息
                finalList.add(OopDataUtils.splitCore(longStr, 4));//模块厂商代码
                finalList.add(dt.get(3));//从节点响应时长
                finalList.add(dt.get(4));//从节点最近一次通信成功时间

                finalList.add(orgNo.substring(0, 5));
                finalList.add(new Date());
                if (src == 0) {
                    refreshKey[1] = tmnlId;
                }
                CommonUtils.putToDataHub(businessDataitemId, tmnlIdEnd, finalList, refreshKey, listDataObj);
            }

        } else if ("FF100200".equals(oopDataItemId)) {//召测电表接线错误信息
            if (dataList.size() == 0) {
                return;
            }
            for (Object ob : dataList) {
                List dL = (List) ob;
                if (dL.size() < 3) {
                    return;
                }
                List alllList = new ArrayList();//sqlmapping list
                List lis = (List) dL.get(2);
                String nodeInfo = (String) lis.get(0);
                alllList.add(tmnlId);
                alllList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                alllList.add(idGenerator.next());
                alllList.add(dL.get(0));
                alllList.add(dL.get(1));
                alllList.add(nodeInfo.substring(0, 1));//表档案状态
                alllList.add(nodeInfo.substring(1, 3));//台区状态
                alllList.add(nodeInfo.substring(3, 6));//相序
                alllList.add(nodeInfo.substring(6, 7));//线路状态
                alllList.add(dL.get(3));//节点台区地址
                alllList.add(orgNo.substring(0, 5));
                alllList.add(new Date());
                if (src == 0) {
                    refreshKey[1] = tmnlId;
                }
                CommonUtils.putToDataHub(businessDataitemId, tmnlId, alllList, refreshKey, listDataObj);
            }
        } else if ("2004207012".equals(businessDataitemId)) {//终端Sim卡信息
            dataListFinal.add(tmnlId);
            dataListFinal.add(orgNo);
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//最后召测时间
            dataListFinal.add(null);//厂商编码
            dataListFinal.add(null);//模块型号
            dataListFinal.add(null);//主用软件版本
            dataListFinal.add(null);//终端软件发布日期
            dataListFinal.add(null);//硬件版本
            dataListFinal.add(null);//终端硬件发布日期
            dataListFinal.add(dataList.get(0));//sim卡 CCID号
            dataListFinal.add(null);//sim卡号
            dataListFinal.add(null);//模块ID长度
            dataListFinal.add(null);//模块ID格式
            dataListFinal.add(null);//模块ID
            dataListFinal.add(null);//组合ID
            dataListFinal.add(null);//运行网络类型
            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());
            dataListFinal.add("02");
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if ("207001".equals(businessDataitemId)) {//终端版本召测 ok
            dataListFinal.add(tmnlId);//terminalId
            List dL = (List) dataList.get(0);
            dataListFinal.add(dL.get(0));//厂商编号
            dataListFinal.add(dL.get(5));//设备编号
            dataListFinal.add(dL.get(1));//软件版本
            dataListFinal.add(dL.get(3));//硬件版本
            dataListFinal.add(OopDataUtils.versionSoftDate(dL.get(2)));//软件版本时间
            dataListFinal.add(OopDataUtils.versionSoftDate(dL.get(4)));//硬件版本时间
            dataListFinal.add(new Date());//
            dataListFinal.add(orgNo.substring(0, 5));
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if("207013".equals(businessDataitemId)){//ESAM信息
            //拼接终端地址用于后续查找档案
            String full = areaCode + termAddr;
            String realAc1 = full.substring(3, 7);
            String realTd1 = full.substring(7, 12);
            String realAc = String.valueOf(Integer.parseInt(realAc1));
            String realTd = String.valueOf(Integer.parseInt(realTd1));
            dataListFinal.add(tmnlId);//TERMINAL_ID:BIGINT:本实体记录的唯一标识
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
            dataListFinal.add(realTd);//TERMINAL_ADDR:VARCHAR:终端通信的地址码信息。
            dataListFinal.add(Integer.valueOf(realAc));//AREA_CODE:INT:行政区划码
            dataListFinal.add(dataList.get(0));//ESAM_INDEX:VARCHAR:芯片序列号
            dataListFinal.add(null);//CERTIFICATE_INDEX:VARCHAR:证书序列号
            dataListFinal.add(null);//COUNTER:VARCHAR:计数器
            dataListFinal.add(null);//CHIP_STATE:VARCHAR:芯片状态
            dataListFinal.add(null);//KEY_VERSION:VARCHAR:密钥版本
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段

            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        }else if("1604008".equals(businessDataitemId)){//通信配置698

            List dList= (List) dataList.get(0);
            //拼接终端地址用于后续查找档案
            String full = areaCode + termAddr;
            String realAc1 = full.substring(3, 7);
            String realTd1 = full.substring(7, 12);
            String realAc = String.valueOf(Integer.parseInt(realAc1));
            String realTd = String.valueOf(Integer.parseInt(realTd1));

            dataListFinal.add(tmnlId);//TERMINAL_ID:BIGINT:本实体记录的唯一标识
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
            dataListFinal.add(realTd);//TERMINAL_ADDR:VARCHAR:终端通信的地址码信息。
            dataListFinal.add(Integer.valueOf(realAc));//AREA_CODE:INT:行政区划码
            dataListFinal.add(dList.get(0));//WORK_PATTERN:VARCHAR:工作模式,0:混合模式,1:客户机模式,2:服务器模式
            dataListFinal.add(dList.get(1));//ONLINE_TYPE:VARCHAR:在线方式,0:永久在线，1:被动激活
            dataListFinal.add(dList.get(2));//CONNECT_TYPE:VARCHAR:连接方式，0:TCP,1:UDP
            dataListFinal.add(dList.get(3));//CONNECT_APPLY_TYPE:VARCHAR:连接应用方式，0:主备模式，1:多连接模式
            List plist= (List) dList.get(4);
            if(plist.size()==0){
                dataListFinal.add(null);//LISTEN_PORT_LIST:VARCHAR:侦听端口列表
            }else{
                dataListFinal.add(plist.toString());//LISTEN_PORT_LIST:VARCHAR:侦听端口列表
            }
            dataListFinal.add(dList.get(5));//APN:VARCHAR:APN
            dataListFinal.add(dList.get(6));//USER_NAME:VARCHAR:用户名
            dataListFinal.add(dList.get(7));//PASSWORD:VARCHAR:密码
            dataListFinal.add(dList.get(8));//PROXY_HOST:VARCHAR:代理服务器地址
            dataListFinal.add(((Long)dList.get(9)).intValue());//PROXY_PORT:INT:代理端口
            dataListFinal.add(dList.get(10));//RETRY_TIME:INT:重发次数
            dataListFinal.add(null);//TIMEOUT:INT:超时时间（秒）
            dataListFinal.add(((Long)dList.get(11)).intValue());//HEARTBEAT_CYCLE:INT:心跳周期(秒)
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        }else if ("2400003".equals(businessDataitemId)) {//设备状态数据
            dataListFinal.add(tmnlId);//terminalId
            List dL = (List) dataList.get(0);

            dataListFinal.add(String.valueOf(dL.get(0)));//GATHER_RATA:INT:采集频率
            dataListFinal.add(String.valueOf(dL.get(1)));//RECORD_STATUS:VARCHAR:录波状态
            dataListFinal.add(String.valueOf(dL.get(2)));//CONNECT_STATUS:INT:接线状态
            dataListFinal.add(String.valueOf(dL.get(3)));//VOL_SEQUENCE:VARCHAR:电压相序
            dataListFinal.add(String.valueOf(dL.get(4)));//ELE_SEQUENCE:VARCHAR:电流相序
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if ("204031".equals(businessDataitemId)) {

            dataListFinal.add(tmnlId);//终端id
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            dataListFinal.add(orgNo);//

            Object objF = dataList.get(0);
            if (objF == null) {
                dataListFinal.add(null);//
            } else {
                try {
                    double dataD = Double.valueOf(objF.toString());
                    if (0 < dataD && dataD < 3) {
                        dataListFinal.add("00000000");//
                    } else if (3 <= dataD && dataD <= 6) {
                        dataListFinal.add("10000000");//
                    } else {
                        dataListFinal.add(null);//
                    }
                } catch (Exception e) {
                    logger.error("the volt is not number ", e);
                    dataListFinal.add(null);//
                }
            }
            dataListFinal.add(objF);


            dataListFinal.add(orgNo.substring(0, 5));
            dataListFinal.add(new Date());//
            indexKey = tmnlId;
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
        } else if ("1603004".equals(businessDataitemId)) {//相位信息
            for (Object list1 : dataList) {
                try {
                    List<Object> dF = new ArrayList<Object>();
                    List dL = (List) list1;
                    meterAddr = (String) dL.get(0);
                    //获取前置写入redis中的真实表地址
                    //获取档案信息
                    terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
                    orgNo = terminalArchivesObject.getPowerUnitNumber();
                    mpedIdStr = terminalArchivesObject.getID();

                    //拼接终端地址用于后续查找档案
                    String full = areaCode + termAddr;
                    String realAc1 = full.substring(3, 7);
                    String realTd1 = full.substring(7, 12);
                    String realAc = String.valueOf(Integer.parseInt(realAc1));
                    String realTd = String.valueOf(Integer.parseInt(realTd1));

                    dF.add(mpedIdStr);//主键
                    dF.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                    dF.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//采集日期
                    dF.add(orgNo);//供电单位编号
                    dF.add(null);//采集点编号
                    dF.add(realAc);//区域码
                    dF.add(meterAddr);//电表地址
                    dF.add(realTd);//终端地址码
                    String pt = (String) dL.get(3);
                    dF.add(OopDataUtils.getPort(pt));//通信端口号
                    dF.add(dL.get(1));//中继路由级数
                    //TODO 转换成3761的数据入库格式 如:3改成001  2改成010  1改成100
                    Object stat = dL.get(6);
                    String end = null;
                    String[] abc = {null, null, null};
                    if (stat != null) {
                        int statNum = Integer.parseInt(stat.toString());
                        if (statNum > 0 && statNum <= 3) {
                            abc[0] = "0";
                            abc[1] = "0";
                            abc[2] = "0";
                            abc[statNum - 1] = "1";
                            end = abc[0] + abc[1] + abc[2];
                        } else if (statNum == 0) {
                            abc[0] = "0";
                            abc[1] = "0";
                            abc[2] = "0";
                            end = "000";
                        }
                    }
                    dF.add(abc[0]);//抄表相位A
                    dF.add(abc[1]);//抄表相位B
                    dF.add(abc[2]);//抄表相位C
                    dF.add(end);//抄表相位

//                    dF.add(null);//抄表相位A
//                    dF.add(null);//抄表相位B
//                    dF.add(null);//抄表相位C
//                    dF.add(dL.get(6));//抄表相位
                    dF.add(dL.get(7));//线路异常标识
                    dF.add(null);//载波表相别
                    dF.add(null);//相序类型
                    dF.add(null);//实际相位A
                    dF.add(null);//实际相位B
                    dF.add(null);//实际相位C
                    dF.add(null);//实际相位
                    dF.add(null);//发送品质
                    dF.add(null);//接收品质
                    dF.add(null);//最近一次抄表成功/失败标志
                    dF.add(dL.get(4));//最近一次抄表成功时间
                    dF.add(null);//最近一次抄表失败时间
                    dF.add(dL.get(5));//最近连续失败累计次数
                    dF.add(orgNo.substring(0, 5));//分库字段
                    dF.add(new Date());//插入或更新时间
                    CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dF, refreshKey, listDataObj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if ("70305".equals(businessDataitemId)) {//零线电流
            List<Object> dF = new ArrayList<>();
            dF.add(mpedIdStr);
            dF.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
            dF.add("0");
            dF.add(orgNo.substring(0, 5));
            dF.add(dataList.get(0));
            dF.add(dates);
            dF.add("00");
            if (src == 0) {
                dF.add("30");
            } else {
                dF.add("34");
            }
            dF.add(new Date());//插入或更新时间
            dF.add(orgNo.substring(0, 5));//分库字段
            CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dF, refreshKey, listDataObj);
        } else if (oopDataItemId.startsWith("00") || "202D0200".equals(oopDataItemId) || "202C0201".equals(oopDataItemId)) {
            if ("00900200".equals(oopDataItemId) || "202D0200".equals(oopDataItemId) || "202C0201".equals(oopDataItemId)) {
                //剩余金额
                dataListFinal.add(new BigDecimal(mpedIdStr));
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//dataDate
                dataListFinal.add(new Date());//抄表日期
                dataListFinal.add(orgNo);//orgNo占位
                dataListFinal.add(null);//剩余电量
                if ("202D0200".equals(oopDataItemId)) {
                    if (protocolId == 8) {
                        //直抄透支金额暂不支持
                        return;
                    }
                    String o = (String) dataList.get(0);
                    double dl = Double.parseDouble(o);
                    if (dl != 0) {
                        dataListFinal.add(-dl);//剩余金额
                    } else {
                        dataListFinal.add(dataList.get(1));//剩余金额
                    }
                    dataListFinal.add(null);//报警电量
                    dataListFinal.add(null);//故障电量
                    dataListFinal.add(null);//累计购电量
                    dataListFinal.add(null);//累计购电金额
                    dataListFinal.add(dataList.get(2));//购电次数
                    dataListFinal.add(null);//赊欠门限值
                    dataListFinal.add(dataList.get(0));//透支电量
                } else {
                    dataListFinal.add(dataList.get(0));//剩余金额
                    dataListFinal.add(null);//报警电量
                    dataListFinal.add(null);//故障电量
                    dataListFinal.add(null);//累计购电量
                    dataListFinal.add(null);//累计购电金额
                    dataListFinal.add(null);//购电次数
                    dataListFinal.add(null);//赊欠门限值
                    dataListFinal.add(null);//透支电量
                }

                dataListFinal.add(new Date());
                dataListFinal.add(orgNo.substring(0, 5));
            } else {
                boolean allNull = OopDataUtils.allNull(dataList);
                if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                    return;
                }
                if (dataList.get(0) == null) {
                    return;
                }
                dataListFinal = OopDataUtils.getDataList("current", dataList, oopDataItemId, dates, mpedIdStr, dates);
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                dataListFinal.add("00");//未分析
                if (src == 0) {
                    if (protocolId == 8) {
                        dataListFinal.add("30");
                    } else {
                        dataListFinal.add("10");
                    }
                } else {
                    if (protocolId == 8) {
                        dataListFinal.add("34");
                    } else {
                        dataListFinal.add("14");
                    }
                }
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                dataListFinal.add(DateUtil.format(OopDataUtils.timeAddOrMinus(dates, -1), DateUtil.defaultDatePattern_YMD));
                if (src == 0) {
                    businessDataitemId = "other_freeze";
                }
            }
        } else if (Arrays.asList("F2090800", "F2091400").contains(oopDataItemId)) {
            List dL = dataList;
            if ("F2090800".equalsIgnoreCase(oopDataItemId)) {
                dL = (List) dataList.get(0);
                meterAddr = (String) dL.get(1);
            }
            //获取前置写入redis中的真实表地址
            //获取档案信息
            terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
            meterId = terminalArchivesObject.getMeterId();
            orgNo = terminalArchivesObject.getPowerUnitNumber();
            tmnlId = terminalArchivesObject.getTerminalId();

            dataListFinal = OopCurrentPush.getDataListFinal(dL, oopDataItemId, tmnlId, meterId, orgNo);
            if (dataListFinal == null || dataListFinal.isEmpty() || dataListFinal.size() == 0) {
                return;
            }
            if (src == 0) {
                refreshKey[1] = tmnlId;
            }
            if (dataListFinal.get(0) instanceof List) {
                for (Object li : dataListFinal) {
                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, (List<Object>) li, refreshKey, listDataObj);
                }
                dataListFinal = null;
            }
            indexKey = tmnlId;
        } else if ("80149".equals(businessDataitemId)) {//节点版本信息
            if (dataList == null) {
                return;
            }
            //拼接终端地址用于后续查找档案
            String full = areaCode + termAddr;
            String realAc1 = full.substring(3, 7);
            String realTd1 = full.substring(7, 12);
            String realAc = String.valueOf(Integer.parseInt(realAc1));
            String realTd = String.valueOf(Integer.parseInt(realTd1));
            for (int i = 0; i < dataList.size(); i++) {
                List endDateList = (List) dataList.get(i);
                if (endDateList.size() < 6) {
                    continue;
                }
                try {
                    String mpedId = null;
                    int nodeIdex = Integer.parseInt(endDateList.get(1).toString());
                    String nodeAddr = endDateList.get(0).toString();
                    Date softDate = (Date) endDateList.get(3);
                    if (nodeIdex == 1) {
                        terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, nodeAddr);
                        mpedId = terminalArchivesObject.getID();
                        if (mpedId == null) {
                            continue;
                        }
                    } else {
                        mpedId = tmnlId;
                    }

                    List alllList = new ArrayList();
                    alllList.add(new BigDecimal(tmnlId));//终端id
                    alllList.add(nodeIdex);//NODE_IDEX
                    alllList.add(realAc);//AREA_CODE
                    alllList.add(realTd);//TERMINAL_ADDR
                    alllList.add(mpedId);//MPED_ID 节点idex不为0写入
                    alllList.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//数据日期
                    alllList.add(nodeAddr);//NODE_ADDR
                    alllList.add(endDateList.get(2).toString());//SOFTWARE_VERSION_NO
                    alllList.add(DateUtil.format(softDate, DateUtil.defaultDatePattern_YMD));//SOFTWARE_VERSION_DATE
                    alllList.add(endDateList.get(4));//MODULE_VENDOR_CODE
                    alllList.add(endDateList.get(5));//CHIP_CODE
                    alllList.add(orgNo.substring(0, 5));
                    alllList.add(new Date());
                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, alllList, refreshKey, listDataObj);
                } catch (Exception e) {
                    logger.error("节点版本信息:"+e);
                    continue;
                }
            }
        } else if ("60105".equals(businessDataitemId) || "60106".equals(businessDataitemId)) {//电表内部时钟电池电压 或 电表内部电池工作时间
            dataListFinal.add(new BigDecimal(mpedIdStr));//MPED_ID:BIGINT:测量点ID
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
            dataListFinal.add(orgNo);//ORG_NO:VARCHAR:供电单位
            if ("60105".equals(businessDataitemId)) {
                dataListFinal.add(dataList.get(0));//BATTERY_VOLTAGE:DECIMAL:时钟电池电压：单位 V
            } else if ("60106".equals(businessDataitemId)) {
                dataListFinal.add(new BigDecimal(dataList.get(0).toString()));//WORK_TIME:BIGINT:时钟电池工作时间：分/单位
            }
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间电表内部电池工作时间
        } else if ("29000200".equals(oopDataItemId) || "29010200".equals(oopDataItemId) || "28070200".equals(oopDataItemId) || "290A0200".equals(oopDataItemId)) {//当前温度、湿度
            indexKey = tmnlId;
            dataListFinal.add(tmnlId);//TERMINAL_ID:BIGINT:本实体记录的唯一标识
            dataListFinal.add(DateUtil.format(dates, "yyyy-MM-dd"));//DATE_DATE:VARCHAR:数据日期
            dataListFinal.add(dataList.get(0));//CURRENT_TEMPERATURE:VARCHAR:当前温度
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            dataListFinal.add(meterAddr);//COMM_ADDR:VARCHAR:断路器地址
            dataListFinal.add(index[0]);//RESIDUAL_INDEX:INT:断路器序号
        } else if ("28000200".equals(oopDataItemId)) {//当前电流最大相剩余电流值
            indexKey = tmnlId;
            dataListFinal.add(tmnlId);//TERMINAL_ID:BIGINT:本实体记录的唯一标识
            dataListFinal.add(DateUtil.format(dates, "yyyy-MM-dd"));//DATE_DATE:VARCHAR:数据日期
            dataListFinal.add(dataList.get(0));//RESIDUAL_CURRENT:VARCHAR:当前剩余电流最大相
            dataListFinal.add(String.valueOf(dataList.get(1)));//RESIDUAL_CURRENT_MAXTERM:VARCHAR:剩余电流值（单位：mA）
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            dataListFinal.add(meterAddr);//COMM_ADDR:VARCHAR:断路器地址
            dataListFinal.add(index[0]);//RESIDUAL_INDEX:INT:断路器序号
        } else if ("29090200".equals(oopDataItemId)) {//电容器数据
            indexKey = tmnlId;
            dataListFinal.add(tmnlId);//TERMINAL_ID:BIGINT:本实体记录的唯一标识
            dataListFinal.add(DateUtil.format(dates, "yyyy-MM-dd"));//DATE_DATE:VARCHAR:数据日期
            for (int i = 0; i < dataList.size(); i++) {
                List data = (List) dataList.get(i);
                for (int j = 0; j < data.size(); j++) {
                    Object obj = data.get(j);
                    dataListFinal.add(String.valueOf(obj));
                }
            }
            dataListFinal.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
            dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            dataListFinal.add(meterAddr);//COMM_ADDR:VARCHAR:断路器地址
            dataListFinal.add(index[0]);//RESIDUAL_INDEX:INT:断路器序号
        } else if ("6010401".equals(businessDataitemId)) {
            List dList = (List) dataList.get(0);
            dataListFinal.add(new BigDecimal(mpedIdStr));
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));//dataDate
            dataListFinal.add(collDate);//抄表日期// COL_TIME:TIMESTAMP,
            dataListFinal.add(orgNo);// ORG_NO:STRING,
            dataListFinal.add(null);//剩余电量  // REMAIN_ENEGY:DOUBLE,
            dataListFinal.add(dList.get(0));//剩余金额 // REMAIN_MONEY:DOUBLE,  1
            dataListFinal.add(null);//报警电量  // ALARM_ENEGY:DOUBLE,
            dataListFinal.add(null);//故障电量  // FAIL_ENEGY:DOUBLE,
            dataListFinal.add(null);//累计购电量   // SUM_ENEGY:DOUBLE,
            dataListFinal.add(null);//累计购电金额 // SUM_MONEY:DOUBLE,
            if (mptl == 3) {//645
                dataListFinal.add(Integer.valueOf(dList.get(2).toString().trim()));//购电次数 // BUY_NUM:BIGINT,
            } else {
                dataListFinal.add(Integer.valueOf(dList.get(1).toString().trim()));//购电次数 // BUY_NUM:BIGINT,
            }
            dataListFinal.add(null);//赊欠门限值 // OVERDR_LIMIT:DOUBLE,
            dataListFinal.add(null);//透支电量 // OVERDR_ENEGY:DOUBLE,
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(orgNo.substring(0, 5));//前闭后开  前5位 不包括[5]
        } else {
            if ("153001".contains(businessDataitemId)) {//节点相位信息单元
                for (Object objL : dataList) {
                    try {
                        List dL = (List) objL;
                        int node_index = Integer.parseInt(dL.get(0).toString());
                        String node_addr = dL.get(1).toString();
                        String by = dL.get(2).toString();
                        int stat = Integer.parseInt(by);
                        String binaryString = Integer.toBinaryString(stat);
                        StringBuilder stringBuilder = new StringBuilder(binaryString);
                        for (int h = 0; h < 8; h++) {
                            if (stringBuilder.length() != 8) {
                                stringBuilder.insert(0, "0");
                            }
                        }
                        String node_phase = stringBuilder.toString();
                        List dFList = new ArrayList();
                        dFList.add(tmnlId);
                        dFList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        dFList.add(node_index);
                        dFList.add(node_addr);
                        dFList.add(node_phase);
                        dFList.add(orgNo.substring(0, 5));
                        dFList.add(new Date());
                        CommonUtils.putToDataHub(businessDataitemId, tmnlId, dFList, refreshKey, listDataObj);
                    } catch (Exception e) {
                        logger.error("F2090A00:" + areaCode + "_" + termAddr + "数据返回异常", e);
                        continue;
                    }
                }

            }
            if (src == 0) {
                if ("31060800".equals(oopDataItemId) || "31060900".equals(oopDataItemId)) {
                    String type = null;
                    String value = "1";
                    if ("31060800".equals(oopDataItemId)) {
                        type = "02";
                    }
                    if ("31060900".equals(oopDataItemId)) {
                        type = "01";
                    }
                    String v = (String) dataList.get(0);
                    if ("0".equals(v)) {
                        value = "0";
                    }
                    dataListFinal.add(new BigDecimal(tmnlId));
                    dataListFinal.add(oopDataItemId);
                    dataListFinal.add(type);
                    dataListFinal.add(value);
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataListFinal.add(new Date());
                    indexKey = tmnlId;
                    refreshKey[1] = tmnlId;
                } else if ("20040201".equals(oopDataItemId)) {
                    dataListFinal.add(meterId);
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    dataListFinal.add("1");//flag
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
                    dataListFinal.add(dataList.get(0));
                    dataListFinal.add(dates);//电压时间点
                    dataListFinal.add("00");//未分析
                    //区分直抄137/预抄133
                    if (protocolId == 8) {
                        dataListFinal.add("30");//直抄
                    } else {
                        dataListFinal.add("10");//预抄
                    }
                    dataListFinal.add(new Date());//入库时间
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    indexKey = meterId;
                    refreshKey[1] = meterId;
                } else if ("0400010C".equals(oopDataItemId)) {//电表时钟

                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    dataListFinal.add(mpedIdStr);
                    String date = (String) dataList.get(0);
                    if (!date.contains(" ")) {
                        return;
                    }
                    String[] str = date.split(" ");
                    if (str.length != 3) {
                        return;
                    }
                    StringBuffer sb = new StringBuffer();
                    sb.append(str[0]);
                    sb.append(" ");
                    sb.append(str[2]);
                    dataListFinal.add(sb.toString());
                    dataListFinal.add(dates);
                    dataListFinal.add("03");
                    dataListFinal.add(new Date());
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                }
            } else if (src == 1) {
                if ("02030000".equals(oopDataItemId)) {
                    dataListFinal.add(mpedIdStr);
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                    dataListFinal.add("1");//flag
                    dataListFinal.add(orgNo);//供电单位
                    dataListFinal.add(dataList.get(0));
                    dataListFinal.add(dates);//电压时间点
                    dataListFinal.add("00");//未分析
                    //区分直抄137/预抄133
                    if (protocolId == 8) {
                        dataListFinal.add("34");//直抄
                    } else {
                        dataListFinal.add("14");//预抄
                    }
                    dataListFinal.add(new Date());//入库时间
                    dataListFinal.add(orgNo.substring(0, 5));
                }
            }

        }
        if (dataListFinal != null && !dataListFinal.isEmpty()) {
            if (eventList.contains(oopDataItemId)) {
                Object checkDate = dataListFinal.get(dataListFinal.size() - 1);
                boolean err = DateFilter.betweenDay(checkDate, 0, 180);
                if (!err) {
                    businessDataitemId = "no_power_err";
                }
            }
            CommonUtils.putToDataHub(businessDataitemId, indexKey, dataListFinal, refreshKey, listDataObj);
        }


    }
}
