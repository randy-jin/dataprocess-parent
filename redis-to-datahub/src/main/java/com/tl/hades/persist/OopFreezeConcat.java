package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.MeterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class OopFreezeConcat {
    private final static Logger logger = LoggerFactory.getLogger(OopFreezeConcat.class);

    private final static String redisName = "Q_BASIC_DATA_50040200_DAY_R";
    public static final Map<String, String> freeMap = new HashMap<>(100);

    static {
        freeMap.put("00110200", "1_A_phread");//有功 正
        freeMap.put("00120200", "1_B_phread");
        freeMap.put("00130200", "1_C_phread");
        freeMap.put("00310200", "2_A_phread");//无功 正
        freeMap.put("00320200", "2_B_phread");
        freeMap.put("00330200", "2_C_phread");
        freeMap.put("00210200", "5_A_phread");//有功
        freeMap.put("00220200", "5_B_phread");
        freeMap.put("00230200", "5_C_phread");
        freeMap.put("00410200", "6_A_phread");//无功
        freeMap.put("00420200", "6_B_phread");
        freeMap.put("00430200", "6_C_phread");
    }


    public static void getDataList(MeterData meterData, int protocolId, String areaCode, String termAddr, List<DataObject> listDataObj, String src) throws Exception {
        List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
        if (doList == null || doList.size() == 0) {
            return;
        }
        //ABC三相电压合格率
        List<Object> dataListABC = new ArrayList<Object>();
        List dataList = null;
        List dataListA = null;
        List dataListB = null;
        List dataListC = null;
        String meterAddr = meterData.getMeterAddr();
        String businessDataitemId;
        Date dates = null;//存储时标
        Date collDate = null;//采集成功时标
        String oopDataItemIdA = null;
        String oopDataItemId = null;
        for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
            try {
                if (dataObject.getDataItem().equals("60420200")) {//抄表时间
                    if (OopDataUtils.notNull(dataObject.getData(), Date.class)) {
                        dates = (Date) dataObject.getData();
                    }
                    //存储时标
                } else if (dataObject.getDataItem().equals("202A0200")) {
                    if (OopDataUtils.notNull(dataObject.getData(), String.class)) {
                        //表地址
                        meterAddr = String.valueOf(dataObject.getData());
                    }
                } else if (dataObject.getDataItem().equals("60410200")) {
                    if (OopDataUtils.notNull(dataObject.getData(), Date.class)) {
                        collDate = (Date) dataObject.getData();//采集成功时标
                    }
                } else if ("20210200".equals(dataObject.getDataItem())) {
                    Object data = dataObject.getData();
                    if (data != null && !"null".equals(data)) {
                        if (dates == null) {
                            dates = DateUtil.parse(String.valueOf(data), "yyyy-MM-dd HH:mm:ss");
                            collDate = new Date();
                        }
                    }
                } else {
                    oopDataItemId = dataObject.getDataItem();//数据项
                    if ("21310201".equals(oopDataItemId)) {
                        oopDataItemIdA = oopDataItemId;
                        dataListA = (List) dataObject.getData();//A相合格率
                    } else if ("21320201".equals(oopDataItemId)) {
                        dataListB = (List) dataObject.getData();//B相合格率
                    } else if ("21330201".equals(oopDataItemId)) {
                        dataListC = (List) dataObject.getData();//C相合格率
                        dataList = dataListC;
                    } else if (freeMap.containsKey(oopDataItemId)) {
                        String[] vals = freeMap.get(oopDataItemId).split("_");
                        String dataType = vals[0];
                        String readType = vals[2];
                        if (dataList == null) {
                            dataList = new ArrayList();
                            dataList.add(readType);
                            dataList.add(dataType);
                            dataList.add(null);
                            dataList.add(null);
                            dataList.add(null);
                        }
                        if (vals[1].equals("A")) {
                            dataList.add(2, dataObject.getData());
                        } else if (vals[1].equals("B")) {
                            dataList.add(3, dataObject.getData());
                        } else if (vals[1].equals("C")) {
                            dataList.add(4, dataObject.getData());
                        }

                    } else {
                        if (dataObject.getData() != null) {
                            if (dataObject.getData() instanceof List) {
                                dataList = (List) dataObject.getData();
                            } else {
                                dataList = new ArrayList();
                                dataList.add(String.valueOf(dataObject.getData()));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("获取map对应的value异常:", e);
            }
        }
        if (collDate == null) {
            collDate = dates;
        }


        if (oopDataItemIdA != null) {
            oopDataItemId = oopDataItemIdA;
        }
        if (oopDataItemId == null || collDate == null || meterAddr == null || dataList == null || dates == null) {
            return;
        }

        //获取档案信息
        TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterAddr);
        String mpedIdStr = terminalArchivesObject.getID();
        if (null == mpedIdStr || "".equals(mpedIdStr)) {
            logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr + "_COMMADDR" + meterAddr);
            return;
        }

        Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
        businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
        if (businessDataitemId == null) {
            logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + oopDataItemId);
            return;
        }

        List<Object> dataListFinal = new ArrayList<Object>();

        if (oopDataItemId.startsWith("2")) {//abc电压合格率
            dataListFinal.add(mpedIdStr);
            for (int i = 1; i < 40; i++) {
                if (i < 16) {
                    if (i == 3) {
                        dataListFinal.add(valueOfInt(dataListA.get(3)));//A相电压超上限时间3
                    } else if (i == 5) {
                        dataListFinal.add(valueOfInt(dataListA.get(0)));//A相电压合格累计时间5
                    } else if (i == 8) {
                        dataListFinal.add(valueOfInt(dataListB.get(3)));
                        //B相电压超上限时间3
                    } else if (i == 10) {
                        dataListFinal.add(valueOfInt(dataListB.get(0)));
                        //B相电压合格累计时间5
                    } else if (i == 13) {
                        dataListFinal.add(valueOfInt(dataListC.get(3)));
                        //C相电压超上限时间3
                    } else if (i == 15) {
                        dataListFinal.add(valueOfInt(dataListC.get(0)));
                        //C相电压合格累计时间5
                    } else {
                        dataListFinal.add(0);
                    }
                } else if (i == 31) {
                    dataListFinal.add(valueOfInt(dataListA.get(2)));//A相电压超上限率31
                } else if (i == 37) {
                    dataListFinal.add(valueOfInt(dataListA.get(1)));//A相电压合格率37
                } else if (i == 32) {
                    dataListFinal.add(valueOfInt(dataListB.get(2)));//B相电压超上限率31
                } else if (i == 38) {
                    dataListFinal.add(valueOfInt(dataListB.get(1)));//B相电压合格率37
                } else if (i == 33) {
                    dataListFinal.add(valueOfInt(dataListC.get(2)));//C相电压超上限率31
                } else if (i == 39) {
                    dataListFinal.add(valueOfInt(dataListC.get(1)));//C相电压合格率37
                } else {
                    dataListFinal.add(null);
                }
            }
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
            dataListFinal.add("00");//未分析
            dataListFinal.add(src);
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
        } else if (oopDataItemId.startsWith("1")) {
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
            dataListFinal.add(collDate);//抄表时间
            Object demand_v = dataList.get(0);

            Object val = null;
            Object d = null;
            if (demand_v != null) {
                List DV = (List) demand_v;
                val = DV.get(0);
                d = DV.get(1);//demandtime
            }
            if(val==null){
                return;
            }
            dataListFinal.add(val);//demand_v
            Date demandDate = null;
            if (d != null) {
                try {
                    demandDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d.toString());
                    demandDate = EventUtils.dataFilter(demandDate);
                } catch (Exception e) {
                }
            }
            dataListFinal.add(demandDate);
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
            dataListFinal.add("00");//未分析
            dataListFinal.add(src);
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
        } else if (oopDataItemId.startsWith("01")) {//上一结算日需量发生时间
            dataListFinal.add(mpedIdStr);
            if (oopDataItemId.equals("01010001")) {//正向有功需量发生时间
                dataListFinal.add("1");//flag
            } else if (oopDataItemId.equals("01020001")) {
                dataListFinal.add("5");//flag
            } else if (oopDataItemId.equals("01030001")) {
                dataListFinal.add("2");//flag
            } else if (oopDataItemId.equals("01040001")) {
                dataListFinal.add("6");//flag
            }
            dataListFinal.add(collDate);//抄表时间
            Object demand_v = dataList.get(0);
            if(demand_v==null){
                return;
            }
            Object d = dataList.get(1);
            dataListFinal.add(demand_v);//demand_v
            Date demandDate = null;
            if (d != null) {
                try {
                    if (d.toString().length() == 17) {
                        d = d + ":00";
                    }
                    demandDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d.toString());
                    demandDate = EventUtils.dataFilter(demandDate);
                } catch (Exception e) {
                }
            }
            dataListFinal.add(demandDate);
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
            dataListFinal.add("00");//未分析
            dataListFinal.add(src);
            dataListFinal.add(new Date());//入库时间
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(DateUtil.format(DateFilter.getPrevMonthLastDay(dates), DateUtil.defaultDatePattern_YMD));
        } else if (oopDataItemId.startsWith("00")) {
            boolean allNull = OopDataUtils.allNull(dataList);
            if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                return;
            }
            if (dataList.get(0) == null) {
                return;
            }
            if (dataList.get(0).equals("phread")) {//分相示值
                businessDataitemId = "233004";
                dataListFinal.add(mpedIdStr);//
                dataListFinal.add(dataList.get(1));//
                dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
                dataListFinal.add(collDate);
                dataListFinal.add(dataList.get(2));
                dataListFinal.add(dataList.get(3));
                dataListFinal.add(dataList.get(4));
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());
                dataListFinal.add("00");//未分析
                dataListFinal.add(src);//自动抄表
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            } else {//正常示值
                if (protocolId == 8) {
                    int mptl = ProtocolArchives.getInstance().getMeterPtl(mpedIdStr);
                    if (mptl == 3) {
                        dataList.remove(dataList.size() - 1);
                    }
                }
                dataListFinal = OopDataUtils.getDataList(null, dataList, oopDataItemId, dates, mpedIdStr, collDate);
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                dataListFinal.add("00");//未分析
                dataListFinal.add(src);//自动抄表
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
                if (DateFilter.isLateWeek(dates, -45)) {
                    dataListFinal = null;
                }
            }
        } else if ("71501".equals(businessDataitemId)) {//电表开盖总次数
            dataListFinal.add(mpedIdStr);//
            dataListFinal.add(businessDataitemId);//
            dataListFinal.add(terminalArchivesObject.getTerminalId());//
            dataListFinal.add(String.valueOf(dataList.get(1)).trim());
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(new Date());//入库时间
        }
        if (dataListFinal == null || dataListFinal.isEmpty()) {
            return;
        }
        Object[] refreshKey = null;
        if (Integer.valueOf(src) % 10 == 0) {
            refreshKey = CommonUtils.refreshKey(terminalArchivesObject.getTerminalId(), mpedIdStr, DateUtil.parse(DateUtil.format(dates), DateUtil.defaultDatePattern_YMD), businessDataitemId, protocolId);
        }
        CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataListFinal, refreshKey, listDataObj);

    }

    private static int valueOfInt(Object o) {
        return new BigDecimal(String.valueOf(o)).intValue();
    }
}
