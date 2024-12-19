package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.DateFilter;
import com.tl.hades.persist.EventUtils;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 日冻结前台点
 *
 * @author easb
 * oop-freeze-front-dataprocessor.xml
 */
public class OopFreezeFrontDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = Logger.getLogger(OopFreezeFrontDataProcessor.class);

    private final static String redisName = "Q_BASIC_DATA_50040200_DAY_R";


    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings("all")
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET OOP FREEZE FRONT DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            int protocolId = ternData.getCommandType();
            if (protocolId != 8) {
                protocolId = 9;
            }
            List<com.tl.hades.persist.PersistentObject> list = new ArrayList<com.tl.hades.persist.PersistentObject>();//持久类
            List<com.tl.hades.persist.DataObject> listDataObj = new ArrayList<com.tl.hades.persist.DataObject>();//数据类

            List<MeterData> meterList = ternData.getMeterDataList();

            //A相电压业务数据项
            String businessDataitemIdA = null;

            for (MeterData meterData : meterList) {
                List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
                if (doList == null || doList.size() == 0) {
                    continue;
                }


                //ABC三相电压合格率
                List<Object> dataListABC = new ArrayList<Object>();
                List dataList = null;
                String meterAddr = null;
                String businessDataitemId = null;
                Date dates = null;//存储时标
                Date collDate = null;//采集成功时标
                String oopDataItemId = null;
                List listA = new ArrayList<>();
                List listB = new ArrayList<>();
                List listC = new ArrayList<>();

                for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
                    try {
                        if (dataObject.getDataItem().equals("60420200")) {
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
                        } else if ("21310201".equals(dataObject.getDataItem())) {
                            oopDataItemId = dataObject.getDataItem();
                            if (OopDataUtils.notNull(dataObject.getData(), List.class)) {
                                listA = (List) dataObject.getData();
                            }
                        } else if ("21320201".equals(dataObject.getDataItem())) {
                            if (OopDataUtils.notNull(dataObject.getData(), List.class)) {
                                listB = (List) dataObject.getData();
                            }
                        } else if ("21330201".equals(dataObject.getDataItem())) {
                            if (OopDataUtils.notNull(dataObject.getData(), List.class)) {
                                listC = (List) dataObject.getData();
                            }
                            dataList = listC;
                        } else {
                            oopDataItemId = dataObject.getDataItem();//数据项
                            if (dataObject.getData() != null) {
                                if (dataObject.getData() instanceof List) {
                                    dataList = (List) dataObject.getData();
                                } else {
                                    dataList = new ArrayList();
                                    dataList.add(String.valueOf(dataObject.getData()));
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
                if (oopDataItemId == null || collDate == null || meterAddr == null || dataList == null || dates == null) {
                    continue;
                }
                //获取前置写入redis中的真实表地址
                String realAddr = TerminalArchives.getInstance().getRealMeterAddrFromLocalOop(areaCode, termAddr, meterAddr);
                //拼接终端地址用于后续查找档案
                String full = areaCode + termAddr;
                String realAc = full.substring(3, 7);
                String realTd1 = full.substring(7, 12);
                String realTd = String.valueOf(Integer.parseInt(realTd1));
                //获取档案信息
                TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesOop(realAc, realTd, realAddr);
                String mpedIdStr = terminalArchivesObject.getID();
                if (null == mpedIdStr || "".equals(mpedIdStr)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + realAc + "_" + realTd + "_COMMADDR" + realAddr);
                    continue;
                }//2340901  Q_BASIC_DATA_50040200_DAY_R8_01010001
                Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                businessDataitemId = TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject, redisName, oopDataItemId);
                if (businessDataitemId == null) {
                    logger.error("无法从缓存获取正确的业务数据项ID:" + redisName + "_" + protocolId + "_" + oopDataItemId);
                    continue;
                }
                List<Object> dataListFinal = new ArrayList<Object>();
                if (oopDataItemId.startsWith("2")) {//A、B、C三相电压合格率

                    List ListABC = new ArrayList<>();
                    dataListABC.add(mpedIdStr);
                    for (int i = 1; i < 40; i++) {
                        if (i < 16) {
                            if (i == 3) {
                                dataListABC.add(valueOfInt(listA.get(3)));//A相电压超上限时间3
                            } else if (i == 5) {
                                dataListABC.add(valueOfInt(listA.get(0)));//A相电压合格累计时间5
                            } else if (i == 8) {
                                dataListABC.add(valueOfInt(listB.get(3)));
                                //B相电压超上限时间3
                            } else if (i == 10) {
                                dataListABC.add(valueOfInt(listB.get(0)));
                                //B相电压合格累计时间5
                            } else if (i == 13) {
                                dataListABC.add(valueOfInt(listC.get(3)));
                                //C相电压超上限时间3
                            } else if (i == 15) {
                                dataListABC.add(valueOfInt(listC.get(0)));
                                //C相电压合格累计时间5
                            } else {
                                dataListABC.add(0);
                            }
                        } else if (i == 37) {
                            dataListABC.add(valueOfInt(listA.get(1)));//A相电压合格率37
                        } else if (i == 38) {
                            dataListABC.add(valueOfInt(listB.get(1)));//B相电压合格率37
                        } else if (i == 39) {
                            dataListABC.add(valueOfInt(listC.get(1)));//C相电压合格率37
                        } else {
                            dataListABC.add(null);
                        }
                    }
                    dataListABC.add(terminalArchivesObject.getPowerUnitNumber());//供电单位
                    dataListABC.add("00");//未分析
                    //区分直抄137 8/预抄133 9
                    if (protocolId == 8) {
                        dataListABC.add("24");//直抄
                    } else {
                        dataListABC.add("4");//预抄
                    }
                    dataListABC.add(new Date());//入库时间
                    dataListABC.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataListABC.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
                    DataHubTopic dataHubTopic = new DataHubTopic("235005");
                    int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
                    String shardId = dataHubTopic.getActiveShardList().get(index);
                    com.tl.hades.persist.DataObject dataObj = new com.tl.hades.persist.DataObject(dataListABC, null, dataHubTopic.topic(), shardId);//给这个数据类赋值:list key classname (fn改为161)
                    listDataObj.add(dataObj);//将组好的数据类添加到数据列表
                } else {
                    if (oopDataItemId.startsWith("1")) {
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
                        //区分直抄137 8/预抄133 9
                        if (protocolId == 8) {
                            dataListFinal.add("24");//直抄
                        } else {
                            dataListFinal.add("4");//预抄
                        }
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
                        //区分直抄137 8/预抄133 9
                        if (protocolId == 8) {
                            dataListFinal.add("24");//直抄
                        } else {
                            dataListFinal.add("4");//预抄
                        }
                        dataListFinal.add(new Date());//入库时间
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                        dataListFinal.add(DateUtil.format(DateFilter.getPrevMonthLastDay(dates), DateUtil.defaultDatePattern_YMD));
                    } else {
                        boolean allNull = OopDataUtils.allNull(dataList);
                        if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                            continue;
                        }
                        if (dataList.get(0) == null) {
                            continue;
                        }
                        String dataSrc = "4";
                        if (protocolId == 8) {
                            dataSrc = "24";
                        }
                        dataListFinal = OopDataUtils.getDataList(null, dataList, oopDataItemId, dates, mpedIdStr, collDate);
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                        dataListFinal.add("00");//未分析
                        //区分直抄8/预抄9
                        dataListFinal.add(dataSrc);//直抄
                        dataListFinal.add(new Date());//入库时间3531778244
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                        dataListFinal.add(DateUtil.format(dates, DateUtil.defaultDatePattern_YMD));
                        if (DateFilter.isLateWeek(dates, -45)) {
                            dataListFinal = null;
                        }
                    }
                    if (dataListFinal == null || dataListFinal.isEmpty()) {
                        continue;
                    }
                    DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
                    int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
                    String shardId = dataHubTopic.getActiveShardList().get(index);
                    com.tl.hades.persist.DataObject dataObj = new com.tl.hades.persist.DataObject(dataListFinal, null, dataHubTopic.topic(), shardId);//给这个数据类赋值:list key classname (fn改为161)
                    listDataObj.add(dataObj);//将组好的数据类添加到数据列表
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

    private int valueOfInt(Object o) {
        return new BigDecimal(String.valueOf(o)).intValue();
    }
}