package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FreezeOtherDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(FreezeOtherDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET FREEZE OTHER DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class<?> clazz = ParamConstants.classMap.get(protocolId);
            if (null == clazz) {
                throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
            }
            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
                int afn = terminalDataObject.getAFN();
                String areaCode = terminalDataObject.getAreaCode();
                int terminalAddr = terminalDataObject.getTerminalAddr();
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    List dataList = data.getList();//本测量点的数据项xxx.xx...
                    boolean allNull = CommonUtils.allNull(dataList);
                    if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
                        continue;
                    }
                    int pn = data.getPn();
                    int fn = data.getFn();
                    logger.info("===MAKE " + afn + "_" + fn + " DATA===");

                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    protocol3761ArchivesObject.setAfn(afn);
                    protocol3761ArchivesObject.setFn(fn);
                    String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
                    String cleadDi=businessDataitemId;
                    //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance()
                            .getOtherTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
                    String mpedIdStr = terminalArchivesObject.getID();
                    if (null == mpedIdStr || "".equals(mpedIdStr)) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                        continue;
                    }

                    boolean isDayRead = true;
                    if (fn == 222) {
                        isDayRead = false;
                    }
                    List<Object> dataListFinal = new ArrayList<>();

                    List<Object> sublists = getDataList(data.getList(), mpedIdStr, isDayRead, businessDataitemId);
                    if (sublists == null || sublists.size() == 0) {
                        continue;
                    }
                    businessDataitemId = sublists.get(sublists.size() - 1).toString();
                    if (businessDataitemId.contains("HEAT")) {
                        dataListFinal = sublists.subList(0, sublists.size() - 5);
                    } else if (businessDataitemId.contains("GAS")) {
                        dataListFinal = sublists.subList(0, sublists.size() - 4);
                    } else if (businessDataitemId.contains("WATER")) {
                        dataListFinal = sublists.subList(0, sublists.size() - 4);
                    }
                    dataListFinal.add(new Date());
                    dataListFinal.add("0");
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));

                    Object[] refreshKey = CommonUtils.refreshKey(terminalArchivesObject.getTerminalId(), mpedIdStr, DateUtil.parse(dataListFinal.get(1).toString()), cleadDi, protocolId);

                    DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
                    int index = (int) (Long.valueOf(mpedIdStr) % dataHubTopic.getShardCount());
                    String shardId = dataHubTopic.getActiveShardList().get(index);
                    DataObject dataObj = new DataObject(dataListFinal, refreshKey, dataHubTopic.topic(), shardId.toString());//给这个数据类赋值:list key classname (fn改为161)
                    listDataObj.add(dataObj);//将组好的数据类添加到数据列表
                }
                PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                list.add(persistentObject);//这个完整报文添加到了持久化list了
                context.setContent(list);
                return false;
            }
        }
        return true;
    }

    /**
     * 修整数据
     * 召测返回结果：
     * ********final list values*********  2015-10-12 00:00:00  		********heat:::
     * *******final list values*********  2015-10-13 08:00:00  	    value ==== Wed Mar 23 00:00:00 CST 2016
     * *******final list values*********  48                   		value ==== Wed Mar 23 07:55:00 CST 2016
     * *******final list values*********  000002.00 (m3)       		value ==== 32
     * *******final list values*********  000002.00 (m3)       		value ==== 019062.47 (kWh)  结算日热量
     * *******final list values*********  EEEEEE.EE (EE)       		value ==== 019062.47 (kWh)  当前热量
     * *******final list values*********  EEEE.EEEE (EE)       		value ==== 000000.00 (kW)   热功率
     * *******final list values*********  EEEEEE.EE (EE)       		value ==== 0000.0000 (m3/h) 流量
     * *******final list values*********  null                 		value ==== 004021.08 (m3)   累积流量
     * *******final list values*********  null                 		value ==== 1.455E7          供水温度
     * *******final list values*********  101308.0             		value ==== 1.452E7          回水温度
     * *******final list values*********  null                 		value ==== 20966.0          累积工作时间
     * *******final list values*********  9                    		value ==== 2016-03-23 07:55:00    实时时间
     * ****************water&gas*****************                       		value ==== 0000000000000000
     */
    private static List<Object> getDataList(List<Object> oriList, String mpedId, boolean isDayRead, String buid) {
        List<Object> dataList = new ArrayList<Object>();
        dataList.add(mpedId); //MPED_ID
        dataList.add(DateUtil.format((Date) oriList.get(0), DateUtil.defaultDatePattern_YMD)); //数据日期
        dataList.add(oriList.get(1)); //抄表时间
        int meterType = Integer.valueOf(String.valueOf(oriList.get(2))); //表类型
        String[] tearVal = String.valueOf(oriList.get(3)).split(" ");
        if (meterType >= 16 && meterType <= 25) {//shui
            buid = buid + "_WATER";
        } else if (meterType >= 48 && meterType <= 57) {//qi
            buid = buid + "_GAS";
        } else if (meterType >= 32 && meterType <= 41) {//re
            buid = buid + "_HEAT";
        } else {
            return null;
        }
        if (meterType >= 32 && meterType <= 41) { //热表
            tearVal = String.valueOf(oriList.get(4)).split(" "); //当前热量
        }
        if (tearVal[1] != null && (!tearVal[1].equals("(m3)") || tearVal[1].equals("(kWh)"))) {
            if (tearVal[0] != null && tearVal[0].contains("E")) {
                tearVal[0] = "0";
            }
            if (tearVal[0] == null) {
                dataList.add(null);
            } else {
                dataList.add((Double.valueOf(tearVal[0])));
            }
        } else if (tearVal[1] != null) {
            if (tearVal[0] != null && tearVal[0].contains("E")) {
                tearVal[0] = "0";
            }
            if (tearVal[0] == null) {
                dataList.add(null);
            } else {
                dataList.add(Double.valueOf(tearVal[0]));
            }
        }
        String[] setVal = String.valueOf(oriList.get(4)).split(" ");
        if (meterType >= 32 && meterType <= 41) { //热表
            setVal = String.valueOf(oriList.get(3)).split(" "); //结算日热量
        }
        if (setVal[1] != null && (!setVal[1].equals("(m3)") || tearVal[1].equals("(kWh)"))) {
            if (setVal[0] != null && setVal[0].contains("E")) {
                setVal[0] = "0";
            }
            if (setVal[0] == null) {
                dataList.add(null);
            } else {
                dataList.add(Double.valueOf(setVal[0]));
            }
        } else if (setVal[1] != null) {
            if (setVal[0] != null && setVal[0].contains("E")) {
                setVal[0] = "0";
            }
            if (setVal[0] == null) {
                dataList.add(null);
            } else {
                dataList.add(Double.valueOf(setVal[0]));
            }
        }
        if (meterType >= 32 && meterType <= 41) {
            String[] heatPu = String.valueOf(oriList.get(5)).split(" "); //热功率
            if (heatPu[1] != null && heatPu[1].equals("(kW)")) {
                if (heatPu[0] != null && heatPu[0].contains("E")) {
                    heatPu[0] = "0";
                }
                if (heatPu[0] == null) {
                    dataList.add(null);
                } else {
                    dataList.add(Double.valueOf(heatPu[0]));
                }
            } else if (heatPu[1] != null) {
                if (heatPu[0] != null && heatPu[0].contains("E")) {
                    heatPu[0] = "0";
                }
                if (heatPu[0] == null) {
                    dataList.add(null);
                } else {
                    dataList.add(Double.valueOf(heatPu[0]));
                }
            }
            String[] heatFlow = String.valueOf(oriList.get(6)).split(" "); //流量
            if (heatFlow[0] != null && heatFlow[0].contains("E")) {
                heatFlow[0] = "0";
            }
            if (heatFlow[0] == null) {
                dataList.add(null);
            } else {
                dataList.add(Double.valueOf(heatFlow[0]));
            }
            String[] heatFlowSum = String.valueOf(oriList.get(7)).split(" "); //积累流量
            if (heatFlowSum[0] != null && heatFlowSum[0].contains("E")) {
                heatFlowSum[0] = "0";
            }
            if (heatFlowSum[0] == null) {
                dataList.add(null);
            } else {
                dataList.add(Double.valueOf(heatFlowSum[0]));
            }
            if (oriList.get(8) == null || String.valueOf(oriList.get(8)).contains("EE")) {
                dataList.add(Double.valueOf("0"));
            } else {
                String supplyTempr = String.valueOf(oriList.get(8)); //供水温度
                String[] stVal = supplyTempr.split("\\.");
                if (stVal[1].length() > 2) {
                    supplyTempr = stVal[0] + "." + stVal[1].substring(0, 2);
                }
                dataList.add(Double.valueOf(supplyTempr));
            }
            if (oriList.get(9) == null || String.valueOf(oriList.get(9)).contains("EE")) {
                dataList.add(Double.valueOf("0"));
            } else {
                String reverseTempr = String.valueOf(oriList.get(9)); //回水温度
                String[] rtVal = reverseTempr.split("\\.");
                if (rtVal[1].length() > 2) {
                    reverseTempr = rtVal[0] + "." + rtVal[1].substring(0, 2);
                }
                dataList.add(Double.valueOf(reverseTempr));
            }
            String statusRaw = oriList.get(oriList.size() - 1).toString(); //状态
            String status = statusRaw.substring(0, 8);
            String[] statArr = getStatus(status);
            dataList.add(statArr[0]);
            dataList.add(statArr[1]);
            dataList.add(String.valueOf(oriList.get(10))); //累积工作时间
            String realDateStr = String.valueOf(oriList.get(11)); //实时时间
            if (realDateStr.contains("EE")) {
                dataList.add(new Date());
            } else {
                try {
                    dataList.add((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(realDateStr)));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
//			dataList.add("00");
        } else {
            String realDateStr = String.valueOf(oriList.get(oriList.size() - 2)); //实时时间
            if (realDateStr.contains("EE")) {
                dataList.add(new Date());
            } else {
                try {
                    dataList.add(new java.sql.Date((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(realDateStr)).getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                    dataList.add(null);
                }
            }
            String statusRaw = oriList.get(oriList.size() - 1).toString();
            String status = statusRaw.substring(0, 8);
            String[] statArr = getStatus(status);
            dataList.add(statArr[0]);
            dataList.add(statArr[1]);
//			dataList.add("00");
        }
        dataList.add(buid);
        return dataList;
    }

    private static String[] getStatus(String statStr) {
        String[] statArr = new String[2];
        String switchStat = statStr.substring(6, 8);
        String batteryStat = String.valueOf(statStr.charAt(5));
        statArr[0] = switchStat;
        statArr[1] = batteryStat;
        return statArr;
    }

    private boolean nullDataCheck(List<Object> dataList) {
        boolean isNull = false;
        if (dataList != null && dataList.size() > 0) {
            if (dataList.get(3).toString().contains("EE") && dataList.get(4).toString().contains("EE")) {
                isNull = true;
            }
        }
        return isNull;
    }

}