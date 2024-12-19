package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class MeterEventDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(MeterEventDataProcessor.class);
    public static final Map<String, String> codeMap = new HashMap<>(16);

    static {
        codeMap.put("03110001", "E_MTER_EVENT_NO_POWER");//上一次掉电发生时刻结束时刻
        codeMap.put("03050001", "E_METER_EVENT_ALL_VOL_LOSE");//上一次全失压发生时刻、电流值、结束时刻
        codeMap.put("03300101", "E_METER_EVENT_CLEAR");//上一次电能表清零记录
        codeMap.put("03300301", "E_METER_EVENT_CLEAR_EVENT");//上一次事件清零记录
        codeMap.put("03300401", "E_METER_EVENT_CHEK_TIME");//上一次校时记录
        codeMap.put("03300901", "E_METER_EVENT_AP_GROUP_PRO");//上一次有功组合方式编程记录
        codeMap.put("03300A01", "E_METER_EVENT_RQ_GROUP_PRO");//上一次无功组合方式1编程记录
        codeMap.put("03300B01", "E_METER_EVENT_RQ_GROUP_PRO");//上一次无功组合方式2编程记录
        codeMap.put("03300C01", "E_METER_EVENT_SETTLEDAY_PRO");//上一结算日编程记录
        codeMap.put("03300D01", "E_METER_EVENT_OPEN_LID");//上一次开表盖事件记录
        codeMap.put("03300D02", "E_METER_EVENT_OPEN_LID");//上2次开表盖事件记录
        codeMap.put("03300D03", "E_METER_EVENT_OPEN_LID");//上3次开表盖事件记录
        codeMap.put("03300D04", "E_METER_EVENT_OPEN_LID");//上4次开表盖事件记录
        codeMap.put("03300D05", "E_METER_EVENT_OPEN_LID");//上5次开表盖事件记录
        codeMap.put("03300D06", "E_METER_EVENT_OPEN_LID");//上6次开表盖事件记录
        codeMap.put("03300D07", "E_METER_EVENT_OPEN_LID");//上7次开表盖事件记录
        codeMap.put("03300D08", "E_METER_EVENT_OPEN_LID");//上8次开表盖事件记录
        codeMap.put("03300D09", "E_METER_EVENT_OPEN_LID");//上9次开表盖事件记录
        codeMap.put("03300D0A", "E_METER_EVENT_OPEN_LID");//上10次开表盖事件记录
        codeMap.put("03300E01", "E_METER_EVENT_OPEN_TERM_LID");//上一次开端扭盖事件记录
        codeMap.put("03120101", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上一次正向有功需量超限记录
        codeMap.put("03120201", "E_METER_EVENT_RAP_DEMD_OVER_LI");//上一次反向有功需量超限记录
        codeMap.put("03120301", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上一次第一象限需量超限记录
        codeMap.put("03120401", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上一次第二象限需量超限记录
        codeMap.put("03120501", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上一次第三象限需量超限记录
        codeMap.put("03120601", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上一次第四象限需量超限记录
        codeMap.put("21000001", "E_METER_EVENT_CURR_RETURN");//上一次潮流反向记录内容
        codeMap.put("1801FF01", "E_METER_EVENT_LOSE_CURR");//上一次A相失流事件
        codeMap.put("1802FF01", "E_METER_EVENT_LOSE_CURR");
        codeMap.put("1803FF01", "E_METER_EVENT_LOSE_CURR");
        codeMap.put("1901FF01", "E_METER_EVENT_OVER_I");//上一次A相过流发生时刻
        codeMap.put("1902FF01", "E_METER_EVENT_OVER_I");
        codeMap.put("1903FF01", "E_METER_EVENT_OVER_I");
        codeMap.put("1A01FF01", "E_METER_EVENT_NO_CURR");//上一次A相断流发生时刻
        codeMap.put("1A02FF01", "E_METER_EVENT_NO_CURR");
        codeMap.put("1A03FF01", "E_METER_EVENT_NO_CURR");
        codeMap.put("1B01FF01", "E_METER_EVENT_P_RETURN");//电能表有功功率反向事件记录
        codeMap.put("1B02FF01", "E_METER_EVENT_P_RETURN");
        codeMap.put("1B03FF01", "E_METER_EVENT_P_RETURN");
        codeMap.put("1001FF01", "E_METER_EVENT_LOSE_VOL");//上一次A相失压
        codeMap.put("1002FF01", "E_METER_EVENT_LOSE_VOL");
        codeMap.put("1003FF01", "E_METER_EVENT_LOSE_VOL");
        codeMap.put("1101FF01", "E_METER_EVENT_LOW_VOL");//A相欠压 未测
        codeMap.put("1102FF01", "E_METER_EVENT_LOW_VOL");
        codeMap.put("1103FF01", "E_METER_EVENT_LOW_VOL");
        codeMap.put("1201FF01", "E_METER_EVENT_OVER_VOL");//A相过压 未测
        codeMap.put("1202FF01", "E_METER_EVENT_OVER_VOL");
        codeMap.put("1203FF01", "E_METER_EVENT_OVER_VOL");
        codeMap.put("1301FF01", "E_METER_EVENT_VOL_BREAK");//A相断相 未测
        codeMap.put("1302FF01", "E_METER_EVENT_VOL_BREAK");
        codeMap.put("1303FF01", "E_METER_EVENT_VOL_BREAK");
        codeMap.put("1400FF01", "E_METER_EVENT_REVE_PHASE");//上一次电压逆相序
        codeMap.put("1500FF01", "E_METER_EVENT_I_REVE_PHASE");//上一次电流逆向序 未测
        codeMap.put("1600FF01", "E_METER_EVENT_VOL_UNBALANCE");//上一次电压不平衡事件
        codeMap.put("2000FF01", "E_METER_EVENT_CUR_HIGH_UNBALAN");//上一次电流严重不平衡事件
        codeMap.put("1D00FF01", "E_METER_EVENT_TRIP");//上一次跳闸事件
        codeMap.put("1E00FF01", "E_METER_EVENT_SWITCH_ON");//上一次合闸事件
        codeMap.put("1F00FF01", "E_METER_EVENT_FACT_LOWER_LIMIT");//上一次总功率因数超下限事件
        codeMap.put("1700FF01", "E_METER_EVENT_CUR_UNBALANCE");//电能表A相电流不平衡事件记录
        codeMap.put("17000101", "E_METER_EVENT_CUR_UNBALANCE");//电能表B相电流不平衡事件记录
        codeMap.put("17001301", "E_METER_EVENT_CUR_UNBALANCE");//电能表C相电流不平衡事件记录
        codeMap.put("03300201", "E_METER_EVENT_CLEAR_DEMAND"); //电能表需量清零事件
        codeMap.put("03060001", "E_METER_EVENT_ASSI_POWER_SHUT");//电能表辅助电源掉电事件记录
        codeMap.put("03300701", "E_METER_EVENT_WEEKDAY_PRO");//上一次周休日编程记录
        codeMap.put("03301201", "E_METER_EVENT_ESAM_KEY_PRO");//上一次密钥更新记录
        codeMap.put("03301301", "E_METER_EVENT_INSERT_CARD");//上一次异常插卡记录
        codeMap.put("03300601", "E_METER_EVENT_TZONE_PRO");//上一时区表编程记录
        codeMap.put("03300501", "E_METER_EVENT_TFRAME_PRO");//上一时段表编程记录
        codeMap.put("03300F01", "E_METER_EVENT_RATE_PRO");//上一次费率参数表编程记录
        codeMap.put("03340001", "E_METER_EVENT_BACK_FEE");//上一次退费记录内容
        codeMap.put("03301001", "E_METER_EVENT_STAIR_PRO");//上一次阶梯表编程记录
        codeMap.put("1C01FF01", "E_METER_EVENT_OVER_LOAD");//电能表A相电能负荷过载记录
        codeMap.put("1C02FF01", "E_METER_EVENT_OVER_LOAD");
        codeMap.put("1C03FF01", "E_METER_EVENT_OVER_LOAD");
        codeMap.put("03300801", "E_METER_EVENT_HOLIDAY_PRO");//上一次节假日编程记录
        codeMap.put("03370001", "E_METER_EVENT_POWER_ABN");//电能表电能异常事件记录
        codeMap.put("03350001", "E_METER_EVENT_MAG_INTERF");//电能表恒定磁场干扰记录
        codeMap.put("03300001", "E_METER_EVENT_PROGRAM");//电能表编程事件记录
        codeMap.put("03360001", "E_METER_EVENT_SWITCH_ERR");//电能表负荷开关误扭动事件


        //		codeMap.put("null","E_METER_EVENT_CLOCK_FAULT");//电能表时钟故障事件记录
        //		codeMap.put("null","E_METER_EVENT_MEASURE_CHIP");//电能表计量芯片故障事件记录
        //		codeMap.put("null","E_METER_EVENT_BUY_ENERGY");//电能表购电记录
        //		codeMap.put("03300201","E_METER_EVENT_RQ_DEMD_OVER_LIM");//电能表无功需量越限记录
    }

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis电能表事件数据进行处理
        logger.info("===GET METER EVENT DATA===");
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
                String areaCode = terminalDataObject.getAreaCode();
                int terminalAddr = terminalDataObject.getTerminalAddr();
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();// 数据类
                List dioList = terminalDataObject.getList();//测量点集合
                if ((dioList != null) && (dioList.size() != 0)) {
                    Iterator iterator1 = dioList.iterator();
                    while (iterator1.hasNext()) {//测量点集合
                        DataItemObject dio = (DataItemObject) iterator1.next();//一个测量点
                        List dataList = dio.getList();//一个电表事件list
                        if ((dataList != null) && (dataList.size() != 0)) {
                            Iterator it = dataList.iterator();
                            List baseInfoList = new ArrayList();
                            while (it.hasNext()) {//遍历事件信息list
                                Object data = it.next();
                                if (data instanceof List) {
                                    List inList = (List) data;
                                    String dataSign = (String) inList.get(1);//获取数据标识
                                    if (!codeMap.containsKey(dataSign)) {
                                        continue;
                                    }

                                    String comm = String.valueOf(baseInfoList.get(1));
                                    comm = MeterEventConcat.reverseComm(comm);
                                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMMidArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + comm, "MI" + comm);
                                    if (terminalArchivesObject == null) {
                                        throw new RuntimeException("无法从缓存获取正确的档案信息！！！！");
                                    }
                                    try {
                                        String meterId = terminalArchivesObject.getMeterId();
                                        String mpedId = terminalArchivesObject.getID();
                                        String orgNo = terminalArchivesObject.getPowerUnitNumber();
                                        //组装事件队列

                                        List dataListFinal = getListData(inList, meterId, mpedId, orgNo, dataSign);

                                        if (dataListFinal == null) continue;
                                        if (!ParamConstants.startWith.equals("11")){
                                            if(dataSign.equals("03110001")){
                                                Object checkDate=dataListFinal.get(dataListFinal.size()-1);
                                                boolean err=DateFilter.betweenDay(checkDate,0,90);
                                                if(!err){
                                                    dataSign="no_power_err";
                                                }
                                            }

                                        }
                                        String businessDataitemId = dataSign;

                                        CommonUtils.putToDataHub(businessDataitemId,meterId,dataListFinal,null,listDataObj);

                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                } else if (data instanceof String) {
                                    baseInfoList.add(data.toString());
                                }
                            }
                        }
                    }
                    PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
                    persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
                    list.add(persistentObject);//这个完整报文添加到了持久化list了
                    context.setContent(list);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 组装事件队列
     *
     * @param inLIst
     * @param meterId
     * @param mpedId
     * @param orgNo
     * @param dataSign
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private static List<Object> getListData(List<Object> inLIst, String meterId, String mpedId, String orgNo, String dataSign)
            throws Exception {
        List<Object> realData = null;
        if (inLIst.get(2) instanceof List) {
            realData = (List<Object>) inLIst.get(2);
        } else {
            return null;
        }
        if (realData.size() == 0) {
            return null;
        }
        List<Object> valueList = new ArrayList<Object>();
        valueList.add(BigDecimal.valueOf(Long.parseLong(meterId)));// 电能表ID METER_ID
        valueList.add(new Date());// 入库时间 INPUT_TIME
        valueList.add(orgNo);// 供电单位 ORG_NO
        String codeVal = codeMap.get(dataSign);//电能表需量清零事件需要测量点id
        if (codeVal == null) return null;
        if (codeVal.equals("E_METER_EVENT_CLEAR_DEMAND")) {
            valueList.add(mpedId);// 测量点ID mpedId
        }
        MeterEventConcat.fillData(valueList, dataSign, realData,codeVal);
        if (valueList == null) {
            return null;
        }
        valueList.add(orgNo.substring(0, 5));
        if(!ParamConstants.startWith.equals("11")) {
            Date date = new Date();
            if (codeVal.equals("E_MTER_EVENT_NO_POWER")) {
                date = (Date) valueList.get(valueList.size() - 2);
            }
            valueList.add(DateUtil.format(date, DateUtil.defaultDatePattern_YMD));

        }else {
            valueList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
        }
        return valueList;
    }

}
