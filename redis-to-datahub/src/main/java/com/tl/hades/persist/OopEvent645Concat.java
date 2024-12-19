package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by huangchunhuai on 2021/7/28.
 */
public class OopEvent645Concat {
    private final static Logger logger = LoggerFactory.getLogger(OopEvent645Concat.class);

    private static final Map<String, String> codeMap = new HashMap<>();
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
        codeMap.put("71501", "E_EVENT_METER_TOTAL_NUM_SOURCE");//开盖次数
        codeMap.put("03300D01", "E_METER_EVENT_OPEN_LID_SOURCE");//开盖事件
        codeMap.put("erc56", "E_EVENT_ERC56_SOURCE");//电表停上电事件
    }
    private final static String redisName = "Q_BASIC_DATA_50040200_R";
    /**
     * 645事件
     *
     * @param dataitemID
     * @param tao
     * @param cj
     * @return
     * @throws Exception
     */
    public static List<Object> getEventDateList(String dataitemID, TerminalArchivesObject tao, Object... cj) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String codeVal = codeMap.get(dataitemID);
        if (codeVal == null) return null;
        List<Object> dataList = new ArrayList<>();
        String meterId = tao.getMeterId();
        String mpedId = tao.getID();
        String orgNo = tao.getPowerUnitNumber();
        List<Object> objList = (List<Object>) cj[0];
        switch (codeVal) {
            case "E_EVENT_METER_TOTAL_NUM_SOURCE":
                if (mpedId == null) {
                    return null;
                }
                dataList.add(mpedId);
                dataList.add(BigDecimal.valueOf(Long.parseLong(dataitemID)));
                dataList.add(tao.getTerminalId());
                dataList.add(BigDecimal.valueOf(Long.parseLong(objList.get(0).toString())));
                dataList.add(tao.getPowerUnitNumber().substring(0, 5));
                dataList.add(new Date());
                break;
            case "E_METER_EVENT_OPEN_LID_SOURCE":
                if (meterId == null) {
                    return null;
                }
                List dList = (List) objList.get(0);
                dataList.add(meterId);//METER_ID:BIGINT:电能表ID
                dataList.add(new Date());//INPUT_TIME:DATETIME:入库时间，yyyy-mm-ddhh24：mi：ss
                dataList.add(orgNo);//ORG_NO:VARCHAR:供电单位
                Date stTime = sdf.parse(String.valueOf(dList.remove(0)));
                Date edTime = sdf.parse(String.valueOf(dList.remove(0)));
                dataList.add(stTime);//EVENT_ST:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                dataList.add(edTime);//EVENT_ET:DATETIME:事件发生时刻，yyyy-mm-ddhh24:mi:ss
                for (int i = 0, j = dList.size(); i < j; i++) {
                    Object v = dList.get(i);
                    if (v != null && v.toString().contains("F")) {
                        v = null;
                    }
                    dataList.add(v);
                }
                dataList.add(0);//SORT_NO:INT:事件序次
                dataList.add("");//EXTEN_INFO:VARCHAR:事件扩展信息，面向对象规约包含不确定的信息，采用键值对字符串方式存放：内容OAD编号#值，几个键值对之间用分号隔开。
                dataList.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                dataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//EVENT_DATE:DATE:事件日期
                break;
            default:
        }
        return dataList;
    }
}
