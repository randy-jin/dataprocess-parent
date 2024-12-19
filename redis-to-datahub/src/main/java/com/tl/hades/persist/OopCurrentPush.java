package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author Dong.wei-CHEN
 * Date 2019/12/27 18:06
 * @Descrintion 面向对象实时公共
 */
public class OopCurrentPush {


    private static Map<String, String> keyMap = new HashMap<>(16);

    static {
        keyMap.put("F2090800", "E_TMNL_NEIGHBOUR_NET_INFO_SOURCE");
        keyMap.put("F2091400", "E_TMNL_REAL_NETWORK_SIZE_SOURCE");
        keyMap.put("null", "E_IP_SEARCH_INFO_SOURCE");
    }


    public static List getDataListFinal(List dataList, Object... obj) {

        List dataListFinal = new ArrayList();
        String dataItemId = (String) obj[0];
        String topicName = keyMap.get(dataItemId);
        switch (topicName) {
            //邻居网络信息
            case "E_TMNL_NEIGHBOUR_NET_INFO_SOURCE":
                List<List> lists = new ArrayList<>();
                Object obList = dataList.get(2);
                if (obList == null || !(obList instanceof List)) {
                    return null;
                }
                List obs = (List) obList;
                if (obs.size() == 0) return null;
                for (int i = 0; i < obs.size(); i++) {
                    dataListFinal = new ArrayList();
                    dataListFinal.add(obj[1]);//TERMINAL_ID:BIGINT:终端ID
                    dataListFinal.add(obs.get(i));//NEIGHBOUR_NET_ID:INT:邻居节短网络标识号
                    dataListFinal.add(i + 1);//POINT_INDEX:VARCHAR:邻居节点序号
                    dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                    dataListFinal.add(obs.size());//POINT_NUM:INT:多网络节点总数量
                    dataListFinal.add(dataList.get(0));//LOCAL_NET_ID:VARCHAR:本节点短网络标识号
                    dataListFinal.add(dataList.get(1));//LOCAL_MAIN_POINT_ADDR:VARCHAR:本节点主节点地址
                    dataListFinal.add(((String) obj[3]).substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                    dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                    lists.add(dataListFinal);
                }
                return lists;
            case "E_TMNL_REAL_NETWORK_SIZE_SOURCE":
                Object size = dataList.get(0);
                dataListFinal.add(obj[1]);//TERMINAL_ID:BIGINT:终端ID
                dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                if (size instanceof Long) {
                    dataListFinal.add(BigDecimal.valueOf((Long) size));//NETWORK_SIZE:INT:网络规模
                } else {
                    dataListFinal.add(BigDecimal.valueOf((Integer) size));
                }
                dataListFinal.add(((String) obj[3]).substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                break;
            case "E_IP_SEARCH_INFO_SOURCE":
                dataListFinal.add(obj[1]);//TERMINAL_ID:BIGINT:终端ID
                dataListFinal.add("");//TMNL_BAR_CODE:VARCHAR:集中器资产编号
                dataListFinal.add("");//TMNL_ADDR:VARCHAR:集中器地址
                dataListFinal.add("");//COMM_ADDR:VARCHAR:电表地址
                dataListFinal.add("");//COMM_PROTOCOL:VARCHAR:电表规约
                dataListFinal.add("");//FMR_ADDR:VARCHAR:采集器地址
                dataListFinal.add("");//MEASURE_NO:VARCHAR:测量点号
                dataListFinal.add("");//REPORT_TIME:DATETIME:上报时间
                dataListFinal.add("");//MEMO:VARCHAR:可记录上报的原始关键信息
                dataListFinal.add("");//SEARCH_TIME:DATETIME:搜索到电表时刻
                dataListFinal.add("");//INDEX_NUM:TINYINT:搜索序号
                dataListFinal.add("");//SIGNAL_QUAL:DECIMAL:信号质量
                dataListFinal.add("");//COMM_PHASE:VARCHAR:1：A，2：B，3：C
                dataListFinal.add("");//DATA_DATE:DATE:数据日期
                dataListFinal.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                dataListFinal.add(((String) obj[3]).substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                break;
            default:

        }
        return dataListFinal;
    }


}
