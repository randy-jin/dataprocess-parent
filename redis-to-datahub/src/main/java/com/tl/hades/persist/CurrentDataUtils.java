package com.tl.hades.persist;

import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.ls.pf.base.utils.tools.StringUtils;
import com.tl.easb.utils.DateUtil;

import java.math.BigDecimal;
import java.util.*;

public class CurrentDataUtils {

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List getDataList(List dataList, int afn, int fn) {
        if (afn == 12) {
            if (fn == 2) {
                return dataList;
            }
        } else if (afn == 10) {
            if (fn == 10) {
                for (Object o : dataList) {
                    List list = new ArrayList();
                    for (Object oo : (List) o) {
                        List listValue = new ArrayList();
                        for (Object ooo : (List) oo) {
                            if (ooo instanceof byte[]) {
                                listValue.add(StringUtils.encodeHex((byte[]) ooo));
                            } else {
                                listValue.add(ooo);
                            }
                        }
                        Map<Integer, Integer> protocolmap = new HashMap<Integer, Integer>();
                        protocolmap.put(1, 2);
                        protocolmap.put(30, 3);
                        protocolmap.put(2, 5);
                        protocolmap.put(31, 6);
                        protocolmap.put(32, 32);
                        protocolmap.put(100, 100);
                        protocolmap.put(3, 8);
                        Map<Integer, String> userflagmap = new HashMap<Integer, String>();
                        userflagmap.put(0, "01");
                        userflagmap.put(1, "02");
                        userflagmap.put(2, "03");
                        userflagmap.put(3, "04");
                        userflagmap.put(4, "05");
                        userflagmap.put(5, "06");
                        listValue.set(2, "0" + listValue.get(2));
                        listValue.set(4, protocolmap.get(listValue.get(4)));
                        listValue.set(8, (int) (listValue.get(8)) + 4);
                        listValue.set(9, (int) (listValue.get(9)) + 1);
                        listValue.set(11, userflagmap.get(listValue.get(11)));

                        list.add(listValue);
                    }
                    return list;
                }
            }
        }
        return dataList;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List getDataList(List dataList, int afn, int fn, TerminalArchivesObject terminalArchivesObject) { //fnpn测量点的数据项list，
        List newDataList = null;
        String mpedId = terminalArchivesObject.getID();
        if (afn == 12) {
            if (fn >= 129 && fn <= 136) {
                newDataList = new ArrayList();
                newDataList.add(1);//从缓存取测量点标识，这里占位
                if (fn == 129)
                    newDataList.add("1");//1正向有功
                else if (fn == 130)
                    newDataList.add("2");//正向无功
                else if (fn == 131)
                    newDataList.add("5");//反向有功
                else if (fn == 132)
                    newDataList.add("6");//反向无功
                else if (fn == 133)
                    newDataList.add("3");//一象限无功
                else if (fn == 134)
                    newDataList.add("4");//二象限无功
                else if (fn == 135)
                    newDataList.add("7");//三象限无功
                else if (fn == 136)
                    newDataList.add("8");//四象限无功
                else
                    newDataList.add("0");//备用

                int m = (Integer) dataList.remove(1);//费率个数m  被删除后，后面的往前平移1
                List list = (List) ((List) dataList.get(2)).get(0);//m个 (m1的正向有功电能示值,m2,m3,m4)
                Object colDate = dataList.get(0);
                if (null != colDate && colDate instanceof Date) {
                    newDataList.add(colDate);
                }
                newDataList.add(dataList.get(1)); //正向有功总电能示值
                newDataList.addAll(list);//4个值的list，一次性添加
                for (int i = m; i < 14; i++) {//补充14-m个正向有功电能示值
                    newDataList.add(null);
                }
                newDataList.add(1, mpedId);//首元素id
                // 把数据时标放在最后，方便外层处理
                Object dataDate = colDate;
                if (null != dataDate && dataDate instanceof Date) {
                    newDataList.add(DateUtil.format(DateUtil.addDaysOfMonth((Date) dataDate, -1), DateUtil.defaultDatePattern_YMD)); //日冻结类数据时标Td_d(数据日期)，取当前采集数据日期-1
                }
                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
            } else if (fn == 246) {//当前掉电记录数据
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(null);//终端id占位
                newDataList.add(dataList.get(0));
                for (int i = 1; i < dataList.size(); i++) {
                    newDataList.add(dataList.get(i));
                }
            } else if (fn == 167) {//购用电信息
                newDataList = new ArrayList();
                newDataList.add(mpedId);
                newDataList.add(DateUtil.format((Date) dataList.get(0), DateUtil.defaultDatePattern_YMD));//dataDate
                newDataList.add(dataList.get(0));//抄表日期
                newDataList.add(null);//orgNo占位
                newDataList.add(dataList.get(4));//剩余电量
                newDataList.add(dataList.get(2));//剩余金额
                newDataList.add(dataList.get(8));//报警电量
                newDataList.add(dataList.get(9));//故障电量
                newDataList.add(dataList.get(6));//累计购电量
                newDataList.add(dataList.get(3));//累计购电金额
                newDataList.add(((Double) dataList.get(1)).intValue());//购电次数
                newDataList.add(dataList.get(7));//赊欠门限值
                newDataList.add(dataList.get(5));//透支电量
                //当前三相及总有/无功功率，功率因数，三相电压，电流，零序电流，视在功率
            } else if (fn == 25) {
                List<List> obList = new ArrayList<>();
                Date colTime = (Date) dataList.get(0);
                String eventDate = DateUtil.format(colTime, DateUtil.defaultDatePattern_YMD);
                if (dataList.size() != 24) {
                    return null;
                }
                Map<String, List> bodyMap = new HashMap<>(6);
                bodyMap.put("active_power", dataList.subList(1, 5));
                bodyMap.put("reactive_power", dataList.subList(5, 9));
                bodyMap.put("power_factor", dataList.subList(9, 13));
                bodyMap.put("voltage", dataList.subList(13, 16));
                bodyMap.put("electricity", dataList.subList(16, 20));
                bodyMap.put("power", dataList.subList(20, 24));

                for (Map.Entry<String, List> m : bodyMap.entrySet()
                ) {
                    String key = m.getKey();
                    List body = m.getValue();
                    newDataList = new ArrayList();
                    newDataList.add(mpedId);//ID:BIGINT:测量点标识（MPED_ID）
                    newDataList.add(eventDate);//DATA_DATE:DATE:数据日期
                    newDataList.add(colTime);//COL_TIME:DATETIME:终端抄表日期时间
                    newDataList.add("");//ORG_NO:VARCHAR:供电单位编号
                    for (int i = 0, j = body.size(); i < j; i++) {
                        newDataList.add(body.get(i));
                    }
                    newDataList.add(key);
                    obList.add(newDataList);
                }
                return obList;
            } else if (fn == 170) {//集中抄表电表抄读信息
                newDataList = new ArrayList();
                newDataList.add(mpedId);//ID:BIGINT:主键
                String date = DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD);
                newDataList.add(DateUtil.format((Date) dataList.get(5), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                newDataList.add(date);//CO_DATE:DATE:采集日期
                newDataList.add(null);//ORG_NO:VARCHAR:供电单位编号
                newDataList.add(null);//CP_NO:VARCHAR:采集点编号
                newDataList.add(null);//AREA_CODE:VARCHAR:区域码
                newDataList.add(null);//TERMINAL_ADDR:VARCHAR:终端地址码
                newDataList.add(dataList.get(0));//CMNT_PORT_NO:INT:通信端口号
                newDataList.add(dataList.get(1));//RELAY_ROUT_SERIES:INT:中继路由级数
                String phaseMessage = new StringBuilder((String) dataList.get(2)).reverse().toString();
                String quality = (String) dataList.get(3);
                char[] chars = phaseMessage.toCharArray();
                newDataList.add(chars[4]);//READ_PHASE_A:VARCHAR:存载波相位信息的D4位，1为A相接入，0为A相未接入或断相
                newDataList.add(chars[5]);//READ_PHASE_B:VARCHAR:存载波相位信息的D5位，1为B相接入，0为B相未接入或断相
                newDataList.add(chars[6]);//READ_PHASE_C:VARCHAR:存载波相位信息的D6位，1为C相接入，0为C相未接入或断相
                newDataList.add(phaseMessage.substring(4, 7));//READ_PHASE:VARCHAR:存载波相位信息的D4-D6位
                newDataList.add(chars[7]);//LINE_ABNORMAL_ID:VARCHAR:存载波相位信息的D7位，0表示接线正常，1表示接线异常，零火互易
                newDataList.add(null);//ACTUAL_PHASE_A:VARCHAR:实际相位A
                newDataList.add(null);//ACTUAL_PHASE_B:VARCHAR:实际相位B
                newDataList.add(null);//ACTUAL_PHASE_C:VARCHAR:实际相位C
                newDataList.add(null);//ACTUAL_PHASE:VARCHAR:实际相位
                newDataList.add(quality.substring(0, 5));//SEND_QUALITY:VARCHAR:存载波信号品质的D7-D4位，可翻译成数字范围1-15
                newDataList.add(quality.substring(5));//RCV_QUALITY:VARCHAR:存载波信号品质的D3-D0位，可翻译成数字范围1-15
                newDataList.add(dataList.get(4));//LAST_READ_FLAG:VARCHAR:最近一次抄表成功/失败标志
                newDataList.add(dataList.get(5));//LAST_READ_SUCC_TIME:DATETIME:最近一次抄表成功时间
                newDataList.add(dataList.get(6));//LAST_READ_FAIL_TIME:DATETIME:最近一次抄表失败时间
                newDataList.add(dataList.get(7));//LAST_FAIL_TIMES:INT:最近连续失败累计次数
                newDataList.add(quality.substring(5, 6));//METER_PHASE:VARCHAR:载波表相别,存载波相位信息的D3位：0代表单相载波表? 1代表三相载波表
                newDataList.add(quality.substring(6));//SEQUENTIAL_TYPE:VARCHAR:相序类型,存载波相位信息的D2-D0位：000表示ABC正常相位，001表示ACB，010表示BAC，011表示BCA，100表示CAB，101表示CBA，110表示零火反接，111为保留
            } else if (fn == 258) {//查询网络规模
                newDataList = new ArrayList();
                String terminalId = terminalArchivesObject.getTerminalId();
                newDataList.add(new BigDecimal(terminalId));//TERMINAL_ID:BIGINT:终端ID
                newDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                newDataList.add(dataList.get(0));//NETWORK_SIZE:INT:网络规模
            } else if (fn == 214) {//查询指定测量点当前相位信息
                newDataList = new ArrayList();
                String orgNo = terminalArchivesObject.getPowerUnitNumber();
                String terminalId = terminalArchivesObject.getTerminalId();
                newDataList.add(new BigDecimal(terminalId));//TERMINAL_ID:BIGINT:本实体记录的唯一标识
                newDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                newDataList.add(orgNo);//ORG_NO:VARCHAR:供电单位
                newDataList.add("");//PHASE_CHANGE_NUM:INT:变更测量点总数量
                newDataList.add("");//RESPONSE_NUM:INT:本帧回复的变更测量点数量
                newDataList.add("");//CHANNEL_QUALITY:VARCHAR:相位变更信息，存储格式为：第1个测量点号,第1个测量点变更前相位信息 ,第1个测量点变更后相位信息|第2个测量点号,第2个测量点变更前相位信息 ,第2个测量点变更后相位信息|......
                newDataList.add(orgNo.substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                newDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
            } else {//电能表购、用电信息
                throw new RuntimeException("Can not find fn type:" + fn);
            }
        }
        return newDataList;
    }


}
