package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.protocol.sg645.callmessage.controllers.Parser;
import com.ls.athena.protocol.sg645.callmessage.controllers.Parser645UtilDataGrid;
import com.ls.athena.protocol.sg645.callmessage.controllers.comm.Data0001FF01;
import com.ls.pf.base.utils.tools.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Split645DataPacket {

    public static List<Object> split645Monitor(DataItemObject item) throws Exception {
        List<Object> objList = item.getList();//解析的list
        String dataItem = "";
        if (objList != null && objList.size() > 0) {//objList中保存透明转发规约解析数据项的值，该list中最后一项保存645报文
            if (objList.size() > 3) {
                List<Object> dList = (List<Object>) objList.get(4);
                return dList;
            }
            int size = Integer.valueOf(objList.get(1).toString());//获取645长度
            if (size > 0) {//若长度大于0，说明有645报文，若等于0，说明没有645报文，可能为没有电表信息
                byte[] byte645 = (byte[]) objList.get(2);//获取645报文byte数组
                Parser parser = new Parser();
                System.out.println(StringUtils.encodeHex(byte645));
                Map<Object, Object> map = parser.parse(byte645);//解析645
                String meterCommId = parser.getMeterComId();//获取终端通信地址
                Map<Object, Object> data = (Map<Object, Object>) map.get("_" + meterCommId + "_");//根据终端地址获取数据
                String error = "";
                if (data == null) {
                    error = null;
                } else {
                    error = (String) data.get("ERROR");
                }
                if (error != null) {//报文解析出错误字信息，不入库
                    return null;
                } else {
                    objList = new ArrayList<Object>();
                    dataItem = parser.getDataItem();//获取数据项
                    if (null != data.get("METER_TIME")) {
                        String meterTime = (String) data.get("METER_TIME");
                        objList.add(meterTime);
                    } else if (null != data.get("METER_DATE")) {
                        String meterDate = (String) data.get("METER_DATE");
                        objList.add(meterDate);
                    } else if (null != data.get("timeBattery")) {
                        objList.add(data.get("timeBattery"));
                    } else if (null != data.get("JDQZT")) {
                        String crunTime = data.get("CRUNTIME").toString();       //当前运行时段 0=第一套 1=第二套
                        String supplyType = data.get("SUPPLYTYPE").toString();   //供电方式 (00主电源，01辅助电源，10电池供电)
                        String setPara = data.get("SETPARA").toString();         //编程允许 (0禁止，1许可)
                        String meterState = data.get("JDQZT").toString();        //继电器状态 (0通，1断)
                        String cyeartable = data.get("CYEARTABLE").toString();   //当前运行时区(0第一套，1第二套)
                        String jdqmlzt = data.get("JDQMLZT").toString();         //继电器命令状态(0通，1断)
                        String switchAlarm = data.get("SWITCHALARM").toString(); //预跳闸报警状态(0无，1有)
                        String meterType = data.get("METERTYPE").toString();     //电能表类型(00非预付费表,01电量型预付费表,10电费型预付费表)
                        String tariff = data.get("TARIFF").toString();           //当前运行费率电价 (0第一套，1第二套)
                        String acdState = data.get("ACDSTATE").toString();       //当前阶梯(0第一套，1第二套)
                        String protectStat = data.get("PROTECTSTAT").toString(); //保电状态(0非保电 1保电)
                        String esamStat = data.get("ESAMSTAT").toString();       //身份认证状态(0失效 1有效)
                        String localAcc = data.get("LOCALACCOUNT").toString();   //本地开户(0开户 1未开)
                        String remoteAcc = data.get("REMOTEACCOUNT").toString(); //远程开户 (0开户 1未开)
                        objList.add(crunTime);
                        objList.add(supplyType);
                        objList.add(setPara);
                        objList.add(meterState);
                        objList.add(cyeartable);
                        objList.add(jdqmlzt);
                        objList.add(switchAlarm);
                        objList.add(meterType);
                        objList.add(tariff);
                        objList.add(acdState);
                        objList.add(protectStat);
                        objList.add(esamStat);
                        objList.add(localAcc);
                        objList.add(remoteAcc);
                        //list [bit0----bit8---bit15]
                    } else if (null != data.get("DATA200100")) {
                        if (dataItem != null && dataItem.length() > 4 && !dataItem.endsWith("00")) {
                        }
                        Data0001FF01 ob = (Data0001FF01) data.get("DATA200100");
                        List<Object> list = getList(ob);//将ben对象的属性数据保存至list中
                        objList.add(list.get(0));//获取总费率
                        list.remove(0);//移除总费率
                        objList.add(list.size());//费率个数
                        objList.add(list);//费率集合
                    } else if (null != data.get("V_PHASEA") || null != data.get("V_PHASEB") || null != data.get("V_PHASEC")) {
                        if (null != data.get("V_PHASEA")) {
                            objList.add(data.get("V_PHASEA"));
                        }
                        if (null != data.get("V_PHASEB")) {
                            objList.add(data.get("V_PHASEB"));
                        }
                        if (null != data.get("V_PHASEC")) {
                            objList.add(data.get("V_PHASEC"));
                        }
                    } else if (null != data.get("AUTO_REPO")) {
                        List<String> markList = null;
                        if (data.get("AUTO_REPO").equals("8")) {
                            markList = Parser645UtilDataGrid.AUTO_REPO_MARKS;
                        } else if (data.get("AUTO_REPO").equals("12")) {
                            markList = Parser645UtilDataGrid.AUTO_REPO_MARKS_OTHER;
                        }
                        if (markList != null) {
                            for (String mark : markList) {
                                String value = String.valueOf(data.get(mark));
                                objList.add(value);
                            }
                        }
                    } else if (Map645AllEventListNameConst.map.get(dataItem) != null) {//全事件
                        if ("1F00FF01".equals(dataItem)) {
                            objList.add(data.get("BEGINTIME"));
                            objList.add(data.get("BEGINENERGY0"));
                            objList.add(data.get("BEGINENERGY1"));
                            objList.add(data.get("BEGINENERGY2"));
                            objList.add(data.get("BEGINENERGY3"));
                            objList.add(data.get("ENDTIME"));
                            objList.add(data.get("ENDENERGY0"));
                            objList.add(data.get("ENDENERGY1"));
                            objList.add(data.get("ENDENERGY2"));
                            objList.add(data.get("ENDENERGY3"));
                        } else {
                            for (int i = 1; i <= data.size(); i++) {
                                objList.add(data.get("item" + i));
                            }
                        }
                    } else if (null != data.get("writeOk")) {//新增  645正常返回94 00
                        objList.add(data.get("writeOk"));//设置的是94
                    } else {
                        if (data != null) {
                            if (data.get("D1") != null) {
                                int num = data.size();
                                for (int i = 1; i <= num; i++) {
                                    objList.add(data.get("D" + i));
                                }
                            } else {
                                Iterator<Object> it = data.keySet().iterator();
                                while (it.hasNext()) {
                                    Object obj = data.get(it.next());
                                    if (obj instanceof List) {
                                        List<String> l = (List<String>) obj;
                                        if (l != null && l.size() > 0) {
                                            for (String s : l) {
                                                objList.add(s);
                                            }
                                        }
                                    } else {
                                        objList.add(obj);
                                    }
                                }
                            }
                        }
                    }
                    objList.add(meterCommId);
                    objList.add(dataItem);
                    return objList;
                }
            }
        }
        return objList;

    }


    private static List<Object> getList(Data0001FF01 ob) throws Exception {
        List<Object> list = new ArrayList<Object>();
        Object val = ob.getPapR();
        list.add(val);
        for (int i = 1; i < 14; i++) {
            val = Data0001FF01.class.getMethod("getPapR" + i, null).invoke(ob, null);
            if (val != null && !val.equals("")) {
                list.add((BigDecimal) val);
            }
        }
        return list;
    }


}
