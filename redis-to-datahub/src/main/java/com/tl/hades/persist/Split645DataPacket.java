package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.protocol.sg645.callmessage.controllers.comm.Data0001FF01;
import com.ls.pf.base.utils.tools.StringUtils;
import com.tl.protocols.sg645.data.output.SG645Data;
import com.tl.protocols.sg645.decoder.P645Parser;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


public class Split645DataPacket {

    public static String backwards(byte[] randomArr) {
        byte[] backwardsArr = new byte[randomArr.length];
        int count = 0;
        for (int i = randomArr.length - 1; i >= 0; i--) {
            backwardsArr[count] = randomArr[i];
            count++;
        }
        String backed = StringUtils.encodeHex(backwardsArr);
        return backed;
    }

    public static List<Object> split645Monitor(DataItemObject item) throws Exception {
        List<Object> objList = item.getList();//解析的list
        if (objList != null && objList.size() > 0) {//objList中保存透明转发规约解析数据项的值，该list中最后一项保存645报文
            if (objList.size() > 3) {
                List<Object> dList = (List<Object>) objList.get(4);
                return dList;
            }
            int size = Integer.valueOf(objList.get(1).toString());//获取645长度
            if (size > 0) {//若长度大于0，说明有645报文，若等于0，说明没有645报文，可能为没有电表信息
                P645Parser p645 = new P645Parser(false);
                byte[] byte645 = (byte[]) objList.get(2);//获取645报文byte数组
                SG645Data sg645Data = p645.parseFrame(StringUtils.decodeHex(StringUtils.encodeHex(byte645)));
                List dataList;
                dataList = sg645Data.getPureBodyResList();
                boolean isOther = false;
                for (String str : otherDi) {
                    isOther = sg645Data.getDataItem().startsWith(str);
                    if (isOther) {
                        break;
                    }
                }
                if (!isOther) {
                    if (sg645Data.getPureBodyResList().size() > 0) {
                        if (sg645Data.getPureBodyResList().get(0) instanceof List) {
                            dataList = (List) sg645Data.getPureBodyResList().get(0);
                        }
                    }
                }
                dataList.add(sg645Data.getMeterComId());
                dataList.add(sg645Data.getDataItem());
                return dataList;
            }
        }
        return objList;

    }

    private static String[] otherDi = {"0101FF", "0102FF", "0103FF", "0104FF"};


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
