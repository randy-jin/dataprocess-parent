package com.test;

import com.alibaba.fastjson.JSON;
import com.tl.easb.utils.CallSend.MpedDataSet_bak;
import com.tl.easb.utils.CallSend.TmnlMessageSet_bak;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangchunhuai on 2022/3/10.
 */
public class Test {

    private static final int MPED_COUNT=10;//测量点数
    private static final int D_COUNT=2; // 数据项数

    public static void main(String[] args) {
        TmnlMessageSet_bak term = new TmnlMessageSet_bak("8000000020706136", "701");
        // 设置terminalId
        term.setTmnlId("8000000020706136");
        // 设置规约类型
        term.setProtocolId("1");
        // 设置objFlag
        term.setDataObj("2");
        // 设置dataFlag
        term.setDataFlag("701");
        term.setAreaCode("1101");
        term.setTmnlAddr("20021");
        // 设置是否写入数据库
        term.setWriteDatabase(true);
        // 设置是否写入内存库
        term.setWriteHbase(true);
        term.setPassword("000000");
        term.setPriority(100);//统一指令优先级

        List<MpedDataSet_bak> mpedList = new ArrayList<MpedDataSet_bak>();

        for (int i=0;i<MPED_COUNT;i++){
            for(int d=0;d<D_COUNT;d++){
                //mpedID  长度 16
                String mpedId=i+"0000000000000000";
                if(mpedId.length()>16){
                    mpedId=mpedId.substring(0,15);
                }
                MpedDataSet_bak mpedData = new MpedDataSet_bak(mpedId, "233008");
                mpedData.setMpedId(mpedId);
                mpedData.setMpedType("52");
                mpedData.setDataItemId("233008");
                // sg3761
                mpedData.setAfn("0D");
                mpedData.setFn("161");
                mpedData.setPn(""+i);
                //addr 003100552233
                mpedData.setMpedAddress(003100552233+i+"");
//                mpedData.setPort("31");
//                mpedData.setCtrl("10");
//                mpedData.setDI("1");
//                mpedData.setBaud("3");
//                mpedData.setStopBit("0");// mpedParam[1]测试用写死
//                mpedData.setNocheckOut("2");
//                mpedData.setCheckOut("0");// mpedParam[3]测试用写死
//                mpedData.setNumber("0");
//                mpedData.setTimeOutUnit("1200");
//                // mpedData.setTimeOut(mpedParam[6]);
//                mpedData.setTimeOut("90");
//                mpedData.setTimeOutDelay("");
                mpedList.add(mpedData);
                // 设置mpedDataList
                term.setMpedDataList(mpedList);
            }

        }
        String json= JSON.toJSONString(term);
       // System.out.println(json);
        System.out.println("UTF-8: (kB) "+json.getBytes().length/1024);
    }

}
