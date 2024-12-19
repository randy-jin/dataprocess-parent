package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wangjunjie
 * @date 2021/12/22 14:59
 */
public class e_mp_day_read implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        dataList.remove(0);//删除数据list中的主键，因为前置为null需要自己放
        String dataType=dataList.remove(0).toString();//删除并取出list当前index为0的值作为dataType
        String dataDate=dataList.remove(0).toString();//删除并取出list当前index为0的值作为数据日期
        Object callObj=dataList.remove(0);//删除并取出list当前index为0的值作为抄表时间
        Date callDate=new Date();
        if(callObj!=null){
            callDate=new Date(Long.parseLong(callObj.toString()));
        }
        //用于缓存删除和datahub自动分配shardid, 每个表不同，要么是 测量点id 要么是 终端id
        String shardIndex=terminalArchivesObject.getMpedId();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);


        finallyList.add(terminalArchivesObject.getMpedId()+"_"+dataDate);
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataType);
        finallyList.add(callDate);
        for (int i=0;i<15;i++) {
            Object obj=dataList.get(i);
            if(obj==null){
                finallyList.add(null);
                continue;
            }
            finallyList.add(Double.parseDouble(dataList.get(i).toString()));
        }
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add("00");
        finallyList.add(dataSrc);
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));
        finallyList.add(dataDate);

        return finallyList;
    }
}
