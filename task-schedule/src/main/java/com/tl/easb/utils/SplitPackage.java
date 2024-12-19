package com.tl.easb.utils;


import com.ls.athena.callmessage.multi.batch.MpedDataSet;
import com.ls.athena.callmessage.multi.batch.TmnlMessageSet;
import com.tl.easb.utils.multi.service.ToRedis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/7/14.
 */
public class SplitPackage {


    /**
     * 根据终端对象中的测量点list进行拆包并返回拆包后的终端对象集合
     *
     * @param tms
     * @return
     */
    public static List<TmnlMessageSet> splitList(TmnlMessageSet tms) {
        List<TmnlMessageSet> tmnlList = new ArrayList<>();
        int packCount = 0;
        // ParamConstants.SPLIT_PACKAGE 是否拆包的判断
        if(PropertyUtils.getIntValue("SPLIT.PACKAGE", 1)==0){
            tmnlList.add(tms);
            return tmnlList;
        }
        //SPLIT.NUM表示每一包的倍率数。
        if (SplitPackageConst.PROTOCOL_SG3761.equals(tms.getProtocolId())) {
            packCount = PropertyUtils.getIntValue("SPLIT.NUM", 1)*getPackCountBy3761(tms.getDataFlag());
        }
        if (SplitPackageConst.PROTOCOL_SGOOP.equals(tms.getProtocolId())) {
            packCount = PropertyUtils.getIntValue("SPLIT.NUM", 1)*getPackCountBy698(tms.getDataFlag());
        }
        List<MpedDataSet> allMpList = tms.getMpedDataList();
        if (allMpList.isEmpty()) {
            return null;
        }
        //将原终端对象中的测量点集合根据拆包规则进行拆包
        List<List<MpedDataSet>> splitMpedList = ToRedis.splitList(allMpList, packCount);
        //将拆好的测量点包重新放到一个新的终端对象中
        for (List<MpedDataSet> mpedDatalist : splitMpedList) {
            // 重新构造TmnlMessage
            TmnlMessageSet term = new TmnlMessageSet(tms.getTmnlId(), tms.getDataFlag());
            // 设置terminalId
            term.setTmnlId(tms.getTmnlId());
            // 设置规约类型
            term.setProtocolId(tms.getProtocolId());
            // 设置objFlag
            term.setDataObj(tms.getDataObj());
            // 设置dataFlag
            term.setDataFlag(tms.getDataFlag());
            term.setAreaCode(tms.getAreaCode());
            term.setTmnlAddr(tms.getTmnlAddr());
            tms.setPassword("000000");
            tms.setPriority(100);//统一指令优先级
            // 设置是否写入数据库
            term.setWriteDatabase(true);
            // 设置是否写入内存库
            term.setWriteHbase(true);
            term.setMpedDataList(mpedDatalist);
            tmnlList.add(term);
        }
        return tmnlList;
    }

    /**
     * 面向对象拆包方式
     *
     * @param dataFlag
     * @return
     */
    public static int getPackCountBy698(String dataFlag) {

        int packCount = 1;

        if (SplitPackageConst.DATA_FLAG_CURVE_DATA_96.equals(dataFlag)) {//96个点的面向对象
//            packCount = 6;//曲线类  面向对象1个拆6包，故拆包格式单设
            //面向对象曲线需要针对单个测量点再进行拆6包的处理，为避免下发频率过大，面向对象曲线不在本服务中拆包。
            packCount = 0;
        }
        if (SplitPackageConst.DATA_FLAG_CONTROL_CMD.equals(dataFlag)) {//8
            packCount = 4;//控制类
        }
        if (SplitPackageConst.DATA_FLAG_MONTH_FREEZE_DATA.equals(dataFlag)) {//6
            packCount = 4;//月冻结类
        }

        if (SplitPackageConst.DATA_FLAG_DATE_FREEZE_DATA.equals(dataFlag)) {//4
            packCount = 4;//冻结类
        }
        if (SplitPackageConst.DATA_FLAG_RECORD_DATE_FREE_DATA.equals(dataFlag)) {//5
            packCount = 4;//冻结类
        }
        if (SplitPackageConst.DATA_FLAG_INSTANCE_DATA.equals(dataFlag)
                || SplitPackageConst.DATA_FLAG_TMNL_DATA.equals(dataFlag)) {//3 10
            packCount = 4;//实时类
        }

        if (SplitPackageConst.DATA_FLAG_PARAM_QUERY.equals(dataFlag)) {
            packCount = 1;//参数查询
        }

        if (SplitPackageConst.DATA_FLAG_PARAM_SET.equals(dataFlag)) {
            packCount = 1;//参数设置(目前没有批量下发参数功能，尚未区分)
        }

        //新增批量直抄实时
        if (SplitPackageConst.DATA_FLAG_NOW_DATA.equals(dataFlag)) {
            packCount = 1;//直抄实时类
        }


        return packCount;

    }

    /**
     * 3761拆包方式
     *
     * @param dataFlag
     * @return
     */
    public static int getPackCountBy3761(String dataFlag) {
        byte packCount = 0;
        if (SplitPackageConst.DATA_FLAG_CONTROL_CMD.equals(dataFlag)) {
            packCount = 8;
        }

        if (SplitPackageConst.DATA_FLAG_CURVE_DATA.equals(dataFlag) || SplitPackageConst.DATA_FLAG_CURVE_DATA_288.equals(dataFlag) || SplitPackageConst.DATA_FLAG_CURVE_DATA_96.equals(dataFlag)) {
            packCount = 1;
        }

        if (SplitPackageConst.DATA_FLAG_DATE_FREEZE_DATA.equals(dataFlag)) {
            packCount = 8;
        }

        if (SplitPackageConst.DATA_FLAG_INSTANCE_DATA.equals(dataFlag) || SplitPackageConst.DATA_FLAG_TMNL_DATA.equals(dataFlag)) {
            packCount = 8;
        }

        if (SplitPackageConst.DATA_FLAG_PARAM_QUERY.equals(dataFlag)) {
            packCount = 8;
        }

        if (SplitPackageConst.DATA_FLAG_PARAM_QUERY_SG3761_0AF10.equals(dataFlag)) {
            packCount = 1;
        }

        if (SplitPackageConst.DATA_FLAG_PARAM_SET.equals(dataFlag)) {
            packCount = 8;
        }

        if ("1".equals(is645(dataFlag))) {
            packCount = 1;
        }

        if (SplitPackageConst.DATA_FLAG_MONTH_FREEZE_DATA.equals(dataFlag)) {
            packCount = 1;
        }

        return packCount;
    }

    public static String is645(String dataflag) {
        return !SplitPackageConst.DATA_FLAG_DIRECT_METER_PARAM_SET.equals(dataflag) && !SplitPackageConst.DATA_FLAG_DIRECT_METER_PARAM_QUERY.equals(dataflag) && !SplitPackageConst.DATA_FLAG_DIRECT_METER_CTRL.equals(dataflag) && !SplitPackageConst.DATA_FLAG_DIRECT_INSTANCE_DATA.equals(dataflag) && !SplitPackageConst.DATA_FLAG_DIRECT_FREEZE_DATA.equals(dataflag) && !SplitPackageConst.DATA_FLAG_DIRECT_EVENT.equals(dataflag) ? "0" : "1";
    }

}