package com.tl.easb.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangchunhuai on 2021/7/14.
 * 前置相关数据项和dataflag
 */
public class SplitPackageConst {

    public static final String DEFAULT_PWD  = "000000000000";

    public static final String PROTOCOL_SG3761  = "1";      //SG376.1
    public static final String PROTOCOL_SGCONTROL  = "4";  //负控规约
    public static final String PROTOCOL_TERMINAL_ONU  = "11";  //

    public static final String PROTOCOL_SG645_97 = "2";  //645_97
    public static final String PROTOCOL_SG645_07 = "3";  //645_07
    public static final String PROTOCOL_SGOOP   = "9";  //面向对象
    public static final String PROTOCOL_SGOOP_METER   = "8";  //面向对象电表


    //========================================================
    public static final String DATA_FLAG_PARAM_SET = "1";                   //参数设置
    public static final String DATA_FLAG_PARAM_SET_SG3761_04F10 = "101";    //参数设置(04F10)
    public static final String DATA_FLAG_PARAM_QUERY = "2";                 //参数查询
    public static final String DATA_FLAG_PARAM_QUERY_SG3761_0AF10 = "201";  //参数查询(0AF10)
    public static final String DATA_FLAG_INSTANCE_DATA = "3";               //实时数据
    public static final String DATA_FLAG_DATE_FREEZE_DATA = "4";            //日冻结数据
    public static final String DATA_FLAG_RECORD_DATE_FREE_DATA = "5";       //抄表日数据
    public static final String DATA_FLAG_MONTH_FREEZE_DATA = "6";           //月冻结数据
    public static final String DATA_FLAG_CURVE_DATA = "7";                  //曲线数据
    public static final String DATA_FLAG_CURVE_DATA_96  = "701";            //曲线数据(96)
    public static final String DATA_FLAG_CURVE_DATA_288 = "702";            //曲线数据(288)

    public static final String DATA_FLAG_NOW_DATA = "54";             //直超实时类  --yuanqiang

    public static final String DATA_FLAG_CONTROL_CMD = "8";                 //控制命令
    public static final String DATA_FLAG_EVENT_DATA = "9";                  //事件
    public static final String DATA_FLAG_TMNL_DATA = "10";               //afn=09终端信息

    public static final String DATA_FLAG_DIRECT_METER_PARAM_SET      = "51";       //电表参数设置
    public static final String DATA_FLAG_DIRECT_METER_PARAM_QUERY    = "52";       //电表参数召测
    public static final String DATA_FLAG_DIRECT_METER_CTRL           = "53";       //电表控制
    public static final String DATA_FLAG_DIRECT_INSTANCE_DATA = "54";       //直抄实时数据
    public static final String DATA_FLAG_DIRECT_FREEZE_DATA   = "55";       //直抄日冻结数据
    public static final String DATA_FLAG_DIRECT_EVENT         = "56";       //直抄事件
    public static final String DATA_FLAG_DIRECT_EIGHT         = "58";
    public static final String MONTH_FREEZE_DATA         = "57"; //直抄月冻结

    public static final String HPLC_0C_221 = "80119";            				 //hplc 0CF221
    public static final String HPLC_0C_223 = "80121";           				 //hplc 0CF223
    public static final String HPLC_0C_15  = "80149";          				 //hplc 0CF15

    public static final String BJ_HPLC_SIGN1  = "2000";          				 //hplc终端行政区划 2000开头
    public static final String BJ_HPLC_SIGN2  = "2001";          				 //hplc终端行政区划 2000开头


    public static final String HPLC_0D_27  = "235005";          				 // 0DF27
    public static final String HPLC_0C_214  = "80128";          				 // 0CF214


    //========================================================
    public static final String OBJ_FLAG_TMNL         = "1";
    public static final String OBJ_FLAG_MPED         = "2";
    public static final String OBJ_FLAG_TOTAL        = "3";
    public static final String OBJ_FLAG_DIRECT_SIMU  = "4";
    public static final String OBJ_FLAG_TURNS        = "5";
    public static final String OBJ_FLAG_TASK         = "6";
    public static final String OBJ_FLAG_GRADE_CLASS  = "7";
    public static final String OBJ_FLAG_GRADE        = "8";

    //========================================================
    public static final int PACK_RTN_ERR_TMNL_OFFLINE   = -1;
    public static final int PACK_RTN_ERR_OVER_TIME      = -2;
    public static final int PACK_RTN_ERR_TMNL_REFUSE    = -3;

    public static final String PACK_RTN_ERR_MSG_TMNL_OFFLINE   = "终端不在线";
    public static final String PACK_RTN_ERR_MSG_OVER_TIME      = "终端返回报文超时";
    public static final String PACK_RTN_ERR_MSG_TMNL_REFUSE    = "终端否认";

    //多规约缓存的常量信息
    public static final String TK_FIN_SUCESSS    = "1";

    //645电表规约
    public static final String METER_CMD_1C = "1C"; //7.14	跳合闸、报警、保电
    public static final String METER_CMD_14 = "14"; //7.3	写数据
    public static final  String METER_CMD_03 = "03";
    public static final  String METER_CMD_04 = "04";//97规约数据


    //================================================调用前置接口返回==================================

    public static final  String FRONT_INTER_ERR_TMNLDATA_ISNULL = "-1";
    public static final  String FRONT_INTER_ERR_MEM_DATA_MISS = "-1";
    ////SG376.1跳合闸 调用send 返回结果在 CallMessage645Utils中定义

    public static final  int [] priorityArray ={1,2,48,99,100};

    //购电单相关di
    public static final  String [] powerOrderArray ={"078102FF","00900200","00900201","070102FF","070101FF","1A","1B","1C"};
}
