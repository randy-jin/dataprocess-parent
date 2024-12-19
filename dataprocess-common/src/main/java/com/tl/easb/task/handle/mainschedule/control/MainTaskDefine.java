package com.tl.easb.task.handle.mainschedule.control;

public class MainTaskDefine {
    public static final String SEPARATOR = "_";

    public static String DATA_FLAG_PARAM_QUERY_SG3761_0AF10 = "201";  //参数查询(0AF10)
    //3：实时数据
    public static final int ITEMS_SCOPE_CURRENT_TIME = 3;
    //4:日冻结数据
    public static final int ITEMS_SCOPE_DAY_READ = 4;
    //5：抄表日数据
    public static final int ITEMS_SCOPE_TRANS_READ = 5;
    //51：电表参数设置
    public static final int ITEMS_SCOPE_TRANS_READ_SET = 51;
    //52：电表参数召测
    public static final int ITEMS_SCOPE_TRANS_READ_GET = 52;
    //6:月冻结数据
    public static final int ITEMS_SCOPE_MON_READ = 6;
    //7：曲线数据
    public static final int ITEMS_SCOPE_CURVE_READ = 7;
    public static String ITEMS_SCOPE_CURVE_READ_96 = "701";
    public static String ITEMS_SCOPE_CURVE_READ_288 = "702";
    //71：当日曲线数据
    public static final int ITEMS_SCOPE_CURRENT_CURVE_READ = 71;

    public static final int DATA_FLAG_DIRECT_METER_PARAM_SET = 51;       //电表参数设置
    public static final int DATA_FLAG_DIRECT_METER_PARAM_QUERY = 52;       //电表参数召测
    public static final int DATA_FLAG_DIRECT_METER_CTRL = 53;       //电表控制
    public static final int DATA_FLAG_DIRECT_INSTANCE_DATA = 54;       //直抄实时数据
    public static final int DATA_FLAG_DIRECT_FREEZE_DATA = 55;       //直抄日冻结数据
    public static final int DATA_FLAG_DIRECT_EVENT = 56;       //直抄事件

    //冻结密度
	/*  1：15分钟
		2：30分钟
		3：60分钟
		4：1分钟
		5：5分钟*/
    public static final int CURVE_TYPE_15 = 1;
    public static final int CURVE_TYPE_30 = 2;
    public static final int CURVE_TYPE_60 = 3;
    public static final int CURVE_TYPE_1 = 255;
    public static final int CURVE_TYPE_5 = 254;

    //采集范围
    //1：终端
    public static final int R_SCOPE_TMNL = 1;
    //2：测量点
    public static final int R_SCOPE_CP = 2;
    //3：总加组
    public static final int R_SCOPE_TOTAL = 3;

    //自动任务类型
    //01:普通任务
    public static final String IF_BROADCAST_TMNL = "01";
    //02:透传任务
    public static final String IF_BROADCAST_MPED = "02";
    /*	//00：数据采集
        public static final String IF_BROADCAST_COll= "00";
        //01：终端时钟
        public static final String IF_BROADCAST_CLOCK= "01";
        //02：终端参数
        public static final String IF_BROADCAST_PARAM= "02";
        //03：终端对时
        public static final String IF_BROADCAST_TIMING= "03";
        //04：版本信息
        public static final String IF_BROADCAST_VERSION= "04";
        //05：载波模块版本
        public static final String IF_BROADCAST_CA_VERSION= "05";
        //06：透传任务
        public static final String IF_BROADCAST_TRANS_SEND= "06";
        //07：终端电池
        public static final String IF_BROADCAST_BATTERY_SEND= "07";*/
    //09：全事件,通过终端预抄电表事件,到终端的sql
//	public static final String IF_BROADCAST_WHOLE_EVENT_PRE_TMNLSQL= "09";
    //08：全事件,通过终端预抄电表事件,到测点的自定义sql
    public static final String IF_BROADCAST_WHOLE_EVENT_PRE = "08";
    //公共事业
    public static final String IF_BROADCAST_UTILITY = "10";

    //采集范围SQL配置类型
    //01：表达式方式
    public static final String TASK_SQL_01 = "01";
    //02：自定义方式
    public static final String TASK_SQL_02 = "02";
}
