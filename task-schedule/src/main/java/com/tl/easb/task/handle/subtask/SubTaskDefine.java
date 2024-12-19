package com.tl.easb.task.handle.subtask;

public class SubTaskDefine {
	public static final String SEPARATOR = "_";
	
	//任务标识组成：开始时间_采集日期_优先级_补采次数_采集信息生成方式_执行标识_应采总数
	public static final int STATUS_PARAMS_START_TIME = 0; //任务状态开始时间
	public static final int STATUS_PARAMS_COLL_DATA_DATE = 1; //采集日期
	public static final int STATUS_PARAMS_PRIORITY = 2; //优先级
	public static final int STATUS_PARAMS_RECOLL_INDEX = 3; //补采次数
	public static final int STATUS_PARAMS_COLLINFO_PRODUCE = 4; //采集信息生成方式
	public static final int STATUS_PARAMS_EXEC_FLAG = 5; //执行标识
	public static final int STATUS_PARAMS_COLL_TOTAL_NUM = 6; //应采总数
	
	//执行标识状态
	public static final String STATUS_EXEC_FLAG_STOP = "0"; //任务停止
	public static final String STATUS_EXEC_FLAG_EXECING = "1"; //任务执行中
	
	//优先级1为最高 9为最低
	public static final int STATUS_PRIORITY_MAX = 1; //任务最高优先级
	public static final int STATUS_PRIORITY_MIN_OUT = 10; //任务超出的最低优先级范围
	
	//采集信息生成方式
	public static final int STATUS_COLLINFO_FROM_HISTORY = 0; //从历史表取数据
	public static final int STATUS_COLLINFO_FROM_CONFIG = 1; //从配置文件取数据
	public static final int STATUS_COLLINFO_FROM_CACHE = 2; //从缓存取数据
	
	//子任务组成：任务编号_行政区码_终端地址_采集时间_终端业务标识
	public static final int SUBTASK_PARAMS_TASKID = 0;  //子任务id
	public static final int SUBTASK_PARAMS_AREA = 1;   //子任务行政区码
	public static final int SUBTASK_PARAMS_TERMINAL_ADDR = 2; //子任务终端地址
	public static final int SUBTASK_PARAMS_TERMINAL_ID = 4; //子任务终端业务标识
	
	
	//terminalId_测量点标识_采集日期（yyyymmddhhmmss）_数据项标识_规约类型；
	public static final int CP_TERMINAL_ID = 0;
	public static final int CP_MPED_ID = 1;
	public static final int CP_COLL_DATE = 2;
	public static final int CP_DATA_ITEM_SIGN = 3;
	public static final int CP_DATA_PROTOCOL_ID = 4;
	public static final int CP_DATA_AREA_CODE = 5;
	public static final int CP_DATA_TERMINAL_ADDR = 6;
	public static final int CP_DATA_MPED_INDEX = 7;
	public static final int CP_DATA_AFN = 8;
	public static final int CP_DATA_FN = 9;
	public static final int CP_DATA_ADDR = 10;
	public static final int CP_DATA_METER_PROTOCOL_ID = 11;
	
	public static final String CP_CMD_SEPARATE = "-";
	//cmd内容1-05-31，格式组成:规约标识-AFN-FN
	public static final int CP_CMD_AFN = 1;
	public static final int CP_CMD_FN = 2;
	public static final int CP_CMD_CONTROL = 3;
	public static final int CP_CMD_DATAID = 4;
	
	public static final int ITEMS_SCOPE_REALTIME_DATA = 3; //实时数据
	public static final int ITEMS_SCOPE_FREEZE_DATA = 4; //日冻结数据
	//月冻结修改
	public static final int ITEMS_SCOPE_MON_FREEZE_DATA = 6; //月冻结数据
	public static final int ITEMS_SCOPE_CURVE_DATA = 7; //曲线数据
	public static final int ITEMS_SCOPE_CURVE_DATA_DAY = 71; //当日曲线数据
	
	public static final int ITEMS_SCOPE_TC_CURVE_DATA = 58; //透抄曲线数据
}
