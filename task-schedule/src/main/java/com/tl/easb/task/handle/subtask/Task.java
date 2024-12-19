package com.tl.easb.task.handle.subtask;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Task {
	private String taskId = null;   //子任务对应的任务ID
	private String[] taskStatusParams = null;   //子任务对应的任务的任务状态
	
	//taskStatusParams 的拆分
	private String statusParamsStartTime = null;   //开始时间
	private String statusParamsCollDataDate = null;
	private int statusParamsPriority = SubTaskDefine.STATUS_PRIORITY_MIN_OUT; //优先级
	private int statusParamsRecollIndex = -1;  //补采次数
	private int statusParamsCollInfoProdMethod = -1; //采集信息生成方式
	private boolean statusParamsExecFlag = false; //执行标识
	private long statusParamsCollTotalNum = -1; //应采总数
	
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public String[] getTaskStatusParams() {
		return taskStatusParams;
	}
	public void setTaskStatusParams(String[] taskStatusParams) {
		this.taskStatusParams = taskStatusParams;
		this.statusParamsCollDataDate = taskStatusParams[SubTaskDefine.STATUS_PARAMS_COLL_DATA_DATE];
		this.statusParamsStartTime = taskStatusParams[SubTaskDefine.STATUS_PARAMS_START_TIME];
		this.statusParamsPriority = Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_PRIORITY]);
		this.statusParamsRecollIndex = Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_RECOLL_INDEX]);
		this.statusParamsCollInfoProdMethod = Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_COLLINFO_PRODUCE]);
		if(SubTaskDefine.STATUS_EXEC_FLAG_EXECING.equals(taskStatusParams[SubTaskDefine.STATUS_PARAMS_EXEC_FLAG])){
			this.statusParamsExecFlag = true;
		}else{
			this.statusParamsExecFlag = false;
		}
		this.statusParamsCollTotalNum = Long.parseLong(taskStatusParams[SubTaskDefine.STATUS_PARAMS_COLL_TOTAL_NUM]);
	}

	public String getStatusParamsStartTime() {
		return statusParamsStartTime;
	}
	public int getStatusParamsPriority() {
		return statusParamsPriority;
	}
	public int getStatusParamsRecollIndex() {
		return statusParamsRecollIndex;
	}
	public int getStatusParamsCollInfoProdMethod() {
		return statusParamsCollInfoProdMethod;
	}
	public boolean isStatusParamsExecFlag() {
		return statusParamsExecFlag;
	}
	public long getStatusParamsCollTotalNum() {
		return statusParamsCollTotalNum;
	}
	public String getStatusParamsCollDataDate() {
		return statusParamsCollDataDate;
	}
	public Date getStatusParamsCollDataDateByDate() throws ParseException{
		DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		return format.parse(this.statusParamsCollDataDate);
	}
	public String toString(){
		return "{任务id:"+this.taskId+", 开始时间："+this.statusParamsStartTime+
				", 采集时间:"+this.statusParamsCollDataDate+", 优先级:"+this.statusParamsPriority+
				", 补采序号："+this.statusParamsRecollIndex+", 信息生成方式："+this.statusParamsCollInfoProdMethod+
				", 执行标志:"+this.statusParamsExecFlag+", 应采总数："+this.statusParamsCollTotalNum+"}";
	}
}
