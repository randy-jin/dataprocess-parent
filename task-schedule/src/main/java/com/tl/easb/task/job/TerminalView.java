package com.tl.easb.task.job;

import com.tl.easb.task.manage.view.AutoTaskConfig;

public class TerminalView {
	private String terminalId;
	private String cpNo;
	// 终端地址
	private String terminalAddr;
	// 行政区码
	private String areaCode;
	// 任务编号
	private String taskId;
	// 该终端对应的任务信息
	private AutoTaskConfig taskConfig;
	// 级别
	private int level;
	// 消息类型，1:正常；2:立即执行任务；3：补采任务；状态定义参见常量类：JobConstants.TASK_TYPE_*
	private char taskType;

	private String cpName;

	// 终端下所有的电表信息
//	private List<MeterInfo> collMeters;

	// 查询电表信息SQL
	private String meterIdSql;

	/**
	 * 补采次数
	 */
	private int hasRedoCount;

	/**
	 * 召测数据是否保存 1:保存 0：不保存
	 */
	private int isSave;

	private boolean hasSetcollMeters = false;

	public String getCpNo() {
		return cpNo;
	}

	public void setCpNo(String cpNo) {
		this.cpNo = cpNo;
	}

	public String getTerminalAddr() {
		return terminalAddr;
	}

	public void setTerminalAddr(String terminalAddr) {
		this.terminalAddr = terminalAddr;
	}

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public AutoTaskConfig getTaskConfig() {
		return taskConfig;
	}

	public void setTaskConfig(AutoTaskConfig taskConfig) {
		this.taskConfig = taskConfig;
		// this.taskType = taskConfig.get
	}

	/**
	 * 消息类型，1:正常；2:立即执行任务；3：补采任务；状态定义参见常量类：JobConstants.TASK_TYPE_*
	 * 
	 * @param taskType
	 */
	public char getTaskType() {
		return taskType;
	}

	/**
	 * 消息类型，1:正常；2:立即执行任务；3：补采任务；4:数据招测；5：参数设置；状态定义参见常量类：JobConstants.TASK_TYPE_
	 * *
	 * 
	 * @param taskType
	 */
	public void setTaskType(char taskType) {
		this.taskType = taskType;
	}

	/**
	 * 采集对象是电表时，获取本终端下所有的电表信息
	 * 
	 * @return
	 */
//	public List<MeterInfo> getCollMeters() {
//		if (!hasSetcollMeters) {
//			this.collMeters = TaskMeterItemCache.getMeterItemsByTerminal(this);
//		}
//
//		return this.collMeters;
//	}
//
//	public void setCollMeters(List<MeterInfo> meterItem) {
//		this.collMeters = meterItem;
//		hasSetcollMeters = true;
//	}
	/**
	 * 测量点名称
	 * 
	 * @param cpName
	 */
	public String getCpName() {
		return cpName;
	}

	/**
	 * 测量点名称
	 * 
	 * @param cpName
	 */
	public void setCpName(String cpName) {
		this.cpName = cpName;
	}

	public int getHasRedoCount() {
		return hasRedoCount;
	}

	public void setHasRedoCount(int hasRedoCount) {
		this.hasRedoCount = hasRedoCount;
	}

	public int getIsSave() {
		return isSave;
	}

	public void setIsSave(int isSave) {
		this.isSave = isSave;
	}

	public String getTerminalId() {
		return terminalId;
	}

	public void setTerminalId(String terminalId) {
		this.terminalId = terminalId;
	}

	public String getMeterIdSql() {
		return meterIdSql;
	}

	public void setMeterIdSql(String meterIdSql) {
		this.meterIdSql = meterIdSql;
	}
}
