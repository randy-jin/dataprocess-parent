package com.ls.athena.framework.sqlconfig;

public class SqlMapping {

	//	private int afn;
	//	private String fn;//这里的code对于一类数据和二类数据是fn 对于三类数据是erc
	private String businessDataitemIds;
	private String pnType;//pn可能是测量点，总加组，直流模拟量（T或者P）
	private String projectName;//datahub工程名
	private String topicName;//主题名
	private int shardCount;//分片数
	private int lifeCycle;//表数据生命周期
	private String fields;//主题字段

	public int getLifeCycle() {
		return lifeCycle;
	}

	public String getBusinessDataitemIds() {
		return businessDataitemIds;
	}

	public void setBusinessDataitemIds(String businessDataitemIds) {
		this.businessDataitemIds = businessDataitemIds;
	}

	public void setLifeCycle(int lifeCycle) {
		this.lifeCycle = lifeCycle;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public int getShardCount() {
		return shardCount;
	}

	public void setShardCount(int shardCount) {
		this.shardCount = shardCount;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public String[] getBusinessDataitemIdArray(){
		return this.businessDataitemIds.split(",");
	}

	public String getPnType() {
		return pnType;
	}
	public void setPnType(String pnType) {
		this.pnType = pnType;
	}

}
