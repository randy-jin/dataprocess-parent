package com.tl.sqlMapping;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class SqlMapping {

	private String pnType;//pn可能是测量点，总加组，直流模拟量（T或者P）
	private String projectName;//datahub工程名
	private String topicName;//主题名
	private int shardCount;//分片数
	private int lifeCycle;//表数据生命周期
	private String fields;//主题字段
	private String businessDataitemIds;

	public String[] getBusinessDataitemIdArray(){
		return this.businessDataitemIds.split(",");
	}


}
