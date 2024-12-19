package com.tl.dataprocess.utils;




import com.tl.datahub.DataHubTopic;

import java.util.List;

public class DataObject {

	private List<Object> objList;
	private Object[] key;
	private DataHubTopic dataHubTopic;
	private String shardId;//数据要入库的分片

	public DataObject(List<Object> objList, Object[] key, DataHubTopic dataHubTopic, String shardId) {
		this.objList = objList;
		this.key = key;
		this.shardId = shardId;
		this.dataHubTopic = dataHubTopic;
	}

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public DataHubTopic getDataHubTopic() {
		return dataHubTopic;
	}

	public void setDataHubTopic(DataHubTopic dataHubTopic) {
		this.dataHubTopic = dataHubTopic;
	}

	public List<Object> getObjList() {
		return objList;
	}

	public void setObjList(List<Object> objList) {
		this.objList = objList;
	}

	public Object[] getKey() {
		return key;
	}

	public void setKey(Object[] key) {
		this.key = key;
	}
}
