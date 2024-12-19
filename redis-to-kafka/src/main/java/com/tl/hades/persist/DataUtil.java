package com.tl.hades.persist;

import com.tl.hades.datahub.DataHubTopic;

import java.util.List;

public class DataUtil {
	public static Object[] makeRefreshKey(Object ... obj){
		Object[] refreshKey = new Object[obj.length];
		for(int i=0;i<obj.length;i++){
			refreshKey[i] = obj[i];
		}
		return refreshKey;
	}
	public static DataObject getDataObj(String mpedIdStr, String businessDataitemId, Object[] refreshKey, List dataValue)
			throws Exception {
		DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
		int index = Math.abs(mpedIdStr.hashCode()) % dataHubTopic.getShardCount();
		String shardId = dataHubTopic.getActiveShardList().get(index);
		DataObject dataObj = new DataObject(dataValue, refreshKey, dataHubTopic.topic(), shardId.toString());
		return dataObj;
	} 
}
