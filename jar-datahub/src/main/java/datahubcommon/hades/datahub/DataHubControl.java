package datahubcommon.hades.datahub;


import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.GetTopicResult;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import datahubcommon.hades.persist.DataObject;
import datahubcommon.refreshkey.RefreshDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 入库控制
 * @author jinzhiqiang
 *
 */
public class DataHubControl {
	private final static Logger logger = LoggerFactory.getLogger(DataHubControl.class);

	private DatahubClient datahubClient = null;

	public DatahubClient getDatahubClient() {
		return datahubClient;
	}

	public void setDatahubClient(DatahubClient datahubClient) {
		this.datahubClient = datahubClient;
	}

	public PutRecordsResult insertion(String project, List<DataObject> doList, RefreshDataCache refreshprocessor) {
		List<RecordEntry> recordEntries = new ArrayList<RecordEntry>(doList.size());
		List<Object[]> refreshKeyArrList = new ArrayList<>(doList.size());
		String batchTopicFlag = null;
		for(DataObject dataObject : doList){
			DataHubTopic dataHubTopic = dataObject.getDataHubTopic();
			String topicName = dataHubTopic.getTopicName();
			if(batchTopicFlag == null){
				batchTopicFlag = topicName;
			}
			//此处先检查topic是否存在，不存在的话会在project中创建
			GetTopicResult topicResult = DataHubTopicMgr.getTopic(topicName);
			if(null == topicResult){
				topicResult = DataHubTopicMgr.createAndGetTopic(project, dataHubTopic);
			}
			//recordEntry录入数据
			//todo可根据实际性能情况改为批量写入
			RecordSchema topicSchema = topicResult.getRecordSchema();//表Schema

			RecordEntry entry = new RecordEntry(topicSchema);
			fillRecEntry(entry, dataObject);//字段=数据
			entry.setShardId(dataObject.getShardId()); //通道id 
			
			if(logger.isInfoEnabled()){
				logger.info("You have a new message write to project " + project + " topic " + topicName + " and shard id " + dataObject.getShardId());
			}
			if(batchTopicFlag != topicName){
				PutRecordsResult putRecordsResult = datahubClient.putRecords(project, batchTopicFlag, recordEntries);
				putRecordsResult = refresh(project, batchTopicFlag, refreshKeyArrList, refreshprocessor, putRecordsResult);
				if(putRecordsResult.getFailedRecordCount() > 0){
					logger.error("Warning!!!!!!Write to datahub " + batchTopicFlag + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId());
				}
				batchTopicFlag = topicName;
				recordEntries.clear();
				refreshKeyArrList.clear();
			} else {
				if(null != dataObject.getKey()){
					refreshKeyArrList.add(dataObject.getKey());
				}
				recordEntries.add(entry);
				continue;
			}
			if(null != dataObject.getKey()){
				refreshKeyArrList.add(dataObject.getKey());
			}
			recordEntries.add(entry);
		}
		PutRecordsResult putRecordsResult = datahubClient.putRecords(project, batchTopicFlag, recordEntries);
		putRecordsResult = refresh(project, batchTopicFlag, refreshKeyArrList, refreshprocessor, putRecordsResult);
		if(putRecordsResult.getFailedRecordCount() > 0){
			logger.error("Warning!!!!!!Write to datahub " + batchTopicFlag + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId());
		}
		recordEntries.clear();
		refreshKeyArrList.clear();

		return putRecordsResult;
	}

	private PutRecordsResult refresh(String project, String topicName, List<Object[]> refreshKeyArrList, RefreshDataCache refreshprocessor, PutRecordsResult putRecordsResult) {
		if(putRecordsResult.getFailedRecordCount() == 0){
			doRefresh(refreshKeyArrList, refreshprocessor);
		} else {
			for(RecordEntry failedRecordEntry : putRecordsResult.getFailedRecords()){
				logger.error("WriteFailed record topic is ["+topicName+"], who's jsonNode is:"+failedRecordEntry.toJsonNode());
			}
			logger.error("Warning!!!!!!Write to datahub " + topicName + " failed! one of them's shardId is " + putRecordsResult.getFailedRecords().get(0).getShardId() + ",to update DataHub-shard-cache.");
			
			
			
			DataHubShardCache.delListShardResultByTopic(project, topicName);
			List<String> activeShardList = DataHubShardCache.getActiveShard(datahubClient, project, topicName);
			for(RecordEntry recordEntry : putRecordsResult.getFailedRecords()){
				int index = Math.abs(recordEntry.hashCode()) % activeShardList.size();
				recordEntry.setShardId(activeShardList.get(index));
			}
			PutRecordsResult failedRecordsResult = datahubClient.putRecords(project, topicName, putRecordsResult.getFailedRecords());
			if(failedRecordsResult.getFailedRecordCount() == 0){
				doRefresh(refreshKeyArrList, refreshprocessor);
			}
		}
		return putRecordsResult;
	}

	/**
	 * 执行刷新
	 * @param refreshKeyArrList
	 * @param refreshprocessor
	 */
	private static void doRefresh(List<Object[]> refreshKeyArrList, RefreshDataCache refreshprocessor) {
		if(refreshKeyArrList.size() == 0){
			return;
		}
		for(Object[] refreshKey : refreshKeyArrList){
			if(null != refreshKey){
				refreshprocessor.refresh(refreshKey);
			}
		}
	}

	/**
	 * 判断字段类型并填入entry对象
	 * @param entry
	 * @param data
	 */
	private void fillRecEntry(RecordEntry entry, DataObject data) {
		List<Object> objList = data.getObjList();
		if(objList != null && objList.size() > 0) {
			int count = 0;
			for(Object obj : objList) {
				if(obj instanceof Date) {
					entry.setTimeStampInDate(count, (Date) obj);
				} else if(obj instanceof String) {
					entry.setString(count, String.valueOf(obj));
				} else if(obj instanceof Double) {
					entry.setDouble(count, Double.valueOf(String.valueOf(obj)));
				} else if(obj instanceof Integer) {
					entry.setBigint(count, Long.valueOf(String.valueOf(obj)));
				} else if(obj instanceof BigDecimal) {
					entry.setBigint(count, Long.valueOf(String.valueOf(obj)));
				}
				count++;
			}
		}
	}
}
