package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.exception.OffsetSessionChangedException;
import com.aliyun.datahub.exception.SubscriptionOfflineException;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SingleSubscriptionExample {
	private static Logger logger = LoggerFactory.getLogger(SingleSubscriptionExample.class);
	//	private String datahub_accessId = "j8E7rBBDCaW9lwfz";
	//	private String datahub_accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
	//	private String datahub_endpoint = "http://dh-cn-hangzhou.aliyuncs.com";
	//	private String datahub_projectName = "hntl";
	//	private String datahub_topicName = "e_mp_power_curve_ud";
	//	private String datahub_subId = "15162711697021vjHl";
	//	private String datahub_shardId = "0";
	//
	//	private String tablestore_endPoint = "http://easotstest.cn-hangzhou.ots.aliyuncs.com";
	//	private String tablestore_accessId = "j8E7rBBDCaW9lwfz";
	//	private String tablestore_accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
	//	private String tablestore_instanceName = "easotstest";
	//	private static final String TABLE_NAME = "e_mp_power_curve_2";
	//	private static final String PRIMARY_KEY_ID = "id";
	//	private static final String PRIMARY_KEY_DATA_DATE = "data_date";
	//	private static final String PRIMARY_KEY_NAME_DATA_TYPE = "data_type";
	//	private static final int tablestore_write_size = 200;

	private String datahub_accessId = "pm7Im9jRRTkGzygs";
	private String datahub_accessKey = "g2kP8GOa93r10JSKjHL1BngyKhcGc9";
	private String datahub_endpoint = "http://datahub.cn-zhengzhou-hndl-d01.dh.easc.ha.sgcc.com.cn";
	private String datahub_projectName = "eas";
	private String datahub_topicName = "e_mp_power_curve_ud";
	private String datahub_subId = "1516359198279u25rq";
	private String datahub_shardId = "1";

	private String tablestore_endPoint = "http://eas.cn-zhengzhou-hndl-d01.ots-internal.easc.ha.sgcc.com.cn";
	private String tablestore_accessId = "vuhIXj3kCEFBDi1a";
	private String tablestore_accessKey = "R34gZ2b5GlVFHZV8PREqR1ipk7ZsC5";
	private String tablestore_instanceName = "eas";
	private static final String TABLE_NAME = "e_mp_power_curve_1";
	private static final String PRIMARY_KEY_ID = "id";
	private static final String PRIMARY_KEY_DATA_DATE = "data_date";
	private static final String PRIMARY_KEY_NAME_DATA_TYPE = "data_type";
	private static final int tablestore_write_size = 200;

	private DatahubConfiguration conf;
	private DatahubClient datahubClient;
	private SyncClient syncClient;

	public SingleSubscriptionExample() {
		this.conf = new DatahubConfiguration(new AliyunAccount(datahub_accessId, datahub_accessKey), datahub_endpoint);
		this.datahubClient = new DatahubClient(conf);
		this.syncClient = new SyncClient(tablestore_endPoint, tablestore_accessId, tablestore_accessKey, tablestore_instanceName);
	}

	public void Start() {
		try {
			boolean bExit = false;
			GetTopicResult topicResult = datahubClient.getTopic(datahub_projectName, datahub_topicName);
			// 首先初始化offset上下文
			OffsetContext offsetCtx = datahubClient.initOffsetContext(datahub_projectName, datahub_topicName, datahub_subId, datahub_shardId);
			String cursor = null; // 开始消费的cursor
			if (!offsetCtx.hasOffset()) {
				// 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
				GetCursorResult cursorResult = datahubClient.getCursor(datahub_projectName, datahub_topicName, datahub_shardId, CursorType.OLDEST);
				cursor = cursorResult.getCursor();
			} else {
				// 否则，获取当前已消费点位的下一个cursor
				cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
			}
			logger.info("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
			long recordNum = 0L;
			while (!bExit) {
				try {
					GetRecordsResult recordResult = datahubClient.getRecords(datahub_projectName, datahub_topicName, datahub_shardId, cursor, 1000, topicResult.getRecordSchema());
					List<RecordEntry> records = recordResult.getRecords();
					if (records.size() == 0) {
						// 将最后一次消费点位上报
						datahubClient.commitOffset(offsetCtx);
						//						logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
						// 可以先休眠一会，再继续消费新记录
						Thread.sleep(1000);
						//						logger.info("sleep 1s and continue consume records! shard id:" + datahub_shardId);
					} else {
						recordNum = batchWriteRow(offsetCtx, recordNum, records);
						cursor = recordResult.getNextCursor();
					}
				} catch (SubscriptionOfflineException e) {
					// 订阅下线，退出
					bExit = true;
					e.printStackTrace();
				} catch (OffsetResetedException e) {
					// 点位被重置，更新offset上下文
					datahubClient.updateOffsetContext(offsetCtx);
					cursor = datahubClient.getNextOffsetCursor(offsetCtx).getCursor();
					logger.info("Restart consume shard:" + datahub_shardId + ", reset offset:" + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
				} catch (OffsetSessionChangedException e) {
					// 其他consumer同时消费了该订阅下的相同shard，退出
					bExit = true;
					e.printStackTrace();
				} catch (Exception e) {
					bExit = true;
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private long batchWriteRow(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
		int recordNumCycle = 0; 
		BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
//		long PK_ID = 0L;
//		String PK_DATA_DATE = null;
//		long PK_DATA_TYPE = 0L;
		for (RecordEntry record : records) {
			String dataTime = record.getString("data_time");
			long pk_id = record.getBigint(PRIMARY_KEY_ID);
			String pk_data_date = record.getString(PRIMARY_KEY_DATA_DATE);
			long pk_data_type = record.getBigint(PRIMARY_KEY_NAME_DATA_TYPE);
			// 构造rowPutChange1
			PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
			pkBuilder.addPrimaryKeyColumn(PRIMARY_KEY_ID, PrimaryKeyValue.fromLong(pk_id));
			pkBuilder.addPrimaryKeyColumn(PRIMARY_KEY_DATA_DATE, PrimaryKeyValue.fromString(pk_data_date));
			pkBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_DATA_TYPE, PrimaryKeyValue.fromLong(pk_data_type));
//			if(!(PK_ID == pk_id && PK_DATA_DATE.equals(pk_data_date) && PK_DATA_TYPE == pk_data_type)){
//				RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, pkBuilder.build());
//				rowPutChange.setReturnType(ReturnType.RT_PK);
//				// 添加一些列
//				rowPutChange.addColumn(new Column("data_point_flag", ColumnValue.fromLong(record.getBigint("data_point_flag"))));
//				rowPutChange.addColumn(new Column("org_no", ColumnValue.fromString(record.getString("org_no"))));
//				rowPutChange.addColumn(new Column("SHARD_NO", ColumnValue.fromString(record.getString("org_no").substring(0, 5))));
//				// 添加到batch操作中
//				batchWriteRowRequest.addRowChange(rowPutChange);
//				recordNumCycle ++;
//
//				PK_ID = pk_id;
//				PK_DATA_DATE = pk_data_date;
//				PK_DATA_TYPE = pk_data_type;
//			}
			RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pkBuilder.build());
			rowUpdateChange.setReturnType(ReturnType.RT_PK);
			Condition condition = new Condition(RowExistenceExpectation.IGNORE);
			rowUpdateChange.setCondition(condition);

			rowUpdateChange.put(new Column("data_point_flag", ColumnValue.fromLong(record.getBigint("data_point_flag"))));
			rowUpdateChange.put(new Column("org_no", ColumnValue.fromString(record.getString("org_no"))));
			rowUpdateChange.put(new Column("SHARD_NO", ColumnValue.fromString(record.getString("org_no").substring(0, 5))));

			if(null != dataTime){
				//	        	logger.info(record.getDouble("v"));
				if(record.getDouble("v") == null){
					logger.error("",record.getDouble("v"));
					continue;
				}
				rowUpdateChange.put(new Column("p"+dataTime, ColumnValue.fromDouble(record.getDouble("v"))));				
			}
			// 添加到batch操作中
			batchWriteRowRequest.addRowChange(rowUpdateChange);
			recordNumCycle ++;

			// 处理记录逻辑
			// logger.info("Consume shard:" + datahub_shardId + " thread process record:"
			//        + record.toJsonNode().toString());
			// 上报点位，该示例是每处理100条记录上报一次点位
			offsetCtx.setOffset(record.getOffset());
			recordNum++;
			if(records.size() >= tablestore_write_size && recordNumCycle % tablestore_write_size == 0){
				doBatchWritRow(batchWriteRowRequest);
//				datahubClient.commitOffset(offsetCtx);
				batchWriteRowRequest = new BatchWriteRowRequest();
			} else if(records.size() < tablestore_write_size && recordNum==records.size()){
				doBatchWritRow(batchWriteRowRequest);
//				datahubClient.commitOffset(offsetCtx);
			}
			//			logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
		}
		if(records.size() > tablestore_write_size && records.size()%tablestore_write_size != 0){
			doBatchWritRow(batchWriteRowRequest);
//			datahubClient.commitOffset(offsetCtx);
		}
		datahubClient.commitOffset(offsetCtx);
		return recordNum;
	}

	private void doBatchWritRow(BatchWriteRowRequest batchWriteRowRequest) {
		BatchWriteRowResponse response = syncClient.batchWriteRow(batchWriteRowRequest);
		//		logger.info("是否全部成功:" + response.isAllSucceed());
		if (!response.isAllSucceed()) {
			for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
				logger.info("失败的行:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
				logger.info("失败原因:" + rowResult.getError());
			}
			/*
			 * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
			 * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
			 */
			BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
		} else {
			for (BatchWriteRowResponse.RowResult rowResult : response.getSucceedRows()) {
				PrimaryKey pk = rowResult.getRow().getPrimaryKey();
//				logger.info("Return PK:" + pk.jsonize());
			}
		}
	}

	public static void main(String[] args) {
		SingleSubscriptionExample example = new SingleSubscriptionExample();
		try {
			example.Start();

			//			logger.info("new object");
			//			int len = 100;
			//			List<String> list = new ArrayList<String>();
			//			for(int i=0;i<len;i++){
			//				list.add("str"+i);
			//			}
			//			int recordNum = 0;
			//			for(String str : list){
			//				logger.info(str);
			//				recordNum ++;
			//				if(list.size() >= 100 && recordNum % 100 == 0){
			//					logger.info("commit1");
			//					logger.info("new object"+recordNum);
			//				}else if(list.size() < 100 && recordNum==list.size()){
			//					logger.info("commit2");
			//				}
			//			}
			//			if(list.size() > 100 && list.size()%100 != 0){
			//				logger.info("commit3");
			//			}

		} catch (DatahubClientException e) {
			e.printStackTrace();
		}
	}
}
