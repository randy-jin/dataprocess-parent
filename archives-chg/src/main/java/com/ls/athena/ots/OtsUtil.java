package com.ls.athena.ots;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OtsUtil {
	private final static Logger logger = LoggerFactory.getLogger(OtsUtil.class);
	/*
	 * 将核查用的档案数据写入ots tableName 表名 saveList 更新或添加终端档案信息list
	 */
	public static void putOts(SyncClient client,String tableName, List<Map<String, String>> saveList)throws ServiceException, ClientException {
		for (Map<String, String> tmp : saveList) {
			BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
			// 构造rowPutChange
			PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
			String pk = "";
			List<Column> columns = new ArrayList<Column>();
			for (String col : tmp.keySet()) {
				if ("mped_id".equals(col)) {
					pk = tmp.get(col);
				} else {
					columns.add(new Column(col,ColumnValue.fromString(tmp.get(col))));
				}
			}
			pk1Builder.addPrimaryKeyColumn("mped_id", PrimaryKeyValue.fromString(pk));
			RowPutChange rowPutChange = new RowPutChange(tableName, pk1Builder.build());
			rowPutChange.addColumns(columns);
			batchWriteRowRequest.addRowChange(rowPutChange);
			BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);
			if (!response.isAllSucceed()) {
				for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
					logger.error("失败的行:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
					logger.error("失败原因:" + rowResult.getError());
				}
			}

		}
	}
	/*
       * 将ots需要删除的档案删除
       * tableName 表名
       * rmList  删除终端档案信息list
       */
	public static void delOts(SyncClient client,String tableName, List<Map<String, String>> rmList) {
		for (Map<String, String> tmp : rmList) {
			BatchWriteRowRequest request = new BatchWriteRowRequest();
			PrimaryKeyBuilder primaryKeyBuilder2 = PrimaryKeyBuilder.createPrimaryKeyBuilder();
			primaryKeyBuilder2.addPrimaryKeyColumn("mped_id", PrimaryKeyValue.fromString(tmp.get("mped_id")));
			RowDeleteChange rowDeleteChange = new RowDeleteChange(tableName,primaryKeyBuilder2.build());
			request.addRowChange(rowDeleteChange);
			BatchWriteRowResponse response = client.batchWriteRow(request);
			if (!response.isAllSucceed()) {
				for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
					logger.error("失败的行:" + request.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
					logger.error("失败原因:" + rowResult.getError());
				}
			}

		}
	}
}