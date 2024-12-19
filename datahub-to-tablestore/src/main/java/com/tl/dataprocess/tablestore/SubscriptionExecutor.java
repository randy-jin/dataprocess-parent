package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.datahub.SingleSubscriptionAsyncExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 从DataHub订阅写入TableStore父类
 *
 * @author jinzhiqiang
 */
public class SubscriptionExecutor extends SingleSubscriptionAsyncExecutor {
    private static Logger logger = LoggerFactory.getLogger(SubscriptionExecutor.class);
    protected static ExecutorService writeExecutor = null;

    protected String vcolumnName;

    protected String tablestoreTableName;
//    protected Map<String, String> tablestorePrimaryKeyMap;

    protected int tablestoreWriteSize;
    protected int datahubSearchSize;

    public int getDatahubSearchSize() {
        return datahubSearchSize;
    }

    public void setDatahubSearchSize(int datahubSearchSize) {
        this.datahubSearchSize = datahubSearchSize;
    }

    public void setVcolumnName(String vcolumnName) {
        this.vcolumnName = vcolumnName;
    }

//    public void setTablestorePrimaryKeyList(Map<String, String> tablestorePrimaryKeyList) {
//        this.tablestorePrimaryKeyMap = tablestorePrimaryKeyList;
//    }

    public void setIsRun(int isRun) {
        this.isRun = isRun;
    }

    public void setTablestoreWriteSize(int tablestoreWriteSize) {
        this.tablestoreWriteSize = tablestoreWriteSize;
    }

    public void setTablestoreTableName(String tablestoreTableName) {
        this.tablestoreTableName = tablestoreTableName;
    }

    public void setDatahubClient(DatahubClient datahubClient) {
        this.datahubClient = datahubClient;
    }

    public void setTablestorePrimaryKey(String tablestorePrimaryKey) {
        this.tablestorePrimaryKey = tablestorePrimaryKey;
    }

    @Override
    protected void init() {

    }

    @Override
    protected long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
        return 0;
    }

    protected String tablestorePrimaryKey;

    protected boolean tableStoreConstructor(Object request, RecordEntry record) {
        String dataTime = record.getString("data_time");
        // 构造rowPutChange1
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        //// TODO: 2021/8/26  hch 测试接入apollo
        String[] pkStrs = tablestorePrimaryKey.split("#");
        for (String str : pkStrs) {
            String key = str.split(":")[0];
            String type = str.split(":")[1];
            if (type.equals("int")) {
                pkBuilder.addPrimaryKeyColumn(key, PrimaryKeyValue.fromLong(record.getBigint(key)));
            } else if (type.equals("string")) {
                pkBuilder.addPrimaryKeyColumn(key, PrimaryKeyValue.fromString(record.getString(key)));
            }
        }

        RowUpdateChange rowUpdateChange = new RowUpdateChange(tablestoreTableName, pkBuilder.build());
        rowUpdateChange.setReturnType(ReturnType.RT_PK);
        Condition condition = new Condition(RowExistenceExpectation.IGNORE);
        rowUpdateChange.setCondition(condition);

        rowUpdateChange.put(new Column("data_point_flag", ColumnValue.fromLong(record.getBigint("data_point_flag"))));
        rowUpdateChange.put(new Column("org_no", ColumnValue.fromString(record.getString("org_no"))));
        rowUpdateChange.put(new Column("shard_no", ColumnValue.fromString(record.getString("org_no").substring(0, 5))));

        if (null != dataTime) {
            if (record.getDouble(vcolumnName) == null || record.getDouble(vcolumnName).equals("Infinity") || record.getDouble(vcolumnName).equals("NaN")) {
                logger.info(vcolumnName + "'s value is: " + record.getDouble(vcolumnName));
                return true;
            }
            rowUpdateChange.put(new Column(vcolumnName + dataTime, ColumnValue.fromDouble(record.getDouble(vcolumnName))));
        }

        // 添加到batch操作中
        if (request instanceof BatchWriteRowRequest) {
            BatchWriteRowRequest batchWriteRowRequest = (BatchWriteRowRequest) request;
            batchWriteRowRequest.addRowChange(rowUpdateChange);
        } else if (request instanceof TableStoreWriter) {
            TableStoreWriter tableStoreWriter = (TableStoreWriter) request;
            tableStoreWriter.addRowChange(rowUpdateChange);
        }

        return false;
    }
}