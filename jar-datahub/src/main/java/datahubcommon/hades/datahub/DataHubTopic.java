package datahubcommon.hades.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import datahubcommon.sqlconfig.SG3761SqlMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class DataHubTopic {

    private final static Logger logger = LoggerFactory.getLogger(DataHubTopic.class);

    @Autowired
    private DataHubControl dataHubControl;

    private RecordType recordType;
    private RecordSchema recordSchema;
    private String desc;
    private String topicName;
    private String fields;
    private int shardCount;
    private int lifeCycle;
    private List<String> activeShardList;

    public List<String> getActiveShardList() {
        return activeShardList;
    }

    public void setActiveShardList(List<String> activeShardList) {
        this.activeShardList = activeShardList;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public void setLifeCycle(int lifeCycle) {
        this.lifeCycle = lifeCycle;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public synchronized RecordType getRecordType() {
        return recordType;
    }

    public synchronized void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public synchronized RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public synchronized void setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    public synchronized String getDesc() {
        return desc;
    }

    public synchronized void setDesc(String desc) {
        this.desc = desc;
    }

    public DataHubTopic(String businessDataitemId) throws Exception {
        try {
            this.topicName = SG3761SqlMapping.getInstance().getTopicName(businessDataitemId);
            this.activeShardList = DataHubShardCache.getActiveShard(null, DataHubProps.project, topicName);
            this.shardCount = activeShardList.size();
            this.fields = SG3761SqlMapping.getInstance().getFields(businessDataitemId);
            this.lifeCycle = SG3761SqlMapping.getInstance().getLifeCycle(businessDataitemId);
        } catch (Exception e) {
            throw new Exception("获取DataHub Topic信息异常，businessDataitemId=" + businessDataitemId + "：", e);
        }
    }

    public DataHubTopic topic() {
        try {
            if (null == fields) {
                return null;
            }
            this.recordSchema = new RecordSchema();
            String[] field = fields.split(",");
            for (String _fieldType : field) {
                if (_fieldType == null || "".equals(_fieldType)) {
                    throw new RuntimeException("DataHub topic Field " + _fieldType + " can not be null,please check file sql-mapping.xml");
                }
                String[] _field_type = _fieldType.trim().split(":");
                if (_field_type.length != 2) {
                    throw new RuntimeException("DataHub topic Field " + _fieldType + " configured error,please check file sql-mapping.xml");
                }
                String _field = _field_type[0];
                String _type = _field_type[1].toUpperCase();
                Field dataField = null;
                if (FieldType.valueOf(_type).equals(FieldType.BIGINT)) {
                    dataField = new Field(_field, FieldType.BIGINT);
                } else if (FieldType.valueOf(_type).equals(FieldType.DOUBLE)) {
                    dataField = new Field(_field, FieldType.DOUBLE);
                } else if (FieldType.valueOf(_type).equals(FieldType.BOOLEAN)) {
                    dataField = new Field(_field, FieldType.BOOLEAN);
                } else if (FieldType.valueOf(_type).equals(FieldType.TIMESTAMP)) {
                    dataField = new Field(_field, FieldType.TIMESTAMP);
                } else if (FieldType.valueOf(_type).equals(FieldType.STRING)) {
                    dataField = new Field(_field, FieldType.STRING);
                } else {
                    logger.error("Not exists FieldType:" + FieldType.valueOf(_type));
                }
                recordSchema.addField(dataField);
            }
            this.recordType = RecordType.TUPLE;
            this.desc = topicName;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }
}
