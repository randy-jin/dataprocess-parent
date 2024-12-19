package com.tl.hades.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.GetCursorRequest;
import com.aliyun.datahub.model.GetCursorResult;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.RecordEntry;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * dataHub 入库测试类
 */
public class DatahubSmoke {
    private static final String CONF_FILE_NAME = "." + File.separator + "datahub.conf";
    private static final String ACCESS_ID = "default.access.id";
    private static final String ACCESS_KEY = "default.access.key";
    private static final String ENDPOINT = "default.endpoint";
    private static final String PROJECT = "default.project";
    private static final String TOPIC = "default.topic";
    private static String accessId = null;
    private static String accessKey = null;
    private static String endpoint = null;
    private static String project = null;
    private static String topic = null;

    public static void main(String args[]) throws IOException {
        loadconfig();
//
//        Account account = new AliyunAccount(accessId, accessKey);
//        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
//        DatahubClient client = new DatahubClient(conf);
//
//        long timestamp = System.currentTimeMillis();
//
//        RecordSchema topicSchema = client.getTopic(project, topic).getRecordSchema();
//        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
//        RecordEntry entry = new RecordEntry(topicSchema);
//        for (int i=0; i < entry.getFieldCount(); i++) {
//            entry.setBigint(i, timestamp); //set your fields' value according to the field's type
//        }
//        entry.setShardId("0");
//        recordEntries.add(entry);
//
//        client.putRecords(project, topic, recordEntries);
        
        //提供了DATAHUB支持的认证账号
    	Account account = new AliyunAccount(accessId, accessKey);
    	//配置阿里云的datahub信息
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        //根据配置信息 创建datahub客户端，使用此客户端的所有服务调用都是阻塞的，将不会返回，直到服务调用完成为止
        DatahubClient client = new DatahubClient(conf);
        //从datahub中获取表信息  参数：项目名  表名
        RecordSchema topicSchema = client.getTopic(project, topic).getRecordSchema();
        //获取光标开始的结果集 参数：项目名  表名  碎片id 时间戳（最近的一条记录）
        GetCursorResult cursor = client.getCursor(project, topic, "0", GetCursorRequest.CursorType.LATEST);
        //从结果集中取记录数据，参数：项目名 表名 碎片id  光标定位  限制返回最大记录是数   字段集对象  （上面获取的是最近的一条数据，这里设置最多获取10条，其实还是获取了1条数据）
        GetRecordsResult r = client.getRecords(project, topic, "0", cursor.getCursor(), 10, topicSchema);
        System.out.println("=== " + r.getRecordCount()); //获取总记录条数
        List<RecordEntry> reList = r.getRecords();//总记录转为list
        if(reList != null && reList.size() > 0) {
        	for(RecordEntry re : reList) { //取每条记录的各个字段值
        		System.out.println(re.getFieldCount());
        		Field[] fs = re.getFields();
        		for(int i = 0; i < re.getFieldCount(); i++) {
        			//TODO findings
        			if(fs[i].getType().equals(FieldType.TIMESTAMP)) {
        				System.out.println(i + "=== " + re.getTimeStamp(i));
        			} else if(fs[i].getType().equals(FieldType.STRING)) {
        				System.out.println(i + "=== " + re.getString(i));
        			} else if(fs[i].getType().equals(FieldType.DOUBLE)) {
        				System.out.println(i + "=== " + re.getDouble(i));
        			} else if(fs[i].getType().equals(FieldType.BIGINT)) {
        				System.out.println(i + "=== " + re.getBigint(i));
        			} else if(fs[i].getType().equals(FieldType.BOOLEAN)) {
        				System.out.println(i + "=== " + re.getBoolean(i));
        			}
        		}
        	}
        }
        System.out.println("Smoke Success!");
    }

    private static void loadconfig() throws IOException {
    	endpoint = "http://dh-cn-hangzhou.aliyuncs.com";
    	project = "hntl";
		accessId = "j8E7rBBDCaW9lwfz";
		accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
//		topic = "tmall_trade_detail";
		topic = "FREEZE0DF161";
    }
}

