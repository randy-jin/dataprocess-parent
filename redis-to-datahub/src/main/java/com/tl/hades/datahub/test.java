package com.tl.hades.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.*;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class test {
	private Jedis jedis; 
	private static final String CONF_FILE_NAME = "." + File.separator + "datahub.conf";
	private static final String ACCESS_ID = "default.access.id";
	private static final String ACCESS_KEY = "default.access.key";
	private static final String ENDPOINT = "default.endpoint";
	private static final String PROJECT = "default.project";
	private static final String TOPIC = "default.topic";
	private static String accessId = null;
	private static String accessKey = null;
	private static String endpoint = null;
	private static String project = "easfz";
	private static String topic = "bt_e_branch_red_source";



	public static void main(String args[])   throws IOException, InterruptedException {

		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(new Date(1592386519000L)));
		loadconfig();     
		long timestamp = System.currentTimeMillis(); //1466653664022
		// String stringfiled="我的";

		Account account = new AliyunAccount(accessId, accessKey);
		DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
		//构建一个新的客户端调用DataHub服务方法。
		DatahubClient client = new DatahubClient(conf);
		List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
		//表信息 对象
		RecordSchema topicSchema = client.getTopic(project, topic).getRecordSchema();
		List<String> list = DataHubShardCache.getActiveShard(client, project, topic);
		for(int n=0;n<10;n++){
			RecordEntry entry = new RecordEntry(topicSchema);//根据表信息创建字段对象 （这个字段按顺序依次表示整个表的字段）
			for (int i=0; i < entry.getFieldCount(); i++) { //从字段对象取字段总数
				entry.setString(i, ""+System.currentTimeMillis()); //根据字段的类型设置字段的值
			}
			BigDecimal shardId = new BigDecimal(System.currentTimeMillis()).remainder(new BigDecimal(list.size()));
			entry.setShardId("4"); //字段对象 设置通道id
//			entry.setPartitionKey("aa");
			recordEntries.add(entry);//将字段对象添加的字段对象列表
		}
//		ListShardResult list = client.listShard(project, topic);
//		Iterator<ShardEntry> iterator = list.getShards().iterator();
//		while(iterator.hasNext()){
//			ShardEntry shardEntry = iterator.next();
//			if(shardEntry.getState().name().equals("ACTIVE")){
//				System.out.println(shardEntry.getShardId());
//			}
//		}
		
		PutRecordsResult putRecordsResult = client.putRecords(project, topic, recordEntries);//通过新客户端将list数据入datahub（项目名， 表名，通道名，字段值）
		//得到鼠标指针的操作  参数：项目名，主题名，cursor的Id,光标类型
		GetCursorResult cursor = client.getCursor(project, topic, "0", GetCursorRequest.CursorType.LATEST);
		//根据表信息获取表结果集 取出1条记录
		GetRecordsResult r = client.getRecords(project, topic, "0", cursor.getCursor(), 1, topicSchema);
		//判断记录集的首个记录的值 ??
		//	        Assert.assertEquals(r.getRecords().get(0).getString(0), timestamp);//
		System.out.println("Smoke Success!");

	}

	/*   private static void loadconfig() throws IOException {
	    	endpoint = "http://dh-cn-hangzhou.aliyuncs.com";
	    	project = "hntl";
			accessId = "j8E7rBBDCaW9lwfz";
			accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
			topic = "FREEZE0DF161";
	    }*/

	private static void loadconfig() throws IOException {
		// Properties properties = new Properties();
		//properties.load(new FileInputStream(CONF_FILE_NAME));
		//URL url = this.getClass().getClassLoader().getResource("datahub.conf");
		endpoint = "http://datahub.cn-zhengzhou-hndl-d01.dh.easc.ha.sgcc.com.cn";
		project = "hntl";
		accessId = "j8E7rBBDCaW9lwfz";
		accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
		topic = "jzq_test";

		//	       accessId = properties.getProperty(ACCESS_ID);
		//	        accessKey = properties.getProperty(ACCESS_KEY);
		//	        endpoint = properties.getProperty(ENDPOINT);
		//	        project = properties.getProperty(PROJECT);
		//	        topic = properties.getProperty(TOPIC);
	}





}
