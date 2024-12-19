//package com.tl.handes.redistodatahup;
//
//import com.aliyun.datahub.DatahubClient;
//import com.aliyun.datahub.DatahubConfiguration;
//import com.aliyun.datahub.auth.Account;
//import com.aliyun.datahub.auth.AliyunAccount;
//import com.aliyun.datahub.common.data.RecordSchema;
//import com.aliyun.datahub.model.GetCursorRequest;
//import com.aliyun.datahub.model.GetCursorResult;
//import com.aliyun.datahub.model.GetRecordsResult;
//import com.aliyun.datahub.model.RecordEntry;
//import redis.clients.jedis.Jedis;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class Todatahub {
//	private static Jedis jedis;
//	private static final String CONF_FILE_NAME = "." + File.separator + "datahub.conf";
//	private static final String ACCESS_ID = "default.access.id";
//	private static final String ACCESS_KEY = "default.access.key";
//	private static final String ENDPOINT = "default.endpoint";
//	private static final String PROJECT = "default.project";
//	private static final String TOPIC = "default.topic";
//	private static String accessId = null;
//	private static String accessKey = null;
//	private static String endpoint = null;
//	private static String project = null;
//	private static String topic = null;
//
//	private static void loadconfig() throws IOException {
//		endpoint = "http://dh-cn-hangzhou.aliyuncs.com";
//		project = "hntl";
//		accessId = "j8E7rBBDCaW9lwfz";
//		accessKey = "0nehrSudL9AhBPgBfQlCeLw2HCWnkc";
//		topic = "fromredis";
//	}
//
//	public static void rediscontent() {
//		jedis = new Jedis("127.0.0.1", 6379);
//	}
//
//	public static List<String> redisdata() {
//		List<String> list = new ArrayList();
//		Long n = jedis.llen("ooo");
//
//		for (int i = 0; i < n; i++) {
//			long number = 0;
//			String num = jedis.lpop("ooo");// 里面的int是以string的形式存的
//
//			if (num != null | num != "") {
//				System.out.println("redis data:" + num);
//				list.add(num);
//			} else {
//				break;
//			}
//		}
//		return list;
//	}
//
//	public static void main(String args[]) throws IOException, Exception {
//		loadconfig();
//		rediscontent();
//		Account account = new AliyunAccount(accessId, accessKey);
//		DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
//		DatahubClient client = new DatahubClient(conf);
//		List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
//		// 表信息 对象
//		RecordSchema topicSchema = client.getTopic(project, topic).getRecordSchema();
//		GetCursorResult cursor = client.getCursor(project, topic, "0", GetCursorRequest.CursorType.LATEST);
//		GetRecordsResult r = client.getRecords(project, topic, "0", cursor.getCursor(), 10, topicSchema);
//		System.out.println("=== " + r.getRecordCount()); // 获取一个记录
//
//		while (true) {
//			List<String> list1 = null;
//			list1 = redisdata();
//			if (list1.size() == 0) {
//				System.out.println("等待读取redis队列数据...");
//				Thread.sleep(2000);
//				 continue;
//			}
//			System.out.println("读取redis记录条数:" + list1.size());
//			for (int i = 0; i < list1.size(); i++) {
//				RecordEntry entry = new RecordEntry(topicSchema);// 创建字段集合对象
//				entry.setShardId("0");
//				entry.setBigint(0, Long.valueOf(list1.get(i)).longValue());// 这里0是字段的位置
//																			// 因为本表只有一个字段
//				recordEntries.add(entry);// 将字段对象添加到字段对象列表
//			}
//			client.putRecords(project, topic, recordEntries);//
//
//			Thread.sleep(1000);
//
//		}
//		// GetCursorResult cursor = client.getCursor(project, topic, "0",
//		// GetCursorRequest.CursorType.LATEST);
//		// 根据表信息获取表结果集 取出1条记录
//		// GetRecordsResult r = client.getRecords(project, topic, "0",
//		// cursor.getCursor(), 1, topicSchema);
//		// 判断记录集的首个记录的值 ??
//		// Assert.assertEquals(r.getRecords().get(0).getBigint(0).longValue(),
//		// timestamp);
//
//
//	}
//
//}
