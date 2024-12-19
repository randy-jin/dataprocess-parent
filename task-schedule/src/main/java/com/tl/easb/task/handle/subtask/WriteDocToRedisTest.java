package com.tl.easb.task.handle.subtask;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.util.StringUtils;

import com.ls.pf.base.common.persistence.utils.DBUtils;

/**
 * 档案加载测试类
 * 
 * @date 2014年4月12日
 */
public class WriteDocToRedisTest {
	private static final String SEPARATOR = "#";
	static HashMap<Integer,String> map = new HashMap<Integer,String>();
	private static AtomicInteger counter = new AtomicInteger(0);
	private static AtomicInteger dup = new AtomicInteger(0);

	// 根据本机内存配置可修改该值，建议如果jvm最大内存为512M的情况下配置为1000~3000，如果为1024M可配置为5000~10000

	public static void main(String args[]) {
		// 组成：开始时间_采集日期_优先级_补采次数_采集信息生成方式_执行标识_应采总数
		//		ApplicationContext factory = new FileSystemXmlApplicationContext("classpath:spring/*.xml");
		//		operateBzData =factory.getBean("operateBzData",OperateBzData.class);

		ResultSet rs = DBUtils.executeQuery("select shard_no,count(1) from R_TMNL_INFO group by SHARD_NO", null);
		try {
			while (rs.next()) {
				final String shardNo = rs.getString("shard_no");
				Thread thread = new Thread(new Runnable() {
					public void run() {
						long startTime = System.currentTimeMillis();
						String GET_CP_AND_TMNL_RUN_INFO_SQL = "SELECT RUN.ORG_NO, RUN.TERMINAL_ID, RUN.CP_NO, RUN.TERMINAL_ADDR, RUN.AREA_CODE, RUN.PROTOCOL_ID FROM R_TMNL_INFO RUN WHERE RUN.SHARD_NO = ? ";
						ResultSet tmnlInfoRs = DBUtils.executeQuery(GET_CP_AND_TMNL_RUN_INFO_SQL, new Object[] { shardNo });
						try {
							while(tmnlInfoRs.next()){
								String areaCode = tmnlInfoRs.getString("AREA_CODE");
								String terminalAddr = tmnlInfoRs.getString("TERMINAL_ADDR");
								if(!StringUtils.hasLength(areaCode) || !StringUtils.hasLength(terminalAddr)){
									continue;
								}
								counter.incrementAndGet();
								String doc = "M$"+areaCode+SEPARATOR+terminalAddr;
								int hash = doc.hashCode();
								if(map.containsKey(hash)){
									String s = (String) map.get(hash);
									dup.incrementAndGet();
									System.out.println(s + ":" + doc); 
								} else {
									map.put(hash, doc);
								}
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
						System.out.println("地市"+shardNo+"耗时["+(System.currentTimeMillis()-startTime)+"]毫秒");
						System.out.println(counter.get() + ":" + dup.get()); 
					}
				}, "sync_thread-" + shardNo);
				thread.start();
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}
}
