package com.tl.easb.utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import redis.clients.jedis.Jedis;

import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.coll.api.ZcMonitorManager;
import com.tl.easb.coll.api.ZcTaskManager;
import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.task.handle.subtask.Task;
import com.tl.easb.task.param.ParamConstants;

public class CacheTools {

	final static String insertOnLineSql = "insert into temp_r_tmnl_run_redis_status(AREA_CODE,TERMINAL_ADDR,AREA_CODE_TERMINAL_ADDR,STATUS,STATUS_TIME) values(?,?,?,?,?)";
	final static String insertDocSql = "insert into temp_r_tmnl_run_mped_redis(AREA_CODE,TERMINAL_ADDR,AREA_CODE_TERMINAL_ADDR,MPED_INDEX,MPED_ID) values (?,?,?,?,?)";
	final static String instanceId = "autotaskInfo";
	final static int step = 1000;
	static int subArrlen = 0;
	static int i = 0;
	static int allNum = 0;

	public static void main(String[] args) {
//		new FileSystemXmlApplicationContext("config/spring/*.xml");
		new FileSystemXmlApplicationContext("classpath:spring/datasource.xml");
		new FileSystemXmlApplicationContext("classpath:spring/redis-cache.xml");
		Jedis jedis = CacheUtil.getJedis();
//		Map<String,String> map = ZcDataManager.getAllTaskStatus(jedis);
//		System.out.println(map.size());
//		Task task = getTask(map);
//		System.out.println(task.getTaskId());
		
//		String jedisInfo= jedis.info("Replication");
//		String hosts=jedisInfo.substring(jedisInfo.indexOf("ip=")+3, jedisInfo.indexOf("ip=")+15);
//		int portNo=Integer.valueOf(jedisInfo.substring(jedisInfo.indexOf("port=")+5, jedisInfo.indexOf("port")+9));
//		Jedis jedis1=new Jedis(hosts, portNo);
		
//		List<Map<String, String>> a=jedis.sentinelSlaves("mymaster3");
//		System.out.println("000");
//		
//		ConcurrentHashMap<String, Jedis> map = RedisCacheManager.getClientMap();
//		for(String key:map.keySet()){
//			Jedis dis = map.get(key);
//			System.out.println(dis.getClient().getHost());
//			
//		}
//		System.out.println(map.size());
//		System.out.println(jedis1.info("Replication"));
		
		
		System.out.println("---------------------------------getLeftMpCounter-------------------------------------------");
		String taskId = "200019698067";
		long startTime1 = System.currentTimeMillis();
//		BigDecimal big1 = CacheUtil.getLeftMpCounter(jedis, taskId, "20161228000000");
//		System.out.println(big1);
		
		
		String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(taskId,null);
//		List<String> subTaskIds = ZcTaskManager.drawSubtasks(jedis,wrapAutoTaskId,ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
//		System.out.println(subTaskIds.size());
//		Set<String> cpSet = ZcTaskManager.getCps(jedis, subTaskIds);
//		CacheUtil.clearTaskAllCpData(jedis, taskId);
		
//		System.out.println(cpSet.size());
		System.out.println(System.currentTimeMillis()-startTime1);
//		jedis1.close();
//		System.out.println("---------------------------------getLeftMpCounter2------------------------------------------");
		
//		long startTime2 = System.currentTimeMillis();
//		BigDecimal big2 = CacheUtil.getLeftMpCounter2(jedis, "200012442157",null);
//		System.out.println(big2);
//		System.out.println(System.currentTimeMillis()-startTime2);
		
//		System.out.println("---------------------------------countLeftCps-----------------------------------------------");
		
//		long startTime3 = System.currentTimeMillis();
//		long big3 = ZcMonitorManager.countLeftCps2(jedis, "100000014564_20161213000000", 0);
//		System.out.println(big3);
//		System.out.println(System.currentTimeMillis()-startTime3);
//		
		
		/**从redis中导出档案信息到数据库**/
//		getRedisDocInfo(jedis, "M$*");
		/**从redis中导出上下线信息到数据库**/
//		getRedisOnLineInfo(jedis, "S$*");
//		execCmd(jedis, instanceId);
		CacheUtil.returnJedis(jedis);
	}
	
	private static Task getTask(final Map<String, String> taskStatusMap){
		Task task = new Task();

//		Iterator<Entry<String, String>> iter = taskStatusMap.entrySet().iterator();
//
//		while (iter.hasNext()) {
//			Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
//			String taskId = entry.getKey();
//			String[] taskStatusParams = entry.getValue().split(SubTaskDefine.SEPARATOR);
//
//			//如果任务不在执行中，那么跳过
//			if(SubTaskDefine.STATUS_EXEC_FLAG_STOP.equals(taskStatusParams[SubTaskDefine.STATUS_PARAMS_EXEC_FLAG])){
//				continue;
//			}
//			//如果任务的优先级是最高的，则设置subTask并跳出循环
//			if(SubTaskDefine.STATUS_PRIORITY_MAX == Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_PRIORITY])){
//				task.setTaskId(taskId);
//				task.setTaskStatusParams(taskStatusParams);
//				break;
//			}
//
//			//如果老的优先级低于taskStatusParams的优先级，把优先级高的子任务信息存储到subtask对象中，1为最高，9为最低
//			if(task.getStatusParamsPriority() > Integer.parseInt(taskStatusParams[SubTaskDefine.STATUS_PARAMS_PRIORITY])){
//				task.setTaskId(taskId);
//				task.setTaskStatusParams(taskStatusParams);
//			}
//		}
		return task;
	}

	private static void execCmd(Jedis jedis, String instanceid2) {
		Set<String> set = jedis.smembers("STCP:100002230355_4105_3347_20140613000000");
		Iterator<String> iterator = set.iterator();
		while(iterator.hasNext()){
			String key = iterator.next();
			System.out.println(key);
		}
	}

	/**
	 * 上下线信息
	 * @param jedis
	 * @param preStr
	 */
	private static void getRedisOnLineInfo(Jedis jedis, String preStr){
		Set<String> set = jedis.keys(preStr);
		Iterator<String> iterator = set.iterator();
		subArrlen = 5;
		Object[][] params = new Object[step][subArrlen];
		int allNum = 0;
		int i = 0;
		while(iterator.hasNext()){
			String key = iterator.next();
			Map<byte[],byte[]> map = jedis.hgetAll(key.getBytes());
			Object[] arr = new Object[subArrlen];
			String areaCode = null;
			String tmnlAddr = null;
			String areaCodeTmnlCode = null;
			try {
				areaCode = key.substring(key.indexOf("$")+1, key.indexOf("#"));
				tmnlAddr = key.substring(key.indexOf("#")+1, key.length());
				areaCodeTmnlCode = key.substring(key.indexOf("$")+1, key.length());
				Integer.parseInt(areaCode);
			} catch (Exception e) {
				System.out.println("行政区码：["+areaCode+"]，终端地址：["+tmnlAddr+"]");
				continue;
			}
			if(StringUtil.isEmpty(areaCode) || StringUtil.isEmpty(tmnlAddr) || StringUtil.isEmpty(areaCodeTmnlCode)){
				continue;
			}
			arr[0] = areaCode;
			arr[1] = tmnlAddr;
			arr[2] = areaCodeTmnlCode;
			Object on = map.get("ON");
			Object time = map.get("TIME");
			System.out.println(on);
			System.out.println(time);
//			arr[3] = (Integer)(map.get("ON"));
//			arr[4] = (Date)(map.get("TIME"));
			params[i]=arr;
			int len = i + 1;
			if(len >= step){
				persis(params,insertOnLineSql);
				allNum += params.length;
				params = null;
				params = new Object[step][len];
				i = 0;
				continue;
			}
			i ++;
		}
		if(i > 0){
			Object[][] lastParams = new Object[i][subArrlen];
			int j = 0;
			for(Object[] _temp : params){
				if(StringUtil.isEmpty(String.valueOf(_temp[0])) || StringUtil.isEmpty(String.valueOf(_temp[1])) || StringUtil.isEmpty(String.valueOf(_temp[2]))){
					continue;
				}
				lastParams[j] = _temp;
				j++;
			}
			persis(lastParams,insertOnLineSql);
			allNum += lastParams.length;
			System.out.println("上下线信息导出完毕，共计："+allNum);
		}
	}

	/**
	 * 档案信息
	 * @param jedis
	 * @param preStr
	 */
	private static void getRedisDocInfo(Jedis jedis, String preStr){
		Set<String> set = jedis.keys(preStr);
		Iterator<String> iterator = set.iterator();

		if(preStr.contains("S")){
			subArrlen = 3;
		} else if(preStr.contains("M")) {
			subArrlen = 5;
		}
		String[][] params = new String[step][subArrlen];

		while(iterator.hasNext()){
			String key = iterator.next();
			String[] arr = new String[subArrlen];
			String areaCode = null;
			String tmnlAddr = null;
			String areaCodeTmnlCode = null;
			try {
				areaCode = key.substring(key.indexOf("$")+1, key.indexOf("$")+5);
				tmnlAddr = key.substring(key.indexOf("$")+5, key.length());
				areaCodeTmnlCode = key.substring(key.indexOf("$")+1, key.length());
				Integer.parseInt(areaCode);
			} catch (Exception e) {
				System.out.println("行政区码：["+areaCode+"]，终端地址：["+tmnlAddr+"]");
				continue;
			}
			if(StringUtil.isEmpty(areaCode) || StringUtil.isEmpty(tmnlAddr) || StringUtil.isEmpty(areaCodeTmnlCode)){
				continue;
			}
			arr[0] = areaCode;
			arr[1] = tmnlAddr;
			arr[2] = areaCodeTmnlCode;
			if(preStr.contains("M")) {
				Map<String, String> map = jedis.hgetAll(key);

				if(null == map || map.size() == 0){
					return;
				}
				Iterator<String> iterator1 = map.keySet().iterator();
				while(iterator1.hasNext()){
					String[] _arr = arr.clone();
					String _key = iterator1.next();
					if(!_key.contains("P")){
						continue;
					}
					String _value = map.get(_key);
					if(_value.contains("	")){
						_value = _value.replace("	", "").trim();
					}
					_key = _key.replace("P", "");
					_arr[3] = _key;
					_arr[4] = _value;

					params[i]=_arr;

					int len = i + 1;
					if(len >= step){
						persis(params,insertDocSql);
						params = null;
						params = new String[step][len];
						i = 0;
						continue;
					}
					i ++;
				}
			}
		}

		if(i > 0){
			String[][] lastParams = new String[i][subArrlen];
			int j = 0;
			for(String[] _temp : params){
				if(StringUtil.isEmpty(_temp[0]) || StringUtil.isEmpty(_temp[1]) || StringUtil.isEmpty(_temp[2])){
					continue;
				}
				lastParams[j] = _temp;
				j++;
			}
			persis(lastParams, insertDocSql);
			System.out.println("档案导出完毕！");
		}
	}

	private static void persis(Object[][] params, String sql){
		Connection con = null;
		try{
			con = DBUtils.getConnection();
			DBUtils.execBatchUpdate(con, sql, params);
		} finally {
			DBUtils.close(con);
		}
	}

}
