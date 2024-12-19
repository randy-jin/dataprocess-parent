package com.tl.easb.coll.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;



import com.tl.easb.coll.base.redis.LuaScriptManager;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.utils.template.RedisOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * 召测过程数据处理
 * 
 * @author JerryHuang
 * 
 *         2014-2-25 下午8:02:07
 */
public class ZcProcedureManager {
	private static Logger logger = LoggerFactory.getLogger(ZcProcedureManager.class);

	/**
	 * 任务过程数据处理
	 * <p>
	 * 执行主体将输入列表数据进行分段（比如每次5000条记录），然后将每段数据作为入参，调用redis自定义脚本命令，
	 * 更新redis中的采集任务测点数据集。Redis自定义脚本逻辑如下：循环遍历每个测点标识，作以下处理：
	 * <p>
	 * 执行主体将输入列表数据进行分段（比如每次5000条记录
	 * ），然后将每段数据作为入参，调用redis自定义脚本命令，更新redis中的采集任务测点数据集
	 * 。Redis自定义脚本逻辑如下：循环遍历每个测点标识，在循环内部作以下处理：
	 * <p>
	 * 1)根据列表中第一个采集点获取到的二级任务编号，截取第一个”_”前面的字符串作为对应的一级任务编号（二级任务编码规则=$一级任务编码_+其它占位符
	 * ） 注意：该步骤必须在第4步前置性，因为第4步会删除对应项
	 * <p>
	 * 2）、根据一级任务编号，更新“一级任务更新时间”
	 * <p>
	 * 3）从“测点信息数据集”获取对应的二级任务编号
	 * <p>
	 * 4)从“测点信息数据集”里删除该测点对应的项
	 * <p>
	 * 5）从“采集任务测点数据集”删除对应的测点标识KEY
	 * <p>
	 * 注：脚本命令执行期间，redis将停止响应其它请求，因此写入分段需要控制在合理范围内，如5000条。
	 * 
	 * @param cps
	 *            被成功采集的测点标识列表
	 * @param newTimeVal
	 *            需要被更新的新的时间值
	 * @return size of processed collection points
	 */
	public static int process(List<String> cps, int batchDelSize, long newTimeVal) {
		if (cps == null || cps.isEmpty()) {
			return 0;
		}
		// update task time.this method should be invoked at first,since items
		// in dataset cp2subtask will be removed in the following process.
		Random r = new Random();
		int j = r.nextInt();
		// begin to handle process data
		int maxSize = cps.size();
		int rounds = maxSize / batchDelSize;
		if (maxSize % batchDelSize > 0) {
			rounds++;
		}
		int fromIdx = 0;
		int toIdx = 0;
		List<String> subcps = null;
//		long startTime = System.currentTimeMillis();
//		logger.info("第" + j + "次总开始操作时间:" + startTime);
		for (int i = 0; i < rounds; i++) {
			fromIdx = batchDelSize * i;
			toIdx = batchDelSize * (i + 1);
			if (toIdx > maxSize) {
				toIdx = maxSize;
			}
			subcps = cps.subList(fromIdx, toIdx);
//			logger.info("第" + j + "次第" + i + "轮，开始更新时间:" + System.currentTimeMillis());
			updTime(subcps);
//			logger.info("第" + j + "次第" + i + "轮，结束更新时间:" + System.currentTimeMillis());
//			logger.info("第" + j + "次第" + i + "轮，开始清理缓存:" + System.currentTimeMillis());
			batchProcess(subcps);
//			logger.info("第" + j + "次第" + i + "轮，结束清理缓存:" + System.currentTimeMillis());
		}
//		logger.info("第" + j + "次总执行耗时:" + (System.currentTimeMillis() - startTime));
		return maxSize;
	}
	// public static int process(Jedis jedis, List<String> cps, int batchSize,
	// long newTimeVal) {
	// if (cps == null || cps.isEmpty()) {
	// return 0;
	// }
	// // update task time.this method should be invoked at first,since items
	// // in dataset cp2subtask will be removed in the following process.
	// updTime(jedis, cps, newTimeVal);
	// // begin to handle process data
	// int maxSize = cps.size();
	// int rounds = maxSize / batchSize;
	// if (maxSize % batchSize > 0) {
	// rounds++;
	// }
	// int fromIdx = 0;
	// int toIdx = 0;
	// List<String> subcps = null;
	// for (int i = 0; i < rounds; i++) {
	// fromIdx = batchSize * i;
	// toIdx = batchSize * (i + 1);
	// if (toIdx > maxSize) {
	// toIdx = maxSize;
	// }
	// subcps = cps.subList(fromIdx, toIdx);
	// batchProcess(jedis, subcps);
	// }
	//
	// return maxSize;
	// }

	private static void batchProcess(final List<String> subcps) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				int size = subcps.size();
				List<String> keys = new ArrayList<String>();
				keys.add(String.valueOf(size));
				keys.add(ZcKeyDefine.KEY_CP2SUBTASK);
				keys.add(ZcKeyDefine.KPREF_SUBTASK2CP);
				Object obj = LuaScriptManager.evalsha(jedis, "lua/ProcedureBatchProcess.mylua", keys, subcps);
			}
		});
	}

	private static void updTime(List<String> subcps) {
		int maxSize = subcps.size();
		if (maxSize < ParamConstants.TASK_FINISH_TIMEOUT_IGNORE) {
			return;
		}
		doUpdTime(subcps);
	}

	private static void doUpdTime(final List<String> subCps) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				List<String> keys = new ArrayList<String>();
				keys.add(String.valueOf(subCps.size()));
				keys.add(ZcKeyDefine.KEY_CP2SUBTASK);
				keys.add(ZcKeyDefine.KPREF_TASKUPDTIME);
				keys.add(System.currentTimeMillis() + "");
				Object obj = LuaScriptManager.evalsha(jedis, "lua/TaskUpTime.mylua", keys, subCps);
			}
		});
	}

	// private static void updTime(Jedis jedis, List<String> cps, long time) {
	// String subtask = null;
	// String preSubtask = null;
	//// subtask = jedis.hget(ZcKeyDefine.KEY_CP2SUBTASK, cps.get(0));
	// for(String cpKey : cps){
	// subtask = jedis.hget(ZcKeyDefine.KEY_CP2SUBTASK, cpKey);
	// if(null == subtask){
	// continue;
	// } else {
	// if(null == preSubtask || !preSubtask.equals(subtask)){
	// //原格式211_4111_85_20161115000000_9000000003400273
	// String tasks = subtask.substring(0,
	// subtask.lastIndexOf("_"));//211_4111_85_20161115000000
	// StringBuffer task = new StringBuffer(tasks.substring(0,
	// tasks.indexOf("_")));//211
	// task.append("_").append(tasks.substring(tasks.lastIndexOf("_")+1,tasks.length()));//211_20161115000000
	// jedis.set(ZcKeyDefine.KPREF_TASKUPDTIME + task, String.valueOf(time));
	// preSubtask = subtask;
	// }
	// continue;
	// }
	// }
	//
	// }
}