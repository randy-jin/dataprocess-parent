package com.tl.easb.coll.api;

import com.tl.easb.coll.base.redis.LuaScriptManager;
import com.tl.easb.utils.template.RedisOperation;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * 召测监控类。
 * <p>
 * 为了不影响实时写入的性能，监控类建议使用主-从Redis实例组中的从实例。
 * 
 * @author JerryHuang
 * 
 *         2014-3-11 下午2:42:18
 */
public class ZcMonitorManager {
	/**
	 * 计算召测测点信息集中未被处理的测点总数
	 * 
	 * @param task
	 * @param step
	 *            计算步长。如果是300W终端，建议设置在100W左右。如果自己无法估算step步长，则填0，系统自动按总长的1/3作为步长
	 * @return
	 */
	public static long countLeftCps2(final String task, final int step) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
			public Long doWithJedis(Jedis jedis) {
				List<String> keys = new ArrayList<String>(3);
				keys.add(ZcKeyDefine.KPREF_TASK2SUBTASK + task);
				keys.add(ZcKeyDefine.KPREF_SUBTASK2CP);
				keys.add(String.valueOf(step));

				Long amount = (Long) LuaScriptManager.evalsha(jedis,
						"lua/LeftCpCounter2.mylua", keys);
				return amount;
			}
		});
	}

	/**
	 * 判断任务下属的子任务是否全部被下发分配
	 * 
	 * @param task
	 *            一级任务编号
	 * @return 如果true,则下属子任务被完全分配，否则表示还存在子任务未被分配
	 */
	public static boolean isTaskAllDistributed(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Boolean>() {
			public Boolean doWithJedis(Jedis jedis) {
				long offset = 0;
				long subtaskLength = 0;
				Transaction tx = jedis.multi();
				Response<Long> rLength = tx.llen(ZcKeyDefine.KPREF_TASK2SUBTASK + task);
				Response<String> rOffset = tx.get(ZcKeyDefine.KPREF_TASKOFFSET + task);
			    tx.exec();
			    offset=Long.parseLong(rOffset.get());
			    subtaskLength=rLength.get();
				return offset >= subtaskLength;
			}
		});
	}
}
