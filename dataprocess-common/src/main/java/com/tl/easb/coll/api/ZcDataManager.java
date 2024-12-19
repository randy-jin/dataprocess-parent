package com.tl.easb.coll.api;

import com.tl.easb.coll.base.redis.LuaScriptManager;
import com.tl.easb.coll.base.utils.StackTraceUtils;
import com.tl.easb.utils.RedisTemplateInstance;
import com.tl.easb.utils.template.RedisOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 负责召测数据的加载、保存等操作。
 * 
 * @author JerryHuang
 * 
 *         2014-2-22 下午7:26:29
 */
public class ZcDataManager {
	private static Logger logger = LoggerFactory.getLogger(ZcDataManager.class);

//	private static RedisTemplate redisTemplate = RedisTemplateInstance.redisTemplate;
	private static JedisSentinelPool jedisSentinelPool = RedisTemplateInstance.jedisSentinelPool;

	/**
	 * redis模版操作示例
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		new FileSystemXmlApplicationContext("classpath:spring/datasource.xml");
		new FileSystemXmlApplicationContext("classpath:spring/redis-cache.xml");
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				jedis.lpush("aa", "dd");
			}
		});
	}

	/**
	 * 一次性初始化以下数据集:测点信息数据集(即测点与二级任务的对应关系数据）、 采集任务测点数据集(二级任务与测点的对应该关心集)
	 * 
	 *            redis客户端实例
	 * @param taskCpData
	 *            KEY:二级任务编号 VALUE:测点标识数据集合。SET类型, ELEMENT是单个测点标识
	 * @return 重复的测点数据集
	 */
	@SuppressWarnings("unchecked")
	public static List<String> initCpAndSubtaskDs(final Map<String, Set<String>> taskCpData) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<List<String>>() {
			public List<String> doWithJedis(Jedis jedis) {
				String subtask = null;
				Set<String> cps = null;
				List<String> args = new ArrayList<String>(50);
				// populate arguments' values
				for (Entry<String, Set<String>> entry : taskCpData.entrySet()) {
					subtask = entry.getKey();
					cps = entry.getValue();
					for (String cp : cps) {
						args.add(subtask);
						args.add(cp);
					}
				}
				int argSize = args.size();
				List<String> keys = new ArrayList<String>();
				keys.add(String.valueOf(argSize));
				keys.add(ZcKeyDefine.KEY_CP2SUBTASK);
				keys.add(ZcKeyDefine.KPREF_SUBTASK2CP);
				Object robj = LuaScriptManager.evalsha(jedis, "lua/PopulateCpAndTaskDs.mylua", keys, args);
				if (logger.isInfoEnabled()) {
					logger.info("[{}] elements are populated into redis", argSize);
				}
				List<String> existedCps = (List<String>) robj;

				return existedCps;
			}
		});
	}

	/**
	 * 初始化测点信息数据集(即测点与二级任务的对应关系数据）
	 * 
	 * @deprecated Use initCpAndSubtaskDs instead
	 *
	 *            redis客户端实例
	 * @param data
	 *            KEY: 测点标识； VALUE:二级任务编号
	 * @return 被处理的数据记录数
	 */
	public static long initCp2subtask(final Map<String, String> data) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {
			public Integer doWithJedis(Jedis jedis) {
				Pipeline pipeline = jedis.pipelined();
				String cpId = null;
				String subtask = null;
				for (Entry<String, String> entry : data.entrySet()) {
					cpId = entry.getKey();
					subtask = entry.getValue();
					pipeline.hset(ZcKeyDefine.KEY_CP2SUBTASK, cpId, subtask);
				}
				pipeline.sync();
				if (logger.isInfoEnabled()) {
					logger.info("[{}] elements are populated into redis", data.size());
				}
				return data.size();
			}
		});
	}

	/**
	 * 采集任务测点数据集(二级任务与测点的对应该关心集)
	 * 
	 * @deprecated Use initCpAndSubtaskDs instead
	 *            redis客户端实例
	 * @param taskCpData
	 *            KEY:二级任务编号 VALUE:测点标识数据集合。SET类型, ELEMENT是单个测点标识
	 * @return
	 */
	public static long initSubtask2CpData(final Map<String, Set<String>> taskCpData) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {
			public Integer doWithJedis(Jedis jedis) {
				Pipeline pipeline = jedis.pipelined();
				String subtask = null;
				Set<String> cps = null;
				int size = 0;
				for (Entry<String, Set<String>> entry : taskCpData.entrySet()) {
					subtask = entry.getKey();
					subtask = ZcKeyDefine.KPREF_SUBTASK2CP + subtask;
					cps = entry.getValue();
					for (String cp : cps) {
						pipeline.sadd(subtask, cp);
						size++;
					}
				}
				pipeline.sync();
				if (logger.isInfoEnabled()) {
					logger.info("[{}] elements are populated into redis", size);
				}
				return size;
			}
		});
	}

	/**
	 * 初始化一级任务-二级任务关系数据集
	 * 
	 *            redis客户端实例
	 * @param task
	 *            一级任务
	 * @param subtasks
	 *            二级任务。LIST类型。
	 * @return
	 */
	public static long initTask2Subtask(final String task, final Set<String> subtasks) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {

			public Integer doWithJedis(Jedis jedis) {
				Pipeline pipeline = jedis.pipelined();
				for (String it : subtasks) {
					pipeline.rpush(ZcKeyDefine.KPREF_TASK2SUBTASK + task, it);
				}
				pipeline.sync();
				if (logger.isInfoEnabled()) {
					logger.info("[{}] elements are populated into redis", subtasks.size());
				}
				return subtasks.size();
			}
		});
	}

	public static long initTask2Subtask(final String task, final List<String> subtasks) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {

			public Integer doWithJedis(Jedis jedis) {
				Pipeline pipeline = jedis.pipelined();
				for (String it : subtasks) {
					pipeline.rpush(ZcKeyDefine.KPREF_TASK2SUBTASK + task, it);
				}
				pipeline.sync();
				if (logger.isInfoEnabled()) {
					logger.info("[{}] elements are populated into redis", subtasks.size());
				}
				return subtasks.size();
			}
		});
	}

	/**
	 * 设置任务队列偏移量。用来存储被移取的二级任务偏移量。主要用来作任务分配和判定二级任务是否全部下发完成。
	 * 
	 * @param task
	 * @param offset
	 */
	public static void setCurrentTaskOffset(final String task, final long offset) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				jedis.set(ZcKeyDefine.KPREF_TASKOFFSET + task, String.valueOf(offset));
			}
		});
	}

	/**
	 * 设置当前一级任务更新时间。
	 * 
	 *            redis客户端实例
	 * @param task
	 *            一级任务编号
	 * @param mstime
	 *            需要设置的时间值(以毫秒为单位进行衡量，即从1970年1月1号午夜0点起(UTC)到所记录时间之间的毫秒数)
	 */
	public static void setCurrentTaskUpdTime(final String task, final long mstime) {
		if (task.contains("null")) {
			logger.error("setCurrentTaskUpdTime has a null char in [" + task + "]");
		}
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				jedis.set(ZcKeyDefine.KPREF_TASKUPDTIME + task, String.valueOf(mstime));
			}
		});
	}

	/**
	 * 删除更新时间数据
	 * 
	 * @param task
	 * @return
	 */
	public static long removeTaskUpdTime(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {

			public Long doWithJedis(Jedis jedis) {
				return jedis.del(ZcKeyDefine.KPREF_TASKUPDTIME + task);
			}
		});
	}

	/**
	 * 清除redis实例范围内所有数据。谨慎使用！
	 * 
	 *            redis客户端实例
	 */
	public static void clearAll() {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				jedis.flushAll();
			}
		});
	}

	/**
	 * 清除采集任务测点数据集（即采集二级任务与测点的对应关系）
	 * <p>
	 * 注：该方法不是线程安全的，需要在单线程环境下执行
	 * 
	 * 
	 *            redis客户端实例
	 * @param task
	 *            一级任务编号
	 * @param step
	 *            每次清除的二级任务数量
	 */
	public static void clearSubtaskCp(final String task, final int step) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				String task2subtaskKey = ZcKeyDefine.KPREF_TASK2SUBTASK + task;
				int fromIdx = 0;
				int toIdx = 0;
				while (true) {
					// exclusive
					toIdx = fromIdx + step - 1;
					List<String> subtasks = jedis.lrange(task2subtaskKey, fromIdx, toIdx);

					if (subtasks == null || subtasks.isEmpty()) {
						return;
					}
					String subtaskCpKey = null;
					Pipeline pipeline = jedis.pipelined();

					for (String subtask : subtasks) {
						subtaskCpKey = ZcKeyDefine.KPREF_SUBTASK2CP + subtask;

						pipeline.del(subtaskCpKey);
					}
					pipeline.sync();
					fromIdx = toIdx + 1;
				}
			}
		});
	}

	/**
	 * 不区分任务，清除 测试信息（测点-二级任务对应关系）数据集合的所有数据。
	 * 
	 *            redis客户端实例
	 */
	public static void clearCpInfo() {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				jedis.del(ZcKeyDefine.KEY_CP2SUBTASK);
			}
		});
	}

	/**
	 * 清除指定任务的测试-二级任务对应关系数据集
	 * 
	 * @param task
	 *            一级任务
	 * @param step
	 *            每次提交删除的二级任务数。
	 * @param waitInterval
	 *            每轮提交等候的时间。如果为小于0，则不等候，持续提交.单位：毫秒
	 */
	public static void clearCpData(final String task, final int step, final long waitInterval) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				int from = 0;
				int end = 0;
				List<String> subtasks = null;
				String task2subtaskKey = ZcKeyDefine.KPREF_TASK2SUBTASK + task;
				while (true) {
					end = from + step - 1;
					subtasks = jedis.lrange(task2subtaskKey, from, end);
					// System.out.println("subtask size:"+subtasks.size());
					if (subtasks == null || subtasks.isEmpty()) {
						break;
					}
					from = end + 1;

					// clear data in jedis
					List<String> keys = new ArrayList<String>(3);
					keys.add(String.valueOf(subtasks.size()));

					keys.add(ZcKeyDefine.KEY_CP2SUBTASK);
					keys.add(ZcKeyDefine.KPREF_SUBTASK2CP);
					String scriptFile = "lua/ClearTaskData.mylua";
					Object obj = LuaScriptManager.evalsha(jedis, scriptFile, keys, subtasks);

					if (waitInterval > 0) {
						try {
							Thread.sleep(waitInterval);
						} catch (InterruptedException e) {
							logger.error(StackTraceUtils.getStackTrace(e));
						}
					}

				}
				// remove task2subtask
				jedis.del(task2subtaskKey);
			}
		});
	}

	/**
	 * 清除 一级任务-二级任务关系数据集
	 * 
	 *            redis客户端实例
	 * @param task
	 *            一级任务编号
	 */
	public static void clearTask2subtask(final String task) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				jedis.del(ZcKeyDefine.KPREF_TASK2SUBTASK + task);
			}
		});
	}

	/**
	 * 设置任务状态数据
	 * 
	 * @param task
	 *            任务编码
	 * @param status
	 *            状态值
	 */
	public static void setTaskStatus(final String task, final String status) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {

			public void doWithJedis(Jedis jedis) {
				jedis.hset(ZcKeyDefine.KEY_TASK2STATUS, task, status);
			}
		});
	}

	/**
	 * 获取任务状态值
	 * 
	 * @param task
	 *            任务编码
	 * @return 状态值
	 */
	public static String getTaskStatus(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<String>() {
			public String doWithJedis(Jedis jedis) {
				return jedis.hget(ZcKeyDefine.KEY_TASK2STATUS, task);
			}
		});
	}

	/**
	 * 删除任务状态
	 * 
	 * @param task
	 * @return
	 */
	public static Long removeTaskStatus(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
			public Long doWithJedis(Jedis jedis) {
				return jedis.hdel(ZcKeyDefine.KEY_TASK2STATUS, task);
			}
		});
	}

	/**
	 * 删除任务队列偏移量
	 * 
	 * @param task
	 * @return
	 */
	public static Long removeTaskOffset(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
			public Long doWithJedis(Jedis jedis) {
				return jedis.del(ZcKeyDefine.KPREF_TASKOFFSET + task);
			}
		});
	}

	/**
	 * 获取所有的任务状态
	 * 
	 * @return
	 */
	public static Map<String, String> getAllTaskStatus() {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Map<String, String>>() {
			public Map<String, String> doWithJedis(Jedis jedis) {
				return jedis.hgetAll(ZcKeyDefine.KEY_TASK2STATUS);
			}
		});
	}

	/**
	 * 设置任务计数器值
	 * 
	 * @param task
	 *            任务编码
	 * @param num
	 *            计算器值
	 */
	public static void setTaskCounter(final String task, final long num) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				jedis.hset(ZcKeyDefine.KEY_TASK2COUNTER, task, String.valueOf(num));
			}
		});
	}

	/**
	 * 删除任务计算器数据
	 * 
	 * @param task
	 * @return
	 */
	public static long removeTaskCounter(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
			public Long doWithJedis(Jedis jedis) {
				return jedis.hdel(ZcKeyDefine.KEY_TASK2COUNTER, task);
			}
		});
	}

	/**
	 * 增加任务计数器值
	 * 
	 * @param task
	 *            任务编码
	 * @param incrNum
	 *            要增加的值
	 */
	public static void incrTaskCounter(final String task, final long incrNum) {
		RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
			public void doWithJedis(Jedis jedis) {
				jedis.hincrBy(ZcKeyDefine.KEY_TASK2COUNTER, task, incrNum);
			}
		});
	}

	/**
	 * 获取任务计数器值
	 * 
	 * @param task
	 *            任务编码
	 * @return 当前计数器值
	 */
	public static long getTaskCounter(final String task) {
		return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
			public Long doWithJedis(Jedis jedis) {
				String ret = jedis.hget(ZcKeyDefine.KEY_TASK2COUNTER, task);
				return ret == null ? 0L : Long.parseLong(ret);
			}
		});
	}

}
