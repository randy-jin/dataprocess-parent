package com.tl.easb.task.job.detection;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunHandle;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunInfoHandle;
import com.tl.easb.task.manage.QuartzManager;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.utils.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 设置新任务检测【采集结果标志表】
 * 
 * @author JinZhiQiang
 * @date 2014年3月8日
 */
public class JobDetectionStart {
	static Logger log = LoggerFactory.getLogger(JobDetectionStart.class);
	public static AutoTaskConfig autoTaskConfig;

	public static void init(AutoTaskConfig taskConfig, char taskType) {
		log.info("QUART调度任务检测【采集结果标志表】启动********************");
		Scheduler scheduler = QuartzManager.getScheduler();
		autoTaskConfig = taskConfig;
		assignDetectionJob(scheduler, taskType);
		log.info("QUART调度任务检测【采集结果标志表】启动完成****************");
	}

	/**
	 * 设置任务检测【采集结果标志表】
	 * 
	 * @param scheduler
	 */
	private static void assignDetectionJob(Scheduler scheduler, char taskType) {
		synchronized (JobDetectionStart.class) {
			Trigger trigger = null;
			try {
				trigger = scheduler.getTrigger(getDetectionTriggerName(), JobConstants.TASK_DETECTION_GROUP);
				if (trigger != null) {
					scheduler.deleteJob(getDetectionJobName(), JobConstants.TASK_DETECTION_GROUP);
				}
				log.info("QUART调度任务检测【采集结果标志表】启动。。。。。。。。。。。。。。。。。。。。。。");
				SimpleTrigger simpleTrigger = getSimpleTrigger(0, taskType);
				JobDetail job = getJobDetail();
				QuartzManager.scheduleJob(job, simpleTrigger);
				log.info("检测【采集结果标志表】任务，新建并启动完成！");
			} catch (Exception e) {
				log.error("检测【采集结果标志表】任务启动异常：", e);
				log.error("trigger【" + JobConstants.TASK_DETECTION_GROUP + "】和【" + getDetectionJobName() + "】的值为："
						+ trigger);
				log.error("将重新调用执行assignDetectionTask()方法");
				// assignDetectionJob(scheduler,taskType);
			}
		}
	}

	/**
	 * 生成JobDetail
	 * 
	 * @return
	 */
	public static JobDetail getJobDetail() {
		JobDetail job = new JobDetail();
		job.setGroup(JobConstants.TASK_DETECTION_GROUP);
		job.setName(getDetectionJobName());
		job.setJobClass(DetectionJob.class);
		job.setVolatility(false);
		return job;
	}

	/**
	 * 生成SimpleTrigger
	 * 
	 * @return
	 */
	public static SimpleTrigger getSimpleTrigger(int leftMpCounter, char taskType) {
		// 获取最小执行间隔时间（毫秒）
		int interval = ParamConstants.TASK_DETECTION_INTERVAL;
		// 单位数据所需时间（毫秒）
		int unitTime = ParamConstants.TASK_DETECTION_UNITTIME;
		SimpleTrigger simpleTrigger = new SimpleTrigger();
		simpleTrigger.setMisfireInstruction(simpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
		simpleTrigger.setName(getDetectionTriggerName());
		simpleTrigger.setGroup(JobConstants.TASK_DETECTION_GROUP);
		simpleTrigger.setRepeatCount(0);
		simpleTrigger.getJobDataMap().put(JobConstants.QUARTZ_TASK_KEY, autoTaskConfig.getAutoTaskId());
		simpleTrigger.getJobDataMap().put(JobConstants.TASK_TYPE, String.valueOf(taskType));
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MILLISECOND, leftMpCounter * unitTime + interval);
		simpleTrigger.setStartTime(cal.getTime());
		return simpleTrigger;
	}

	public static String getDetectionTriggerName() {
		return getName("TRIG_DET_");
	}

	public static String getDetectionJobName() {
		return getName("JOB_DET_");
	}

	private static String getName(String prtype) {
		int runCycle = autoTaskConfig.getRunCycle();
		String type = "";
		switch (runCycle) {
		case 1: // 年
			type = "YR";
			break;
		case 2: // 月
			type = "MON";
			break;
		case 3: // 周
			type = "WK";
			break;
		case 4: // 日
			type = "DAY";
			break;
		case 5: // 时
		case 6: // 30分
		case 7: // 15分
		case 8: // 5分
		case 9: // 1分
			type = "MIN";
			break;
		}
		type = type + "_";
		return prtype + type + autoTaskConfig.getAutoTaskId();

	}

	/**
	 * 任务结束处理公共方法
	 * 
	 * @param jedis
	 * @param runCount
	 * @param colDataDate
	 */
	public static void taskFinishDeal(String autotaskId, String colDataDate, AutoTaskConfig taskConfig) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		if (colDataDate != null) {
			colDataDate = df.format(new Date(colDataDate));
		}
		// 将任务状态置为“停止”
		CacheUtil.setTaskPause(autotaskId);
		// 获取采集失败的测点数
		BigDecimal leftMpCounter = CacheUtil.getLeftMpCounter(autotaskId, colDataDate);
		// 更新任务运行明细表
		try {
			RautotaskRunInfoHandle.update(autotaskId, leftMpCounter);
		} catch (Exception e) {
			log.error("强制结束任务异常:", e);
		}
		try {
			RautotaskRunHandle.update(autotaskId);
		} catch (Exception e) {
			log.error("强制结束任务异常:", e);
		}
		// 删除采集任务相关信息
		CacheUtil.clearTaskAbout(autotaskId, colDataDate);
	}
}
