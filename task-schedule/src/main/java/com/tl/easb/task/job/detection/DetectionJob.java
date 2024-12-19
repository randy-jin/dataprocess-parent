package com.tl.easb.task.job.detection;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.StatefulJob;
import org.quartz.Trigger;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunHandle;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunInfoHandle;
import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.task.manage.AutoTaskManage;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.CacheUtil;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自动任务检测【采集结果标志表】
 * 
 * @author JinZhiQiang
 * @date 2014年3月8日
 */
public class DetectionJob implements StatefulJob {
	static Logger log = LoggerFactory.getLogger(DetectionJob.class);

	private Scheduler scheduler;
	private AutoTaskConfig taskConfig;
	private String autotaskId;
	private BigDecimal leftMpCounter = new BigDecimal(0);
	private char taskType;

	public void execute(JobExecutionContext jobexecutioncontext) {
		try {
			Trigger trigger = jobexecutioncontext.getTrigger();
			scheduler = jobexecutioncontext.getScheduler();
			JobDataMap jobDataMap = trigger.getJobDataMap();
			autotaskId = (String) jobDataMap.get(JobConstants.QUARTZ_TASK_KEY);
			taskType = jobDataMap.getChar(JobConstants.TASK_TYPE);
			// 获取任务状态
			String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
			if (null == taskStatus) {
				log.error("从redis缓存中无法获取任务【" + autotaskId + "】的状态信息");
				return;
			}
			// 获取任务对应的配置信息
			taskConfig = AutoTaskManage.findTaskConfig(autotaskId);
			if (null == taskConfig) {
				return;
			}
			// 补采次数
			int runCount = Integer.parseInt(taskStatus[3]);
			//应采总数
			int itemTotal = Integer.parseInt(taskStatus[6]);
			// 采集数据日期
			String colDataDate = taskStatus[1];
			taskConfig.setDataDate(colDataDate);
			// 0:从历史数据表生成、1:根据任务配置重新生成、2:从缓存直接获取
			String dataSrc = taskStatus[4];
			if (!dataSrc.equals(SubTaskDefine.STATUS_COLLINFO_FROM_CACHE)) {
				// 判断缓存任务队列是否完全分配
				if (!CacheUtil.isTaskAllDistributed(autotaskId, colDataDate)) {
					// 安排下次监控任务
					assignDetectionTask();
					return;
				}
			}

			// 判断任务是否执行完成
			boolean finished = CacheUtil.checkTaskComplete(autotaskId, colDataDate,itemTotal,taskConfig.getIfBroadCast());
			// 判断任务是否超过最大时长
			// 最多执行时长
			int runMaxTime = taskConfig.getRunMaxTime();// 单位：分钟
			String startExecTime = taskStatus[0];
			if (runMaxTime != 0) {
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
				Date date = format.parse(startExecTime);
				long s = (System.currentTimeMillis() - date.getTime()) / (1000 * 60);// 当前时间与执行时间间隔分钟数
				if (s >= runMaxTime) {
					finished = true;
					log.info("任务【" + autotaskId + "】第【" + runCount + "】次已执行" + s + "分钟,本次采集结束！");
				}
			}
			if (finished) {
				log.info("任务【" + autotaskId + "】第【" + runCount + "】次采集执行结束");
				// 将任务状态置为“停止”
				CacheUtil.setTaskPause(autotaskId);
				// 获取采集失败的测点数
				leftMpCounter = CacheUtil.getLeftMpCounter(autotaskId, colDataDate);
				// 更新任务运行明细表
				BigDecimal successRatio = RautotaskRunInfoHandle.update(autotaskId, leftMpCounter);
				// 如果采集成功率为100%，则不需要补采，如果采集任务为当日曲线，则补采条件也不再生效
				if (successRatio.compareTo(new BigDecimal(100)) == 0 || runCount >= taskConfig.getRetryCount()
						|| taskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ) {
					log.info("任务【" + autotaskId + "】第【" + runCount + "】次执行结束");
					RautotaskRunHandle.update(autotaskId);
					// 删除采集任务相关信息
					CacheUtil.clearTaskAbout(autotaskId, colDataDate);
					// **********************************************************************
					if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)) {// 普通任务
						if (!DateUtil.beforeYesterday(colDataDate)) {// 如果抄历史日冻结，直接入历史表
							// 3：实时数据，4：日冻结数据，7：曲线数据，71：当日曲线数据
							if (taskConfig.getItemsScope() != MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ) {
								if (!theTriggerLastFire()) {//当前任务并非当天最后一轮执行
									return;
								}else {//当前任务当天最后一轮执行
//									if(theLastTriggerFire()){//所有任务中，当天最后一个执行的任务
//										//TODO如果之后没有任务执行，执行flushdb操作
//										try {
//											CacheUtil.clearDB();
//										} catch (Exception e) {
//											log.error("redis flushDb :",e);
//										}
//									}
								}
							}
						}
						if (taskConfig.getrScope() == MainTaskDefine.R_SCOPE_CP) {// 测量点
								if (taskConfig.getCpSql().toLowerCase().indexOf("mped_id") > -1) {// 需要临时缓存
									// 任务结束时将任务信息从内存中清除。
									CacheUtil.delTaskScope(autotaskId);
								}
							// 将缓存中的失败信息入历史表，
							// todo hch 同时在写入历史表后清理相关任务缓存
							CacheUtil.startThread(taskConfig, colDataDate);
						} else if (taskConfig.getrScope() == MainTaskDefine.R_SCOPE_TMNL) {// 采集范围终端
							log.info("当前任务采集范围终端，直接清除缓存！");
							CacheUtil.clearTaskCpData(autotaskId, colDataDate);
						} else if (taskConfig.getrScope() == MainTaskDefine.R_SCOPE_TOTAL) {// 采集范围总加组
							log.info("当前任务采集范围总加组，直接清除缓存！");
							CacheUtil.clearTaskCpData(autotaskId, colDataDate);
						}
					} else if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {// 透传任务
						if (taskConfig.getCpSql().toLowerCase().indexOf("mped_id") > -1) {// 需要临时缓存
							log.info("透传任务【" + autotaskId + "】即将清除临时缓存");
							// 任务结束时将任务信息从内存中清除。
							CacheUtil.delTaskScope(autotaskId);
						} else {

						}
						// 将缓存中的失败信息入历史表
						CacheUtil.startThread(taskConfig, colDataDate);
					}

				} else {// 安排补采
					runCount++;
					log.info("任务【" + autotaskId + "】即将安排第【" + runCount + "】次补采");
					assignRedoTask(runCount);
				}
			} else {
				// 安排下次监控任务
				assignDetectionTask();
				return;
			}
		} catch (Exception e) {
			log.error("DetectionJob Excetion:", e);
		}
	}

	/**
	 * 是否为当天本任务最后一次执行 返回值为TRUE：本任务最后一次执行 FALSE：不是最后一次执行
	 * 
	 * @return
	 */
	private boolean theTriggerLastFire() {
		Calendar tomorrow = Calendar.getInstance();
		tomorrow.add(Calendar.DAY_OF_MONTH, 1);
		tomorrow.set(Calendar.HOUR_OF_DAY, 0);
		tomorrow.set(Calendar.MINUTE, 0);
		tomorrow.set(Calendar.SECOND, 0);
		tomorrow.set(Calendar.MILLISECOND, 0);
		Date nowTime = null;
		Date nextFireTime = null;
		try {
			String[] triggerGroupNames = scheduler.getTriggerGroupNames();
			for (String triggerGroupName : triggerGroupNames) {
				// 自动任务
				if (triggerGroupName.equals(JobConstants.QUARTZ_TRIGGER_GROUP_COLLECT)) {
					String[] triggerNames = scheduler.getTriggerNames(triggerGroupName);
					for (String triggerName : triggerNames) {
						if (triggerName.contains(autotaskId)) {
							Trigger trigger = scheduler.getTrigger(triggerName, triggerGroupName);
							Date _tempNextFireTime = trigger.getNextFireTime();
							if (_tempNextFireTime.before(tomorrow.getTime())) {
								if (nextFireTime == null) {
									nextFireTime = _tempNextFireTime;
								}
								if (nextFireTime.before(_tempNextFireTime)) {
									nextFireTime = _tempNextFireTime;
								}
							} else {
								continue;
							}
						}
					}
				}
			}
			if (taskType == JobConstants.TASK_TYPE_IMMEDIATELY || taskType == JobConstants.TASK_TYPE_REDO) {
				nowTime = new Date();
				if (null != nextFireTime) {
					if (nextFireTime.after(tomorrow.getTime())) {
						return true;
					} else {
						return nowTime.after(nextFireTime);
					}
				} else {
					return true;
				}
			}
		} catch (SchedulerException e) {
			log.error("判断当前任务是否为本trigger最后一次执行的任务时发生异常：", e);
		}
		if (null == nextFireTime) {
			return true;
		}
		return nextFireTime.after(tomorrow.getTime());
	}

	/**
	 * 是否为当天最后一个执行的任务 返回值为TRUE：本日最后一次执行 FALSE：不是本日最后一次执行
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean theLastTriggerFire() throws Exception  {
		Date currentNextFireTime = null;
		boolean flag = false;
		String[] triggerGroupNames;
		try {
			triggerGroupNames = scheduler.getTriggerGroupNames();
			for (String triggerGroupName : triggerGroupNames) {
				if (triggerGroupName.equals(JobConstants.QUARTZ_TRIGGER_GROUP_COLLECT)) {
					String[] triggerNames = scheduler.getTriggerNames(triggerGroupName);
					for (String triggerName : triggerNames) {
						Trigger trigger = scheduler.getTrigger(triggerName, triggerGroupName);
						if (triggerName.contains(autotaskId)) {
							currentNextFireTime = trigger.getNextFireTime();
						}
						// 首先判断该trigger是否为当日最后一次触发
						if (theTriggerLastFire()) {
							if (currentNextFireTime.before(trigger.getNextFireTime())) {
								flag = false;
								return flag;
							} else {
								flag = true;
							}
						} else {
							flag = false;
						}
					}
				} else {
					flag = true;
				}
			}
		} catch (SchedulerException e) {
			log.error("判断当前任务是否为当日最后一个执行的任务时发生异常：", e);
		}
		return flag;
	}

	/**
	 * 重新安排检测任务
	 */
	private void assignDetectionTask() {
		synchronized (DetectionJob.class) {
			Trigger tr = null;
			try {
				JobDetectionStart.autoTaskConfig = taskConfig;
				tr = scheduler.getTrigger(JobDetectionStart.getDetectionTriggerName(),
						JobConstants.TASK_DETECTION_GROUP);
				if (tr != null) {
					scheduler.deleteJob(JobDetectionStart.getDetectionJobName(), JobConstants.TASK_DETECTION_GROUP);
				}
				// 重新安排检测任务
				SimpleTrigger simpleTrigger = JobDetectionStart.getSimpleTrigger(leftMpCounter.intValue(), taskType);
				JobDetail jobDetail = JobDetectionStart.getJobDetail();
				scheduler.scheduleJob(jobDetail, simpleTrigger);
			} catch (SchedulerException e) {
				log.error("删除或者安排新的JOB" + JobConstants.TASK_DETECTION_GROUP + "-" + JobConstants.TASK_DETECTION_NAME
						+ "异常：", e);
				log.error("trigger【" + JobConstants.TASK_DETECTION_GROUP + "】和【" + JobConstants.TASK_DETECTION_NAME
						+ "】的值为：" + tr);
				log.error("将重新调用执行assignDetectionTask()方法");
				assignDetectionTask();
			}
		}
	}

	/**
	 * 安排补采任务
	 */
	private void assignRedoTask(int runCount) {
		try {
			log.info("安排补采任务，任务号：[" + taskConfig.getAutoTaskId() + "]，补采次数：[" + runCount + "]");
			AutoTaskManage.getInstance().assignRedoTask(taskConfig, runCount);
		} catch (SchedulerException e) {
			log.error("安排补采任务失败！", e);
		}
	}

}
