package com.tl.easb.task.job;

import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.utils.CacheUtil;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;
import org.quartz.Trigger;


import com.tl.easb.task.JobConstants;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.handle.mainschedule.control.MainTaskManage;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunHandle;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunInfoHandle;
import com.tl.easb.task.job.detection.JobDetectionStart;
import com.tl.easb.task.manage.AutoTaskManage;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @描述
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public abstract class BaseJob implements StatefulJob {

	private static Logger log = LoggerFactory.getLogger(BaseJob.class);

	protected AutoTaskConfig taskConfig;
	private Trigger trigger = null;
	protected Integer hasRedoCount = 0;
	private String colDataDate = null;

	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		trigger = ctx.getTrigger();
		String autoTaskId = (String) trigger.getJobDataMap().get(JobConstants.QUARTZ_TASK_KEY);
		// 获取当前任务补采次数
		Object o = trigger.getJobDataMap().get(JobConstants.TASK_REDO_COUNT);
		if (o != null){
			hasRedoCount = Integer.parseInt((String) o);
		}
		if (hasRedoCount == null){
			hasRedoCount = 0;
		}
		// 获取任务对应的配置信息
		taskConfig = AutoTaskManage.findTaskConfig(autoTaskId);
		if(null == taskConfig){
			return;
		}
		// 获取当前采集数据日期
		Object oDataDate = trigger.getJobDataMap().get(JobConstants.TASK_COL_DATA_DATE);
		if(oDataDate != null){
			colDataDate = String.valueOf(oDataDate);
			// 设置采集数据日期
			taskConfig.setDataDate(colDataDate);
		}
		// 任务处理
		deal(autoTaskId);
	}

	/**
	 * 方法说明：执行主调度服务
	 *
	 * Author：       朱嗣珍                
	 * Create Date：  2014-3-8
	 *
	 */
	public void deal(String autoTaskId){
		boolean flag = false;
		String collectDate = null;
		String startExecTime= DateUtil.getCurrentDate("yyyy/MM/dd HH:mm:ss");
		if (null != this.getTaskConfig().getState()&& this.getTaskConfig().getState().equals("01")) {
			if (JobConstants.TASK_TYPE_REDO == getType()) {//任务状态 补采
				//执行补采
				flag = MainTaskManage.recollent(autoTaskId,hasRedoCount);
			}else if(JobConstants.TASK_TYPE_COMMON == getType()){//任务状态正常
				// 获取当前任务采集日期
				collectDate = MainTaskManage.getCollectDate(taskConfig);
				if(null == collectDate){
					collectDate = taskConfig.getDataDate();
					collectDate = collectDate.substring(0,4)+"/"+collectDate.substring(4,6)+"/"+collectDate.substring(6,8);
				}
				//执行非补采（正常采集）
				flag = MainTaskManage.collectInfo(autoTaskId,collectDate,startExecTime,taskConfig);
				if(flag){
					String[] status= CacheUtil.getTaskStatus(autoTaskId);
					int itemTotal = 0;  //应采总数
					if(null != status){
						itemTotal = Integer.parseInt(status[SubTaskDefine.STATUS_PARAMS_COLL_TOTAL_NUM]);
					}
					//初始化任务运行汇总表
					RautotaskRunHandle.init(taskConfig, startExecTime, collectDate, itemTotal);
				}
			}else if(JobConstants.TASK_TYPE_IMMEDIATELY == getType()){//任务状态 立即执行（临时）
				// 获取当前任务采集日期
				Object oDataDate = trigger.getJobDataMap().get(JobConstants.TASK_COL_DATA_DATE);
				String collectDateTmp = "";
				if(oDataDate != null){
					collectDateTmp = String.valueOf(oDataDate);
				}

				if(taskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ||taskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_MON_READ || taskConfig.getItemsScope() == MainTaskDefine.DATA_FLAG_DIRECT_MON_FREEZE_DATA){
					// 获取当前任务采集日期
					collectDate = MainTaskManage.getCollectDate(taskConfig);
				} else {
					collectDate = collectDateTmp.substring(0,4)+"/"+collectDateTmp.substring(4,6)+"/"+collectDateTmp.substring(6,8);
				}
				//执行非补采（正常采集）
				flag = MainTaskManage.collectInfo(autoTaskId,collectDate,startExecTime,taskConfig);
				if(flag){
					String[] status= CacheUtil.getTaskStatus(autoTaskId);
					int itemTotal = 0;  //应采总数
					if(null != status){
						itemTotal = Integer.parseInt(status[SubTaskDefine.STATUS_PARAMS_COLL_TOTAL_NUM]);
					}
					//初始化任务运行汇总表
					RautotaskRunHandle.init(taskConfig, startExecTime, collectDate, itemTotal);
				}
			}
			//初始化任务运行明细表
			if(flag){
				RautotaskRunInfoHandle.init(autoTaskId, startExecTime);
				JobDetectionStart.init(taskConfig,getType());
			}
		} else {
			log.info("任务【"+autoTaskId+"】，已被设置为暂停");
		}
	}

	/**
	 * 获取任务配置信息
	 *
	 * @return
	 * @throws RuntimeException
	 */
	public AutoTaskConfig getTaskConfig() throws RuntimeException {
		if (this.taskConfig == null)
			throw new RuntimeException("任务尚未执行，无法取到任务信息！");
		return this.taskConfig;
	}

	/**
	 * 设置当前任务类型，具体类型参见：<br>
	 * JobConstants.TASK_TYPE_*
	 *
	 * @return
	 */
	public abstract char getType();

}
