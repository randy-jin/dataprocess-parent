package com.tl.easb.task.job.common;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.job.common.exec.ProcessExecJob;
import com.tl.easb.task.job.common.procedure.Procedure;
import com.tl.easb.task.job.common.procedure.ProcedureHandle;
import com.tl.easb.task.manage.AutoTaskAssign;
import com.tl.easb.task.manage.QuartzManager;
import com.tl.easb.task.param.ParamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通用任务安排，分别从配置文件或数据库中安排任务
 * @author JinZhiQiang
 * @date 2014年4月15日
 */
public class CommonJobAssign {
	private static final Logger log = LoggerFactory.getLogger(CommonJobAssign.class);

	public Map<String, CommonBaseJob[]> executers;

	public void setExecuters(Map<String, CommonBaseJob[]> executers) {
		this.executers = executers;
	}

	/**
	 * 逐个安排任务
	 * @param procedure
	 * @param triggerType 0：新增 1：修改  2：删除 3：立即执行
	 */
	public void assignJobFromDb(Procedure procedure, String ... triggerType){
		try{
			AutoTaskAssign autoTaskAssign = new AutoTaskAssign(procedure, false);
			List<CronTrigger> triggers = autoTaskAssign.buildTaskCronTrigger();
			for(int i=0;i<triggers.size();i++){
				ProcessExecJob job = new ProcessExecJob();
				job.setCronExpression(triggers.get(i).getCronExpression());
				job.setParams(procedure.getPackCode()+"."+procedure.getProCode());
				String triggerName = null;
				if(0==i){
					triggerName = procedure.getPackCode()+"."+procedure.getProCode()+"."+procedure.getPdId();
				} else {
					triggerName = procedure.getPackCode()+"."+procedure.getProCode()+"."+procedure.getPdId()+"#"+i;
				}
				job.setTriggerName(triggerName);
				if(0 == triggerType.length){
					assignNewJob(job, JobConstants.TASK_PROC_DB_GROUP,procedure.getPdId().toString(),procedure.getSpId().toString());
				} else if("3".equals(triggerType[0])){
					assignNewImJob(job, JobConstants.TASK_PROC_DB_IM_GROUP,procedure);
					return;
				}
			}

			if(null == triggers || triggers.size() < 1){
				if(procedure.getRunCycle() == 0){
					ProcessExecJob job = new ProcessExecJob();
					job.setParams(procedure.getPackCode()+"."+procedure.getProCode());
					String triggerName = procedure.getPackCode()+"."+procedure.getProCode()+"."+procedure.getPdId();
					job.setTriggerName(triggerName);
					if(0 == triggerType.length){
						assignNewJob(job, JobConstants.TASK_PROC_DB_GROUP,procedure.getPdId().toString(),procedure.getSpId().toString());
					} else if("3".equals(triggerType[0])){
						assignNewImJob(job, JobConstants.TASK_PROC_DB_IM_GROUP,procedure);
						return;
					}
				}
			}
		} catch (Exception e) {
			log.error("从数据库中安排任务异常：",e);
		}
	}

	/**
	 * 从数据库中安排所有任务
	 */
	public void assignJobFromDbAll(){
		List<Procedure> procedures = ProcedureHandle.getProcedures();//GROUP_JOB_COLLECT
		try{
			// 移除该任务下分组
			QuartzManager.removeTriggerByGroupName(JobConstants.TASK_PROC_DB_GROUP);
			for(Procedure procedure : procedures){
				assignJobFromDb(procedure);
			}
		} catch (Exception e) {
			log.error("从数据库中安排任务异常：",e);
		}
	}

	/**
	 * 从配置文件中安排任务
	 */
	public void assignJobFromFile() {
		if (executers == null || executers.size() == 0)
			return;
		Iterator<String> it = this.executers.keySet().iterator();
		try {
			while (it.hasNext()) {
				String groupName = it.next();
				// 移除该任务下分组
				QuartzManager.removeTriggerByGroupName(groupName);

				CommonBaseJob[] groupExecuters = this.executers.get(groupName);
				for (int i = 0; i < groupExecuters.length; i++) {
					assignNewFileJob(groupExecuters[i], groupName);
				}
			}
		} catch (Exception e) {
			log.error("安排新任务异常@CommonJobAssign.assignJobFromFile:", e);
		}
	}

	/**
	 * 安排存储过程立即执行任务
	 * @param commonBaseJob
	 * @param groupName
	 * @param pdId
	 * @param spId
	 * @throws SchedulerException
	 */
	@SuppressWarnings("static-access")
	public void assignNewImJob(CommonBaseJob commonBaseJob, String groupName, Procedure procedure) throws SchedulerException{
		String pdId = procedure.getPdId().toString();
		String spId = procedure.getSpId().toString();
		String dataDate = procedure.getDataDate();
		JobDetail jobDetail = new JobDetail(commonBaseJob.getTriggerName(), groupName, commonBaseJob.getClass());
		SimpleTrigger simpleTrigger = new SimpleTrigger();
		simpleTrigger.setMisfireInstruction(simpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
		simpleTrigger.setName(commonBaseJob.getTriggerName());
		simpleTrigger.setGroup(groupName);
		simpleTrigger.setRepeatCount(0);
		setJobDataMap(simpleTrigger, pdId, spId, commonBaseJob, dataDate);
		Calendar cal = Calendar.getInstance() ;
		cal.add(Calendar.SECOND, ParamConstants.TASK_SCHEDULE_NEWJOB_START_INTERVEL);
		simpleTrigger.setStartTime(cal.getTime());
		QuartzManager.getScheduler().scheduleJob(jobDetail, simpleTrigger);
	}

	private void assignNewFileJob(CommonBaseJob commonBaseJob, String groupName) throws ParseException, SchedulerException{
		assignNewJob(commonBaseJob, groupName,null,null);
	}

	/**
	 * 安排新任务
	 * 
	 * @param commonBaseJob
	 * @throws ParseException
	 * @throws SchedulerException
	 */
	private void assignNewJob(CommonBaseJob commonBaseJob, String groupName, String pdId, String spId) throws ParseException, SchedulerException {
		JobDetail job = new JobDetail(commonBaseJob.getTriggerName(), groupName, commonBaseJob.getClass());
		CronTrigger trigger = new CronTrigger(commonBaseJob.getTriggerName(), groupName, commonBaseJob.getCronExpression());
		setJobDataMap(trigger, pdId, spId, commonBaseJob,null);
		Trigger tr = QuartzManager.getScheduler().getTrigger(commonBaseJob.getTriggerName(), groupName);
		if(tr != null){
			QuartzManager.getScheduler().deleteJob(trigger.getJobName(), groupName);
		}
		QuartzManager.scheduleJob(job, trigger);
	}

	private void setJobDataMap(Trigger trigger,String pdId, String spId, CommonBaseJob commonBaseJob, String colDataDate){
		trigger.getJobDataMap().put("params", commonBaseJob.getParams());
		trigger.getJobDataMap().put("triggerName", commonBaseJob.getTriggerName());
		if(null != colDataDate){
			trigger.getJobDataMap().put("DATA_DATE", colDataDate);
		}
		if(null != spId){
			trigger.getJobDataMap().put("SP_ID", spId);
		}
		if(null != pdId){
			trigger.getJobDataMap().put("PD_ID", pdId);
			trigger.getJobDataMap().put(JobConstants.QUARTZ_TASK_KEY, pdId);
		}
	}
}
