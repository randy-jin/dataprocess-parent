package com.tl.easb.task.manage;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.tl.easb.task.manage.view.AutoTaskJob;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.tl.easb.task.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class QuartzManager {
	static Logger log = LoggerFactory.getLogger(QuartzManager.class);
	private static Scheduler scheduler = null;

	/**
	 * 获取当前的QuartzServer
	 * 接入apollo配置，把quartz文件内容放到apollo中
	 * @return
	 */
	public static Scheduler getScheduler() {
		if (scheduler == null) {
			try {
//				PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
//				propertiesFactoryBean.setLocation(new ClassPathResource("/quartz.properties"));
//				propertiesFactoryBean.afterPropertiesSet();
//				Properties props = propertiesFactoryBean.getObject();
//
//				String pws = props.get("org.quartz.dataSource.myDS.password").toString();
//				BASE64Decoder decoder = new BASE64Decoder();
//				String pwsDecrypt=new String(decoder.decodeBuffer(pws));
//				props.put("org.quartz.dataSource.myDS.password", pwsDecrypt);
//
//				StdSchedulerFactory factory = new StdSchedulerFactory();
//				factory.initialize(props);
//				scheduler = factory.getScheduler();

				StdSchedulerFactory factory = new StdSchedulerFactory();
				Properties props = new Properties();
				Config config=ConfigService.getConfig("quartz");
				Set<String> keySet= config.getPropertyNames();
				for (String key:keySet) {
					props.put(key,config.getProperty(key,null));
				}
				factory.initialize(props);
				scheduler = factory.getScheduler();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return scheduler;
	}

	/**
	 * 安排新任务（自动从当前Context中获取Schedule)
	 * @param job
	 * @throws SchedulerException
	 */
	public static void scheduleJob(AutoTaskJob job) throws SchedulerException {
		try {
			log.info("安排新任务：autoTaskId:[" + job.getTaskConfig().getAutoTaskId() + "] TRIGGER[" + job.getTrigger().toString() + "] JOB:[" + job.getJobDetail().toString() + "]");
			//安排新任务
			scheduleJob(job.getJobDetail(), job.getTrigger());
		} catch (SchedulerException e) {
			throw e;
		}
	}

	/**
	 * 删除旧任务
	 * @param job
	 * @throws SchedulerException
	 */
	public static void removeJob(AutoTaskJob job) throws SchedulerException{
		//删除任务
		removeJobByTaskId(job.getTaskConfig().getAutoTaskId(), job.getJobType());
	}

	/**
	 * 安排任务
	 * @param jobDetail
	 * @param trigger
	 * @throws SchedulerException
	 */
	public static void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
		Scheduler s = QuartzManager.getScheduler();
		log.info("安排新任务：jobName:[" + jobDetail.getName() + "] jobGroup[" + jobDetail.getGroup() + "] jobClz:[" + jobDetail.getJobClass().getName() + "]");

		s.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 判断JOB是否存在
	 * 
	 * @param job
	 * @return
	 * @throws SchedulerException
	 */
	public static boolean isJobExists(JobDetail job) throws SchedulerException {
		JobDetail job1 = findJobDetail(job.getName(), job.getGroup());
		if (job1 != null) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 根据任务编号，清除Job
	 * 
	 * @param autoTaskId
	 * @param jobType
	 * @return
	 * @throws SchedulerException
	 */
	public static boolean removeJobByTaskId(String autoTaskId,char jobType) throws SchedulerException {
		List<Trigger> triggers = findTaskTrigger(autoTaskId, jobType);
		Scheduler s = QuartzManager.getScheduler();
		for (Trigger t : triggers) {
			try {
				s.deleteJob(t.getJobName(), t.getJobGroup());
				log.info("删除JOB: autoTaskId [" + autoTaskId + "] JOB_NAME [" + t.getJobName() + "] JOB_GROUP [" + t.getJobGroup() + "] ");
			} catch (SchedulerException e) {
				log.error("根据任务编号，删除Job时异常@QuartzManager.removeJob，任务编号：" + autoTaskId, e);
				throw e;
			}

		}
		return true;
	}

	/**
	 * 删除指定job
	 * 
	 * @param job
	 * @throws SchedulerException
	 */
	public static void removeJob(JobDetail job) throws SchedulerException {
		Scheduler s = QuartzManager.getScheduler();
		try {
			log.info("删除JOB JobName:" + job.getName() + ",jobGroup:" + job.getGroup());
			s.deleteJob(job.getName(), job.getGroup());
		} catch (SchedulerException e) {
			log.error("删除JOB异常@QuartzManager.removeJob(JobDetail job) JobName:" + job.getName() + ",jobGroup:" + job.getGroup(), e);
			throw e;
		}
	}

	/**
	 * 删除Trigger对应的Job
	 * 
	 * @param tigger
	 * @throws SchedulerException
	 */
	public static void removeJob(Trigger tigger) throws SchedulerException {
		Scheduler s = QuartzManager.getScheduler();
		try {
			//qrtz_job_details
			log.info("删除JOB JobName:" + tigger.getJobName() + ",jobGroup:" + tigger.getJobGroup());
			s.deleteJob(tigger.getJobName(), tigger.getJobGroup());
		} catch (SchedulerException e) {
			log.error("删除JOB异常@QuartzManager.removeJob(Trigger tigger) JobName:" + tigger.getJobName() + ",jobGroup:" + tigger.getJobGroup(), e);
			throw e;
		}
	}

	/**
	 * 根据任务编号，查找Job
	 * 
	 * @param autoTaskId
	 * @param jobType
	 * <br>
	 *    从类型JobConstants.TASK_TYPE*中获取，如果传入JobConstants.TASK_TYPE_ALL，
	 *    则返回所有autoTaskId任务对应的JOB，否则返回该类型对应的JOB
	 * @return
	 */
	public static List<Trigger> findTaskTrigger(String autoTaskId, char jobType) {
		Scheduler s = QuartzManager.getScheduler();
		List<Trigger> triggers = new ArrayList<Trigger>();
		try {
			String[] groups = s.getTriggerGroupNames();
			for (String _g : groups) {
				String[] triggerNames = s.getTriggerNames(_g);
				for (String _t : triggerNames) {
					Trigger t = s.getTrigger(_t, _g);
					if (autoTaskId.equals(t.getJobDataMap().get(JobConstants.QUARTZ_TASK_KEY))) {
						// 获取JOB类型
						String jobTypeTemp = (String) t.getJobDataMap().get(JobConstants.QUARTZ_TASK_KEY_TYPE);
						char _jobType = JobConstants.TASK_TYPE_ALL;
						if (jobTypeTemp != null && jobTypeTemp.length() > 0)
							_jobType = jobTypeTemp.charAt(0);
						// 如果JOB类型不为全部，并且job类型与当前的类型不同，则不返回
						if (!(JobConstants.TASK_TYPE_ALL == jobType) && !(jobType == _jobType)) {
							continue;
						} else {
							if(_t.contains(autoTaskId) && !_g.equals(JobConstants.TASK_DETECTION_GROUP)){
								triggers.add(t);
							}
						}
					}
				}
			}
		} catch (SchedulerException e) {
			log.error("根据任务编号，查找trigger时异常@QuartzManager.findTaskTrigger，任务编号：" + autoTaskId, e);
			return triggers;
		}
		return triggers;

	}

	/**
	 * 根据任务编号，查找Job
	 * 
	 * @param autoTaskId
	 * @param triggerGroupName
	 *            trigger所在分组
	 * @return
	 */
	public static List<Trigger> findTaskTrigger(String autoTaskId, String triggerGroupName) {
		Scheduler s = QuartzManager.getScheduler();
		List<Trigger> triggers = new ArrayList<Trigger>();
		try {
			String[] groups = s.getTriggerGroupNames();
			for (String _g : groups) {
				String[] triggerNames = s.getTriggerNames(_g);
				for (String _t : triggerNames) {
					if (!_t.equals(triggerGroupName))
						continue;
					Trigger t = s.getTrigger(_t, _g);
					if (autoTaskId.equals(t.getJobDataMap().get(JobConstants.QUARTZ_TASK_KEY))) {
						triggers.add(t);
					}
				}
			}
		} catch (SchedulerException e) {
			log.error("根据任务编号，查找trigger时异常@QuartzManager.findTaskTrigger，任务编号：" + autoTaskId, e);
			return triggers;
		}
		return triggers;
	}

	/**
	 * 根据任务编号，查找所有分组下的Job
	 * 
	 * @param autoTaskId
	 * @return
	 */
	public static List<Trigger> findTaskTrigger(String autoTaskId) {
		Scheduler s = QuartzManager.getScheduler();
		List<Trigger> triggers = new ArrayList<Trigger>();
		try {
			String[] groups = s.getTriggerGroupNames();
			for (String _g : groups) {
				String[] triggerNames = s.getTriggerNames(_g);
				for (String _t : triggerNames) {
					Trigger t = s.getTrigger(_t, _g);
					if (autoTaskId.equals(t.getJobDataMap().get(JobConstants.QUARTZ_TASK_KEY))) {
						triggers.add(t);
					}
				}
			}
		} catch (SchedulerException e) {
			log.error("根据任务编号，查找trigger时异常@QuartzManager.findTaskTrigger，任务编号：" + autoTaskId, e);
			return triggers;
		}
		return triggers;
	}

	/**
	 * 根据任务编号，查找指定分组下的Job
	 * 
	 * @param groupName
	 * @return
	 * @throws SchedulerException
	 */
	public static List<Trigger> findTaskTriggerByGroupName(String groupName) throws SchedulerException {
		Scheduler s = QuartzManager.getScheduler();
		List<Trigger> triggers = new ArrayList<Trigger>();
		try {
			String[] triggerNames = s.getTriggerNames(groupName);
			for (String _t : triggerNames) {
				Trigger t = s.getTrigger(_t, groupName);
				triggers.add(t);
			}
		} catch (SchedulerException e) {
			log.error("根据组名称，查找trigger时异常@QuartzManager.findTaskTriggerByGroupName，组名称：" + groupName, e);
			throw e;
		}
		return triggers;
	}

	public static void removeTriggerByGroupName(String groupName) throws SchedulerException {
		List<Trigger> triggers = findTaskTriggerByGroupName(groupName);
		try {
			for (Trigger t : triggers) {
				removeJob(t);
			}
		} catch (SchedulerException e) {
			log.error("根据组名称，删除trigger时异常@QuartzManager.removeTriggerByGroupName，组名称：" + groupName, e);
			throw e;
		}
	}

	/**
	 * 查找JOB
	 * 
	 * @param jobName
	 * @param jobGroup
	 * @return
	 * @throws SchedulerException
	 */
	public static JobDetail findJobDetail(String jobName, String jobGroup) throws SchedulerException {
		JobDetail job = QuartzManager.getScheduler().getJobDetail(jobName, jobGroup);
		return job;
	}
}
