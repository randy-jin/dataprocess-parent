package com.tl.easb.system.listener;

import javax.servlet.ServletContextEvent;

import org.apache.commons.lang3.StringUtils;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.job.common.CommonJobAssign;
import com.tl.easb.task.job.scan.ScanJob;
import com.tl.easb.task.manage.QuartzManager;
import com.tl.easb.utils.PropertyUtils;
import com.tl.easb.utils.SpringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @描述 任务监听
 * @author lizhenming
 * @version 1.0
 * @history: 修改时间 修改人 描述 2014-2-21 Administrator
 */
public class JobListener extends AbstractSystemListener {

	private static Logger log = LoggerFactory.getLogger(JobListener.class);

	@Override
	public void onInit(ServletContextEvent event) {
		Scheduler scheduler = QuartzManager.getScheduler();
		try {
			// 安排采集调度任务
			assignCollectJob(scheduler);

			// 从配置文件安排通用任务
			assignFileJob(scheduler);
			
			// 从数据库安排通用任务
			assignDbJob(scheduler);

			// 启动任务
			if (!scheduler.isStarted())
				scheduler.start();

		} catch (SchedulerException e) {
			log.error("",e);
		}

		log.info("QUART调度启动完成****************");
	}

	@Override
	public void onDestory(ServletContextEvent event) {

	}
	
	/**
	 * 自动采集任务
	 * @param scheduler
	 */
	private void assignCollectJob(Scheduler scheduler) {
		if (StringUtils.isNotEmpty(this.getParameters())) {
			if (this.getParameters().length() == 3) {
				if (this.getParameters().charAt(0) == '0') {
					return;
				}
			} else {
				log.info("配置项：【" + this.getConfigString() + "】配置参数" + this.getParameters() + "，但格式不正确，参数不生效，将全部启动！");
			}
		}
		try {
			Trigger trigger = scheduler.getTrigger(JobConstants.TASK_DEFAULT_NAME, JobConstants.TASK_DEFAULT_GROUP);
			if (trigger == null) {
				log.info("QUART调度启动，未加载到监护任务，进行新建。。。。。。。。。。。。。。。。。。。。。。");

				CronTrigger t = new CronTrigger();
				t.setGroup(JobConstants.TASK_DEFAULT_GROUP);
				t.setName(JobConstants.TASK_DEFAULT_NAME);
				t.setCronExpression(PropertyUtils.getProperValue("TASK.SCAN.CRONEXPRESSION", "0/2 * * * * ?"));

				JobDetail job = new JobDetail();
				job.setGroup(JobConstants.TASK_DEFAULT_GROUP);
				job.setName(JobConstants.TASK_DEFAULT_NAME);
				job.setJobClass(ScanJob.class);
				job.setVolatility(false);

				QuartzManager.scheduleJob(job, t);

				log.info("监护任务，新建并启动完成！。。。。。。。。。。。。。。。。。。。。。。");
			} else {
				log.info("监护任务启动完成！。。。。。。。。。。。。。。。。。。。。。。");
			}
		} catch (Exception e) {
			log.error("监护任务启动异常，新安排的任务将无法响应:",e);
		}
	}
	
	/**
	 * 从数据库中安排通用调度任务
	 * @param scheduler
	 */
	private void assignDbJob(Scheduler scheduler) {
		try {
			if (StringUtils.isNotEmpty(this.getParameters())) {
				if (this.getParameters().length() == 3) {
					// 从数据库中安排任务
					if (this.getParameters().charAt(2) == '1') {
						CommonJobAssign jobAssign = (CommonJobAssign) SpringUtils.getBean("commonJobAssign");
						if (jobAssign != null) {
							System.out.println("从数据库安排任务");
							jobAssign.assignJobFromDbAll();
						}
					}
				} else {
					log.info("配置项：【" + this.getConfigString() + "】配置参数" + this.getParameters() + "，但格式不正确，参数不生效，将全部启动！");
				}
			}
		} catch (Exception e) {
			log.error("数据库通用任务启动异常:", e);
		}
	}

	/**
	 * 从配置文件中安排通用调度任务
	 * @param scheduler
	 */
	private void assignFileJob(Scheduler scheduler) {
		try {
			if (StringUtils.isNotEmpty(this.getParameters())) {
				if (this.getParameters().length() == 3) {
					// 从配置文件中安排任务
					if (this.getParameters().charAt(1) == '1') {
						CommonJobAssign jobAssign = (CommonJobAssign) SpringUtils.getBean("commonJobAssign");
						if (jobAssign != null) {
							System.out.println("从配置文件安排任务");
							jobAssign.assignJobFromFile();
						}
					}
				} else {
					log.info("配置项：【" + this.getConfigString() + "】配置参数" + this.getParameters() + "，但格式不正确，参数不生效，将全部启动！");
				}
			}
		} catch (Exception e) {
			log.error("配置文件通用任务启动异常:", e);
		}
	}
}
