package com.tl.easb.task.manage;

import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.cache.dataitem.DataItemCache;
import com.tl.easb.task.job.common.CommonJobAssign;
import com.tl.easb.task.job.common.procedure.Procedure;
import com.tl.easb.task.job.common.procedure.ProcedureHandle;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.SpringUtils;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @描述 自动任务管理
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class AutoTaskManage_bak {
	private static Logger log = LoggerFactory.getLogger(AutoTaskManage_bak.class);

	private static AutoTaskManage_bak autoTaskManager = null;

	private static Map<String, AutoTaskConfig> autoTaskConfigMapCache = new HashMap<String, AutoTaskConfig>();

	private AutoTaskManage_bak() {

	}

	public static synchronized AutoTaskManage_bak getInstance() {
		if (null == autoTaskManager)
			autoTaskManager = new AutoTaskManage_bak();
		return autoTaskManager;
	}

	/**
	 * 根据任务编号获取一条任务信息
	 * @param autotaskId
	 * @return
	 */
	public static AutoTaskConfig findTaskConfig(String autotaskId) {
		AutoTaskConfig autoTaskConfig = autoTaskConfigMapCache.get(autotaskId);
		if(null != autoTaskConfig){
			return autoTaskConfig;
		}
		synchronized (AutoTaskManage_bak.class) {
			autoTaskConfig = autoTaskConfigMapCache.get(autotaskId);
			if(null != autoTaskConfig){
				return autoTaskConfig;
			}
			log.error("Can not find task ["+autotaskId+"] in MapCache,will be initing!");
			initAutoTaskConfig(autotaskId);
		}
		return autoTaskConfigMapCache.get(autotaskId);
	}

	/**
	 * 功能：从内存中清理任务信息
	 * 业务场景：当采集任务执行完成后，调用此方法清理内存中的任务信息
	 * @param autotaskId
	 */
	public static void removeTaskConfig(String autotaskId){
		if(null != autoTaskConfigMapCache && null != autoTaskConfigMapCache.get(autotaskId)){
			log.info("delete["+autotaskId+"] from MapCache !");
			autoTaskConfigMapCache.remove(autotaskId);
		}
		DataItemCache.removeDataitemIdbyAutotaskId(autotaskId);
	}

	/**
	 * 将任务配置信息初始化入本地内存
	 */
	private static void initAutoTaskConfig(String autotaskId){
		try {
			ResultSet rs =  DBUtils.executeQuery(FIND_AUTO_TASK_SQL,new Object[]{autotaskId});
			if(null == rs){
				return;
			}
			// 采集任务采集点范围表达式key
//			List<String> expressionKey = new ArrayList<String>();
			// 采集任务采集点范围表达式VALUE
//			List<String> expressionValue = new ArrayList<String>();
			AutoTaskConfig ac = null;
			while(rs.next()){
				if(rs.isFirst()){
					ac = new AutoTaskConfig();
					ac.setAutoTaskId(rs.getString("AUTOTASK_ID"));
					ac.setName(rs.getString("NAME"));
					ac.setOrgNo(rs.getString("ORG_NO"));
					ac.setIfBroadCast(rs.getString("IF_BROADCAST"));
					ac.setPri(rs.getInt("PRI"));
					ac.setState(rs.getString("STATE"));
					ac.setRetryCount(rs.getInt("RETRY_COUNT"));
					ac.setRetryInterval(rs.getInt("RETRY_INTERVAL"));
					ac.setRunMaxTime(rs.getInt("RUN_MAX_TIME"));
					ac.setRunCycle(rs.getInt("RUN_CYCLE"));
					ac.setRunTimeMonth(rs.getInt("RUN_TIME_MONTH"));
					ac.setRunTimeWeek(rs.getInt("RUN_TIME_WEEK"));
					ac.setRunTimeDay(rs.getInt("RUN_TIME_DAY"));
					ac.setRunTimeHour(rs.getInt("RUN_TIME_HOUR"));
					ac.setRunTimeMinute(rs.getInt("RUN_TIME_MINUTE"));
					ac.setWeekRun(rs.getString("WEEK_RUN"));
					ac.setByHours(rs.getString("DAY_HOURS"));
					ac.setCurveType(rs.getInt("CURVE_TYPE"));
					ac.setRunCycleLimitStart(rs.getInt("RUN_CYCLE_LIMIT_START"));
					ac.setRunCycleLimitEnd(rs.getInt("RUN_CYCLE_LIMIT_END"));
					ac.setCpSql(rs.getString("CP_SQL"));
					ac.setrScope(rs.getInt("R_SCOPE"));
					ac.setItemsScope(rs.getInt("ITEMS_SCOPE"));
					ac.setCurveCondition(rs.getString("CURVE_CONDITION"));
					ac.setDayScope(rs.getString("DAY_SCOPE"));
					ac.setOptCode(rs.getString("OPT_CODE"));
					ac.setOptTime(rs.getDate("OPT_TIME"));
					ac.setDelay(rs.getInt("DELAY"));
					ac.setRetryAll(rs.getString("RETRY_ALL"));
//					ac.setTaskScopeType(rs.getString("TASK_SCOPE_TYPE"));
				}
//				if("AC_SAMPLING_FLAG".equals(rs.getString("EXPRESSION_KEY")) && "1".equals(rs.getString("EXPRESSION_VALUE"))){
//					expressionKey.add(rs.getString("EXPRESSION_KEY"));
//					expressionValue.add(rs.getString("EXPRESSION_VALUE"));
//				}
//				if("204".equals(rs.getString("EXPRESSION_KEY"))){
//					expressionKey.add(rs.getString("EXPRESSION_KEY"));
//					expressionValue.add(rs.getString("EXPRESSION_VALUE"));
//				}
				if(rs.isLast()){
//					ac.setExpressionKey(expressionKey);
//					ac.setExpressionValue(expressionValue);
					autoTaskConfigMapCache.put(rs.getString("AUTOTASK_ID"), ac);
				}
			}
		} catch (SQLException e) {
			log.error("获取自动任务信息异常，执行语句为：" + FIND_AUTO_TASK_SQL, e);
		}
	}


	/**
	 * 查找新产生的任务，并进行任务安排
	 * @throws SQLException
	 */
	public void scanAndAssignTask() {
		Connection con = null;
		boolean isAutoCommit = true;
		try {
			con = DBUtils.getConnection();
			isAutoCommit = con.getAutoCommit();
			con.setAutoCommit(false);
			ResultSet rs =  DBUtils.executeQuery(con, FIND_TASK_TO_ASIGN_SQL, null);
			if(null == rs){
				return;
			}
			if(rs.next()){
				String autoTaskId = rs.getString("AUTOTASK_ID");
				String taskType = rs.getString("TASK_TYPE");
				String id = rs.getString("ID");
				String colDataDate = rs.getString("COL_DATA_DATE");
				String taskSort = rs.getString("TASK_SORT");
				if(null == taskSort || "01".equals(taskSort)){
					DBUtils.executeQuery(con, UPDATE_TASK_TO_ASIGN_SQL, new String[] { id });
					con.commit();
					dealCollectTask(taskType, autoTaskId, colDataDate);
				} else if("02".equals(taskSort)){
					dealDbCommonTask(taskType, autoTaskId, colDataDate);
				}
//				DBUtils.executeQuery(con, UPDATE_TASK_TO_ASIGN_SQL, new String[] { id });
			}

		} catch (Exception e) {
			try {
				con.rollback();
			} catch (SQLException e1) {
				log.error("",e1);
			}
			log.error("扫描并安排任务异常：",e);
		} finally {
			try {
				con.setAutoCommit(isAutoCommit);
			} catch (SQLException e) {
				log.error("",e);
			}
			DBUtils.close(con);
		}
	}

	private void dealDbCommonTask(String taskType, String pdId, String colDataDate) throws SchedulerException {
		Procedure procedure = ProcedureHandle.getProcedureByPdId(pdId);
		if(null == procedure){
			log.error("无法根据pdId="+pdId+"找到对应的存储过程配置信息！");
			return;
		}
		CommonJobAssign jobAssign = (CommonJobAssign) SpringUtils.getBean("commonJobAssign");
		/**
		 * 0：新增 1：修改  2：删除 3：立即执行
		 */
		if ("0".equals(taskType)) {
			log.info("存储过程：[" + procedure.getPackCode() + "." + procedure.getProCode() + "]安排新任务！");
			jobAssign.assignJobFromDb(procedure);
		} else if ("1".equals(taskType)) {
			log.info("存储过程：[" + procedure.getPackCode() + "." + procedure.getProCode() + "]修改任务！");
			procedure.setAutoTaskId(pdId);
			removeTask(procedure);
			jobAssign.assignJobFromDb(procedure);
		} else if ("2".equals(taskType)) {
			log.info("存储过程：[" + procedure.getPackCode() + "." + procedure.getProCode() + "]移除任务！");
			procedure.setAutoTaskId(pdId);
			removeTask(procedure);
		} else if("3".equals(taskType)) {
			procedure.setDataDate(colDataDate);
			log.info("存储过程：[" + procedure.getPackCode() + "." + procedure.getProCode() + "]安排立即执行任务！");
			jobAssign.assignJobFromDb(procedure,"3");
		}
	}

	/**
	 * 根据状态处理任务，
	 *
	 * @param taskType
	 *            0：新增 1：修改 2：删除
	 * @param autoTaskId
	 * @throws SchedulerException
	 */
	public void dealCollectTask(String taskType, String autoTaskId, String colDataDate) throws SchedulerException {
		AutoTaskManage_bak.removeTaskConfig(autoTaskId);
		AutoTaskConfig taskConfig = AutoTaskManage_bak.findTaskConfig(autoTaskId);
		if(null == taskConfig){
			log.error("任务【"+autoTaskId+"】已被置为停用或删除！");
			return;
		}
		taskConfig.setDataDate(colDataDate);

		log.info("开始进行任务处理，任务编号为：[" + autoTaskId + "]，任务状态为：" + taskConfig.getState());
		// 判断任务状态是否为“正在执行”
		if (null != taskConfig.getState() && taskConfig.getState().equals("01")) {
			if (!taskConfig.isToRun()) {
				log.error("任务编号：[" + autoTaskId + "]对应的任务状态不是可运行，该任务不安排！");
				return;
			}
			if (taskConfig.isIfDel()) {
				log.error("任务编号：[" + autoTaskId + "]对应的任务IF_DEL状态为删除，该任务不安排！");
				return;
			}

			/**
			 * 0：新增 1：修改  2：删除 3：立即执行
			 */
			if ("0".equals(taskType)) {
				log.info("任务编号：[" + autoTaskId + "]安排新任务！");
				assignTask(taskConfig);
			} else if ("1".equals(taskType)) {
				log.info("任务编号：[" + autoTaskId + "]修改任务！");
				updateTask(taskConfig);
			} else if ("2".equals(taskType)) {
				log.info("任务编号：[" + autoTaskId + "]移除任务！");
				removeTask(taskConfig);
			} else if ("3".equals(taskType)) {
				log.info("任务编号：[" + autoTaskId + "]安排立即执行任务！");
				assignRunImidiatlyTask(taskConfig);
			}
		} else {
			if ("2".equals(taskType)) {
				log.info("任务编号：[" + autoTaskId + "]移除任务！");
				removeTask(taskConfig);
			}
			log.info("任务编号：[" + autoTaskId + "]的任务已停用！");
		}
	}

	/**
	 * 安排新任务
	 *
	 * @param taskConfig
	 *            任务配置信息
	 * @throws SchedulerException
	 */
	public void assignTask(AutoTaskConfig taskConfig) throws SchedulerException {
		AutoTaskAssign taskAssign = new AutoTaskAssign(taskConfig, taskConfig.getRunImmediately());
		taskAssign.assignTask();
	}

	/**
	 * 更新任务
	 *
	 * @param taskConfig
	 * @throws SchedulerException
	 */
	public void updateTask(AutoTaskConfig taskConfig) throws SchedulerException {
		AutoTaskAssign taskAssign = new AutoTaskAssign(taskConfig, taskConfig.getRunImmediately());
		taskAssign.updateTask();
	}

	/**
	 * 移除任务
	 *
	 * @param taskConfig
	 * @throws SchedulerException
	 */
	public void removeTask(AutoTaskConfig taskConfig) throws SchedulerException {
		AutoTaskAssign taskAssign = new AutoTaskAssign(taskConfig, taskConfig.getRunImmediately());
		taskAssign.removeTask();
	}

	/**
	 * 安排补采任务
	 *
	 * @param taskConfig
	 * @param hasRedoCount
	 * @throws SchedulerException
	 */
	public void assignRedoTask(AutoTaskConfig taskConfig, int hasRedoCount) throws SchedulerException {
		AutoTaskAssign taskAssign = new AutoTaskAssign(taskConfig, taskConfig.getRunImmediately());
		taskAssign.assignRedoTask(hasRedoCount);
	}

	/**
	 * 安排立即执行任务
	 *
	 * @param taskConfig
	 * @throws SchedulerException
	 */
	public void assignRunImidiatlyTask(AutoTaskConfig taskConfig) throws SchedulerException {
		AutoTaskAssign taskAssign = new AutoTaskAssign(taskConfig, taskConfig.getRunImmediately());
		taskAssign.assignRunImidiatlyTask();
	}

	//	private final static String FIND_AUTO_TASK_SQL = "select R_AUTOTASK_CONFIG.AUTOTASK_ID, NAME, ORG_NO, IF_BROADCAST, PRI, STATE, RETRY_FLAG, RETRY_COUNT, RETRY_INTERVAL, RUN_MAX_TIME, RUN_CYCLE, RUN_TIME_MONTH, RUN_TIME_WEEK, RUN_TIME_DAY, RUN_TIME_HOUR, RUN_TIME_MINUTE, WEEK_RUN, RUN_CYCLE_LIMIT_START, RUN_CYCLE_LIMIT_END, CP_SQL, CURVE_TYPE, CURVE_CONDITION, ITEMS_SCOPE, R_SCOPE, DAY_HOURS, DAY_SCOPE, TASK_SCOPE_TYPE, EXPRESSION_KEY, EXPRESSION_VALUE from R_AUTOTASK_CONFIG LEFT JOIN R_AUTOTASK_OBJS ON R_AUTOTASK_CONFIG.AUTOTASK_ID=R_AUTOTASK_OBJS.AUTOTASK_ID where STATE = '01' and R_AUTOTASK_CONFIG.AUTOTASK_ID = ? ";
	private final static String FIND_AUTO_TASK_SQL = "select AUTOTASK_ID, NAME, ORG_NO, IF_BROADCAST, PRI, STATE, RETRY_COUNT, RETRY_INTERVAL, RUN_MAX_TIME, RUN_CYCLE, RUN_TIME_MONTH, RUN_TIME_WEEK, RUN_TIME_DAY, RUN_TIME_HOUR, RUN_TIME_MINUTE, WEEK_RUN, DAY_HOURS, CURVE_TYPE, RUN_CYCLE_LIMIT_START, RUN_CYCLE_LIMIT_END, CP_SQL, R_SCOPE, ITEMS_SCOPE, CURVE_CONDITION, DAY_SCOPE, OPT_CODE, OPT_TIME, DELAY, RETRY_ALL from R_AUTOTASK_CONFIG where AUTOTASK_ID = ? ";
	private final static String FIND_TASK_TO_ASIGN_SQL = "SELECT AUTOTASK_ID,TASK_TYPE,STATUS,ID,date_format(COL_DATA_DATE,'%Y%m%d') COL_DATA_DATE,TASK_SORT FROM R_AUTOTASK_TRIGGER WHERE STATUS = '0' LIMIT 1 FOR UPDATE";
	private final static String UPDATE_TASK_TO_ASIGN_SQL = "UPDATE R_AUTOTASK_TRIGGER SET STATUS = '1',DEAL_DATE=sysdate() WHERE ID = ?  ";
}