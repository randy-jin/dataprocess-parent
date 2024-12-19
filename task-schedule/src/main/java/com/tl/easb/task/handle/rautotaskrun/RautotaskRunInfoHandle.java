package com.tl.easb.task.handle.rautotaskrun;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.ls.pf.base.utils.task.DateUtils;
import com.tl.easb.utils.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：工具类，用于对表【R_AUTOTASK_RUN_INFO】的操作
 * @author JinZhiQiang
 * @date 2014年3月10日
 */
public class RautotaskRunInfoHandle {
	private static Logger log = LoggerFactory.getLogger(RautotaskRunInfoHandle.class);
	
	private static final String oncSucessRatioSql = getOncSucessRatioSql();
	private static final String successCountSql = getSuccessCountSql();
	private static final String updSql = getUpdSql();
	private static final String initSql = getInitSql();
	
	private RautotaskRunInfoHandle(){

	}

	/**
	 * 表【R_AUTOTASK_RUN_INFO】的初始化
	 * @param autotaskId
	 * @param startExecTime
	 */
	public static void init(String autotaskId, String startExecTime){
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String endExecTime = null;
		String endFlag = "0";//0:正在执行,1:已结束
		int runCount = 0;
		int itemTotal = 0;
		if(null != taskStatus){
			runCount = Integer.parseInt(taskStatus[3]);
			itemTotal = Integer.parseInt(taskStatus[6]);
		}
		DBUtils.executeUpdate(initSql, new Object[]{autotaskId,startExecTime,endExecTime,endFlag,runCount,itemTotal,0,0,0,0});
	}

	/**
	 * 更新表【R_AUTOTASK_RUN_INFO】
	 * @param autotaskId
	 * @return
	 */
	public static BigDecimal update(String autotaskId, BigDecimal leftMpCounter){
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String endExecTime = DateUtils.getCurrTime();
		String endFlag = "1";
		BigDecimal itemTotal = new BigDecimal(taskStatus[6]);
		BigDecimal itemFinished = CacheUtil.getTaskCounter(autotaskId);
		// 如果完成量大于应采总数
		if(itemFinished.compareTo(itemTotal) > 0){
			itemFinished = itemTotal;
		}
		BigDecimal finishRatio = itemFinished.divide(itemTotal,4,BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100));
		BigDecimal itemSuccess = itemFinished.subtract(leftMpCounter);
		BigDecimal successRatio = itemSuccess.divide(itemTotal, 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100));
		String startExecTime = taskStatus[0];
		String runCount = taskStatus[3];
		log.info("任务【"+autotaskId+"】执行表【R_AUTOTASK_RUN_INFO】的更新，SQL为："+updSql+"参数为："+endExecTime+"_"+endFlag+"_"+itemFinished+"_"+finishRatio+"_"+itemSuccess+"_"+successRatio+"_"+autotaskId+"_"+startExecTime+"_"+runCount);
		int ret = DBUtils.executeUpdate(updSql,new Object[]{endExecTime,endFlag,itemFinished,finishRatio,itemSuccess,successRatio,autotaskId,startExecTime,runCount});
		log.info("任务【"+autotaskId+"】更新表【R_AUTOTASK_RUN_INFO】成功数：" + ret);
		return successRatio;
	}

	/**
	 * 从采集任务运行情况明细信息中统计采集成功数
	 * @param autotaskId
	 * @return
	 */
	public static BigDecimal getSuccessCount(String autotaskId){
		BigDecimal successCount = new BigDecimal(0); 
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String startExecTime = taskStatus[0];
		ResultSet rs = DBUtils.executeQuery(successCountSql, new Object[]{autotaskId, startExecTime});
		try {
			if(rs.next()){
				successCount = rs.getBigDecimal("ITEM_SUCCESS");
			}
		} catch (SQLException e) {
			log.error("从采集任务运行情况明细表中获取采集总数失败："+successCountSql, e);
		}
		return successCount;
	}

	/**
	 * 从采集任务运行情况明细信息中获取一次采集成功率
	 * @param autotaskId
	 * @return
	 */
	public static BigDecimal getOncSuccessRatio(String autotaskId){
		BigDecimal oncSuccessRatio = new BigDecimal(0); 
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String startExecTime = taskStatus[0];
		ResultSet rs = DBUtils.executeQuery(oncSucessRatioSql, new Object[]{autotaskId, startExecTime});
		try {
			if(rs.next()){
				oncSuccessRatio = rs.getBigDecimal("SUCCESS_RATIO");
			}
		} catch (SQLException e) {
			log.error("从采集任务运行情况明细表中获取一次采集成功率失败："+oncSucessRatioSql, e);
		}
		return oncSuccessRatio;
	}
	
	/**
	 * 方法说明：获取应采总数与完成数
	 * @param autoTaskId
	 * @param startExecTime
	 * @param endExecTime
	 * @return
	 */
	public static ResultSet getTotalNum(String autoTaskId, Date startExecTime, Date endExecTime) {
		String sql = " SELECT IFNULL(SUM(T.ITEM_TOTAL),0) SUMTOTAL, " 
				+" IFNULL(SUM(T.ITEM_FINISHED),0) SUMFINISHED "
				+" FROM R_AUTOTASK_RUN_INFO T "
				+" WHERE T.AUTOTASK_ID = ? "
				+" AND T.START_EXEC_TIME >= ?"
				+" AND T.END_EXEC_TIME <= ?";
		ResultSet rs =  DBUtils.executeQuery(sql,  new Object[] { autoTaskId,startExecTime,endExecTime});
		return rs;
	}

	private static String getOncSucessRatioSql(){
		String sql = "select SUCCESS_RATIO"
				+"  from R_AUTOTASK_RUN_INFO"
				+" where "
				+"   AUTOTASK_ID = ?"
				+"   and START_EXEC_TIME >= STR_TO_DATE(?, '%Y%m%d%H%i%s')"
				+"   and END_EXEC_TIME is not null "
				+" order by START_EXEC_TIME"
				+ "  LIMIT 1 ";
		return sql;
	}

	private static String getSuccessCountSql(){
		String sql = "select sum(ITEM_SUCCESS) ITEM_SUCCESS"
				+"  from R_AUTOTASK_RUN_INFO"
				+" WHERE AUTOTASK_ID = ?"
				+"   and START_EXEC_TIME >= STR_TO_DATE(?, '%Y%m%d%H%i%s')"
				+"   and END_EXEC_TIME is not null";
		return sql;
	}

	private static String getUpdSql(){
		String sql = "update R_AUTOTASK_RUN_INFO"
				+"   set END_EXEC_TIME = STR_TO_DATE(?, '%Y-%m-%d %H:%i:%s'),"
				+"       END_FLAG      = ?,"
				+"       ITEM_FINISHED = ?,"
				+"       FINISH_RATIO  = ?,"
				+"       ITEM_SUCCESS  = ?,"
				+"       SUCCESS_RATIO = ?"
				+" where AUTOTASK_ID = ?"
				+"   and START_EXEC_TIME >= STR_TO_DATE(?, '%Y%m%d%H%i%s')"
				+"   and RUNCOUNT = ?";
		return sql;
	}

	private static String getInitSql(){
		String sql = "insert into R_AUTOTASK_RUN_INFO"
				+"  (AUTOTASK_ID,"
				+"   START_EXEC_TIME,"
				+"   END_EXEC_TIME,"
				+"   END_FLAG,"
				+"   RUNCOUNT,"
				+"   ITEM_TOTAL,"
				+"   ITEM_FINISHED,"
				+"   FINISH_RATIO,"
				+"   ITEM_SUCCESS,"
				+"   SUCCESS_RATIO)"
				+"values"
				+"  (?, STR_TO_DATE(?, '%Y/%m/%d %H:%i:%s'), ?, ?, ?, ?, ?, ?, ?, ?)";
		return sql;
	}

}
