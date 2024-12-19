package com.tl.easb.task.handle.rautotaskrun;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.ls.pf.base.utils.task.DateUtils;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：工具类，用于对表【R_AUTOTASK_RUN】的操作
 * @author JinZhiQiang
 * @date 2014年3月10日
 */
public class RautotaskRunHandle {
	private static Logger log = LoggerFactory.getLogger(RautotaskRunHandle.class);

	private static String initSql = getInitSql();
	private static String itemTotalSql = getItemTotalSql();
	private static String updSql = getUpdSql();
	private static String isFirstSql = "select 1 FLAG from R_AUTOTASK_RUN r where r.start_exec_time>=DATE_FORMAT(SYSDATE(),'%Y-%m-%d')";

	private RautotaskRunHandle(){

	}
	
	/**
	 * 方法说明：更新任务运行汇总表应采总数
	 * @param autoTaskId
	 * @param startExecTime
	 * @param itemTotal
	 */
	public static void updateTotalNum(String autoTaskId, String startExecTime,
			String itemTotal) {
		String sql = " UPDATE R_AUTOTASK_RUN "
				+" SET ITEM_TOTAL = ? "
				+" WHERE AUTOTASK_ID = ? "
				+" AND START_EXEC_TIME =" 
				+" STR_TO_DATE(?, '%Y-%m-%d %H:%i:%s') ";
		DBUtils.executeUpdate(sql,  new String[] { itemTotal,autoTaskId,startExecTime});
	}

	/**
	 * 方法说明：判断是否为当日第一次执行
	 * @param autoTaskId
	 * @return
	 */
	public static boolean ifFirstExe(String autoTaskId) {
		String sql = " SELECT T.AUTOTASK_ID "
				+" FROM R_AUTOTASK_RUN T "
				+" WHERE T.AUTOTASK_ID = ? "
				+" AND DATE_FORMAT(T.START_EXEC_TIME, '%Y-%m-%d') = "
				+"  DATE_FORMAT(SYSDATE(), '%Y-%m-%d') ";
		ResultSet rs = DBUtils.executeQuery(sql,  new String[] { autoTaskId });
		try {
			while(rs.next()){
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 判断任务是否当前第一次执行
	 * true:是
	 * false：否
	 * @return
	 */
	public static boolean isFirst(){
		boolean flag = true;
		ResultSet rs = DBUtils.executeQuery(isFirstSql, null);
		try {
			if(rs.next()){
				flag = false;
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return flag;
	}

	/**
	 * 初始化表【R_AUTOTASK_RUN】
	 * @param taskConfig
	 * @param startExecTime
	 * @param dataDate
	 * @param itemTotal
	 */
	public static void init(AutoTaskConfig taskConfig, String startExecTime, String dataDate, int itemTotal){
		String autotaskId = taskConfig.getAutoTaskId();
		String ifBroadcast = taskConfig.getIfBroadCast();
		DBUtils.executeUpdate(initSql, new Object[]{autotaskId,startExecTime,ifBroadcast,itemTotal,0,0,0,0,dataDate});
	}

	/**
	 * 更新表【R_AUTOTASK_RUN】
	 * @param autotaskId
	 */
	public static void update(String autotaskId){
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String endExecTime = DateUtils.getCurrTime();
		BigDecimal itemSuccess = RautotaskRunInfoHandle.getSuccessCount(autotaskId);
		BigDecimal itemTotal = getItemTotal(autotaskId);
		BigDecimal itemFail = itemTotal.subtract(itemSuccess);
		BigDecimal onceSuccessRatio = RautotaskRunInfoHandle.getOncSuccessRatio(autotaskId);
		BigDecimal cycleSuccessRatio = itemTotal.compareTo(new BigDecimal(0))==0?new BigDecimal(0) : itemSuccess.divide(itemTotal, 4, BigDecimal.ROUND_HALF_DOWN).multiply(new BigDecimal(100));
//		BigDecimal itemFinished = new BigDecimal(100);
		String startExecTime = taskStatus[0];
		DBUtils.executeUpdate(updSql, new Object[]{endExecTime,itemSuccess,itemFail,onceSuccessRatio,cycleSuccessRatio,autotaskId,startExecTime});
	}

	/**
	 * 从采集任务运行情况表中获取采集总数
	 * @param autotaskId
	 * @return
	 */
	public static BigDecimal getItemTotal(String autotaskId){
		BigDecimal itemTotal = new BigDecimal(0);
		String[] taskStatus = CacheUtil.getTaskStatus(autotaskId);
		String startExecTime = taskStatus[0];
		ResultSet rs = DBUtils.executeQuery(itemTotalSql, new Object[]{autotaskId, startExecTime});
		try {
			if(rs.next()){
				itemTotal = rs.getBigDecimal("ITEM_TOTAL");
			}
		} catch (SQLException e) {
			log.error("从采集任务运行情况表中获取采集总数失败：", e);
		}
		return itemTotal;
	}
	
	/**
	 * 方法说明：获取开始时间与完成时间
	 * @param autoTaskId
	 * @param collectDate
	 * @return
	 */
	public static ResultSet getStartEndTime(String autoTaskId, String collectDate) {
		String tme[] = collectDate.split(" ");
		String sql = " SELECT T.START_EXEC_TIME, T.END_EXEC_TIME "
				+"  FROM R_AUTOTASK_RUN T "
				+"  WHERE T.AUTOTASK_ID = ? "
				+"   AND DATE_FORMAT(T.DATA_DATE, '%Y/%m/%d') = ? "
				+"   AND T.END_EXEC_TIME IS NOT NULL "
				+"  ORDER BY T.START_EXEC_TIME DESC "
				+ "  LIMIT 1 " ;
		ResultSet rs =  DBUtils.executeQuery(sql,  new String[] { autoTaskId ,tme[0]});
		return rs;
	}

	private static String getInitSql(){
		String sql = "  INSERT INTO R_AUTOTASK_RUN  "
				+"  (AUTOTASK_ID,  "
				+"  START_EXEC_TIME,  "
				+"  IF_BROADCAST,  "
				+"  ITEM_TOTAL,  "
				+"  ITEM_SUCCESS,  "
				+"  ITEM_FAIL,  "
				+"  ONCE_SUCCESS_RATIO,  "
				+"  CYCLE_SUCCESS_RATIO,  "
				+"  DATA_DATE)  "
				+"  VALUES  "
				+"  (?, " 
				+"  STR_TO_DATE(?, '%Y/%m/%d %H:%i:%s')" 
				+  ", ?, ?, ?, ?, ?, ?, " 
				+  "STR_TO_DATE(?, '%Y/%m/%d %H:%i:%s'))";
		return sql;
	}

	private static String getItemTotalSql(){
		String sql = "select ITEM_TOTAL"
				+"  from R_AUTOTASK_RUN"
				+" where AUTOTASK_ID = ?"
				+"   and START_EXEC_TIME >= str_to_date(?, '%Y%m%d%H%i%s')"
				+"   and END_EXEC_TIME is null";
		return sql;
	}

	private static String getUpdSql(){
		String sql = "update R_AUTOTASK_RUN "
				+"   set END_EXEC_TIME       = str_to_date(?, '%Y-%m-%d %H:%i:%s'),"
				+"       ITEM_SUCCESS        = ?,"
				+"       ITEM_FAIL           = ?,"
				+"       ONCE_SUCCESS_RATIO  = ?,"
				+"       CYCLE_SUCCESS_RATIO = ?"
//				+"       ITEM_FINISHED       = ?"
				+" where AUTOTASK_ID = ?"
				+"   and START_EXEC_TIME = str_to_date(?, '%Y%m%d%H%i%s')";
		return sql;
	}
}
