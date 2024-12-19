package com.tl.easb.task.job.common.procedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.task.job.common.CommonJobDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储过程对象管理
 * @author JinZhiQiang
 * @date 2014年4月9日
 */
public class ProcedureHandle {
	private static Logger log = LoggerFactory.getLogger(ProcedureHandle.class);
	
	private static final String qryByPdIdSql = "select * from R_STORED_PROCEDURES s,R_PROCEDURES_DEPLOY p where s.sp_id = p.sp_id and p.state = '01' and p.pd_id= ?";
	private static final String qrySql = "select * from R_STORED_PROCEDURES s,R_PROCEDURES_DEPLOY p where s.sp_id = p.sp_id and p.state = '01' and p.run_mode= ?";
	private static final String qryByReplyIdSql = "select * from R_STORED_PROCEDURES s,R_PROCEDURES_DEPLOY p where s.sp_id = p.sp_id and p.state = '01' and p.rely_id = ? and p.run_mode = ?";
	
	private ProcedureHandle(){
		
	}
	
	/**
	 * 根据pd_id查询存储过程信息
	 * @param pdId
	 * @return
	 */
	public static Procedure getProcedureByPdId(String pdId){
		ResultSet rs = DBUtils.executeQuery(qryByPdIdSql, new Object[]{pdId});
		Procedure procedure = null;
		try {
			if(rs.next()){
				procedure = translateResultToProcedure(rs);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return procedure;
	}
	
	/**
	 * 根据rely_id查询存储过程信息
	 * @param id
	 * @return
	 */
	public static List<Procedure> getProcedureByReplyId(String id,int runMode){
		ResultSet rs = DBUtils.executeQuery(qryByReplyIdSql, new Object[]{id, runMode});
		List<Procedure> procedures = new ArrayList<Procedure>();
		try {
			while(rs.next()){
				Procedure procedure = translateResultToProcedure(rs);
				procedures.add(procedure);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return procedures;
	}
	
	/**
	 * 查询所有存储过程
	 * @return
	 */
	public static List<Procedure> getProcedures(){
		ResultSet rs = DBUtils.executeQuery(qrySql, new Object[]{CommonJobDefine.RUN_MODE_TIMER});
		List<Procedure> procedures = new ArrayList<Procedure>();
		try {
			while(rs.next()){
				Procedure procedure = translateResultToProcedure(rs);
				procedures.add(procedure);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return procedures;
	}
	
	/**
	 * 对象转换
	 * @param rs
	 * @return
	 */
	private static Procedure translateResultToProcedure(ResultSet rs){
		Procedure procedure = new Procedure();
		try {
			procedure.setSpId(rs.getBigDecimal("SP_ID"));
			procedure.setPackName(rs.getString("PACK_NAME"));
			procedure.setProName(rs.getString("PRO_NAME"));
			procedure.setProCode(rs.getString("PRO_CODE"));
			procedure.setPackCode(rs.getString("PACK_CODE"));
			procedure.setPdId(rs.getBigDecimal("PD_ID"));
			procedure.setRunMode(rs.getString("RUN_MODE"));
			procedure.setRelyId(rs.getInt("RELY_ID"));
			procedure.setRunCycle(rs.getInt("RUN_CYCLE"));
			procedure.setRunTimeMonth(rs.getInt("RUN_TIME_MONTH"));
			procedure.setRunTimeWeek(rs.getInt("RUN_TIME_WEEK"));
			procedure.setRunTimeDay(rs.getInt("RUN_TIME_DAY"));
			procedure.setWeekRun(rs.getString("WEEK_RUN"));
			procedure.setByHours(rs.getString("DAY_HOURS"));
			procedure.setState(rs.getString("STATE"));
			procedure.setOptCode(rs.getString("OPT_CODE"));
			procedure.setOptTime(rs.getDate("OPT_TIME"));
			procedure.setRunTimeHour(rs.getInt("RUN_TIME_HOUR"));
			procedure.setRunTimeMinute(rs.getInt("RUN_TIME_MINUTE"));
			procedure.setIsOrg(rs.getString("IS_ORG"));
			procedure.setIsSyn(rs.getString("IS_SYN"));
			procedure.setRunCycleLimitStart(rs.getInt("RUN_CYCLE_LIMIT_START"));
			procedure.setRunCycleLimitEnd(rs.getInt("RUN_CYCLE_LIMIT_END"));
		} catch (SQLException e) {
			log.error("",e);
		}
		return procedure;
	}
}
