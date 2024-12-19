package com.tl.easb.task.handle.mainschedule.exec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.task.handle.mainschedule.control.MainTaskDefine;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainTaskDao {
	private static Logger log = LoggerFactory.getLogger(MainTaskDao.class);

	/**
	 * 通过测量点信息表，获取测量点总数
	 * @param taskConfig
	 * @return
	 */
	public static int getMeasurementNum(AutoTaskConfig taskConfig) {
		String innerSql = taskConfig.getCpSql();
		int total = 0;
		String newSql = " SELECT COUNT(1) as TOTAL FROM ("+innerSql+") aa, R_MPED xx  where xx.SHARD_NO = aa.shard_no and xx.TERMINAL_ID = aa.terminal_id ";
		ResultSet rs =  DBUtils.executeQuery(newSql,  new String[] {});
		try {
			while(rs.next()){
				total = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return total;
	}

	/**
	 * 通过总加组信息表，获取测量点总数
	 * @param taskConfig
	 * @return
	 */
	public static int getTotalGroupNum(AutoTaskConfig taskConfig) {
		List<String> expressionKeyList = taskConfig.getExpressionKey();
		List<String> expressionValueList = taskConfig.getExpressionValue();
		String innerSql = taskConfig.getCpSql();
		int total = 0;
		String newSql = "SELECT COUNT(1) as TOTAL FROM P_TOTAL WHERE CP_NO IN (SELECT CP_NO FROM ("+innerSql+") aa)";
		if(null != expressionKeyList && expressionKeyList.contains("204")){//包含用户大类号
			StringBuilder _temp = new StringBuilder();
			if(null != expressionValueList && expressionValueList.size() > 0){
				int size = expressionValueList.size();
				_temp.append(" AND USERFLAG IN (");
				for(int i=0;i<expressionValueList.size();i++){
					String expressValue = expressionValueList.get(i);
					if(i<size-1){
						_temp.append("'"+expressValue+"',");
					} else {
						_temp.append("'"+expressValue+"'");
					}
				}
				_temp.append(")");
			}
			newSql += _temp;
		}
		ResultSet rs =  DBUtils.executeQuery(newSql,  new String[] {});
		try {
			while(rs.next()){
				total = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return total;
	}

	/**
	 * 通过配置sql，获取测量点总数
	 * @param taskConfig
	 * @return
	 */
	public static int getTotalMeasureNum(AutoTaskConfig taskConfig) {
		String innerSql = taskConfig.getCpSql();
		int total = 0;
		String newSql = "SELECT COUNT(1) as TOTAL FROM  ( "
				+ innerSql + ") a ";
		ResultSet rs = DBUtils.executeQuery(newSql, new String[] {});
		try {
			while (rs.next()) {
				total = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return total;
	}

	/**
	 * 获取任务包含的终端总数
	 * @param sql
	 * @return
	 */
	public static int getTmnlNum(String sql){
		int tmnlNum = 0;
		String newSql = "SELECT COUNT(1) as NUM FROM  ( "+sql+") a ";
		try {
			ResultSet rs = DBUtils.executeQuery(newSql, null);
			if(rs.next()){
				tmnlNum = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return tmnlNum;
	}

	/**
	 * 组织子任务列表
	 * @param sql
	 * @param autoTaskId
	 * @param subtaskList
	 * @param collectDateTmp
	 * @throws SQLException
	 */
	public static void getSubTaskList(String sql, String autoTaskId,
			Set<String> subtaskList, String collectDateTmp)
					throws SQLException {
		ResultSet rs2 = DBUtils.executeQuery(sql,  new String[] {});
		String areaCode= "";
		String terminalAddr = "";
		String terminalId = "";
		while (rs2.next()){
			areaCode = rs2.getString("area_code");
			terminalAddr = rs2.getString("terminal_addr");
			terminalId = rs2.getString("terminal_id");
			subtaskList.add(StringUtil.arrToStr(MainTaskDefine.SEPARATOR, autoTaskId,areaCode,terminalAddr,collectDateTmp,terminalId));
		}
	}

	/**
	 * 功能：
	 * 1、组织子任务列表
	 * 2、初始化任务执行测量点缓存
	 * @param sql
	 * @param autoTaskId
	 * @param subtaskList
	 * @param collectDateTmp
	 * @return
	 * @throws SQLException
	 */
	public static List<String[]> getSubTaskListForPenetrate(String sql, String autoTaskId,
			Set<String> subtaskList, String collectDateTmp) throws SQLException{
		ResultSet rs2 = DBUtils.executeQuery(sql,  new String[] {});
		List<String[]> list = new ArrayList<String[]>(); 
		String areaCode= "";
		String terminalAddr = "";
		String mpedIndex = "";
		String mpedId = "";
		while (rs2.next()){
			areaCode = rs2.getString("AREA_CODE");
			terminalAddr = rs2.getString("TERMINAL_ADDR");
			mpedIndex = rs2.getString("MPED_INDEX");
			mpedId = rs2.getString("MPED_ID");
			String[] arr = new String[4];
			arr[0] = areaCode;
			arr[1] = terminalAddr;
			arr[2] = mpedIndex;
			arr[3] = mpedId;
			list.add(arr);
			if(subtaskList!=null){
				subtaskList.add(StringUtil.arrToStr(MainTaskDefine.SEPARATOR, autoTaskId,areaCode,terminalAddr,collectDateTmp));
			}
		}
		return list;
	}
	/**
	 * 功能：
	 * 1、组织子任务列表
	 * 2、初始化任务执行子任务列表
	 * @param sql
	 * @param autoTaskId
	 * @param subtaskList
	 * @param collectDateTmp
	 * @return
	 * @throws SQLException
	 */
	public static Set<String> getSubTaskSetForPenetrate(String sql, String autoTaskId,
			Set<String> subtaskList, String collectDateTmp) throws SQLException{
		ResultSet rs2 = DBUtils.executeQuery(sql,  new String[] {});
		String areaCode= "";
		String terminalAddr = "";
		String terminalId = "";
		if(subtaskList==null){
			subtaskList = new HashSet<String>();
		}
		while (rs2.next()){
			areaCode = rs2.getString("AREA_CODE");
			terminalAddr = rs2.getString("TERMINAL_ADDR");
			terminalId = rs2.getString("TERMINAL_ID");
			subtaskList.add(StringUtil.arrToStr(MainTaskDefine.SEPARATOR, autoTaskId,areaCode,terminalAddr,collectDateTmp,terminalId));
		}
		return subtaskList;
	}
}
