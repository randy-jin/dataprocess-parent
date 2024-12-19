package com.tl.easb.task.handle.rautotaskhistory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RautotaskHistoryHandle {
	private static Logger log = LoggerFactory.getLogger(RautotaskHistoryHandle.class);

	private static final String insertSql = getInsertSql();
	private static final String delSql = "delete from R_AUTOTASK_HISTORY where autotask_id=? and shard_no=? and data_date=STR_TO_DATE(?, '%Y/%m/%d')";

	private RautotaskHistoryHandle(){

	}

	public static void del(String autotaskId, String dataDate){
		if(null == autotaskId || null == dataDate){
			return;
		}
		DBUtils.executeUpdate(delSql, new Object[]{autotaskId,autotaskId,dataDate});
	}

	/**
	 * 方法说明：获取终端信息，包括不重复的行政编码与终端地址
	 * @param autoTaskId
	 * @param collectDate
	 * @return
	 */
	public static ResultSet getAreaAddr(String autoTaskId, String collectDate) {
		String sql = " SELECT  T.TERMINAL_ID "
				+" FROM R_AUTOTASK_HISTORY T "
				+" WHERE T.AUTOTASK_ID = ? "
				+" AND T.SHARD_NO = ? "
				+" AND T.DATA_DATE = "
				+" STR_TO_DATE(?, '%Y/%m/%d') ";
		ResultSet rs =  DBUtils.executeQuery(sql,  new String[] { autoTaskId,autoTaskId,collectDate});
		return rs;
	}
	
	/**
	 * 方法说明：获取终端信息，包括不重复的行政编码与终端地址
	 * @param autoTaskId
	 * @param collectDate
	 * @return
	 */
	public static ResultSet getAreaAddrFromTerminalID(String terminalId) {
		String sql = " SELECT AREA_CODE, TERMINAL_ADDR "
				+" FROM R_TMNL_INFO "
				+" WHERE TERMINAL_ID = ? ";
		ResultSet rs =  DBUtils.executeQuery(sql,  new String[] { terminalId});
		return rs;
	}

	/**
	 * 将测点信息批量插入历史表
	 * @param autotaskId
	 * @param list
	 */
	public static void batchInsert(String autotaskId, List<String> list){
		if(null == list || list.size() == 0){
			return;
		}
		Object[][] params = new Object[list.size()][7];
		int i=0;
		for(String mp : list){
			String[] strs = StringUtil.explodeString(mp, "_");
			for(int j=0;j<6;j++){
				params[i][0] = autotaskId;
				params[i][1] = strs[2];
				params[i][2] = strs[0];
				params[i][3] = strs[1];
				params[i][4] = strs[3];
				params[i][5] = strs[4];
				params[i][6] = autotaskId;
			}
			i ++;
		}
		Connection con = null;
		try{
			con = DBUtils.getConnection();
			DBUtils.execBatchUpdate(con, insertSql, params);
		} finally {
			DBUtils.close(con);
		}
	}

	/**
	 * 方法说明：获取应采总数
	 * @param autoTaskId
	 * @param collectDate
	 * @return
	 */
	public static int getItemTotal(String autoTaskId, String collectDate) {
		int itemTotal = 0;
		String sql = " SELECT COUNT(T.AUTOTASK_ID) TOTAL "
				+" FROM R_AUTOTASK_HISTORY T "
				+" WHERE T.AUTOTASK_ID = ? "
				+" AND T.SHARD_NO = ? "
				+" AND T.DATA_DATE = "
				+" STR_TO_DATE(?, '%Y/%m/%d')" ;
		ResultSet rs =  DBUtils.executeQuery(sql,  new String[] { autoTaskId,autoTaskId,collectDate});
		try {
			while(rs.next()){
				itemTotal = rs.getInt("TOTAL");
			}
		} catch (SQLException e) {
			log.error("任务分发失败！,执行SQL:" + sql, e) ;
		}
		return itemTotal;
	}

	private static String getInsertSql(){
		String sql = "insert into R_AUTOTASK_HISTORY"
				+"  (AUTOTASK_ID,"
				+"   DATA_DATE,"
				+"   TERMINAL_ID,"
				+"   MPED_ID,"
				+"   BUSINESS_DATAITEM_ID,"
				+"   PROTOCOL_ID,"
				+ "  SHARD_NO)"
				+" values "
				+"  (?, STR_TO_DATE(?, '%Y%m%d%H%i%s'), ?, ?, ?, ?, ?)";
		return sql;
	}
}
