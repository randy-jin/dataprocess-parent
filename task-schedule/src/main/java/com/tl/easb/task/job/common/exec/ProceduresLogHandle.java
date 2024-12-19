package com.tl.easb.task.job.common.exec;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProceduresLogHandle {
	private static Logger log = LoggerFactory.getLogger(ProceduresLogHandle.class);
	private static String initSql = "insert into R_PROCEDURES_LOG(PD_ID,BE_TIME,EN_TIME,SUCC_FLAG,ERR_MSG,ORG_NO) values(?,sysdate(),?,?,?,?)";
	private static String updSql = "update R_PROCEDURES_LOG set EN_TIME=sysdate(),SUCC_FLAG=?,ERR_MSG=? where PL_ID=?";

	public static BigDecimal init(String pdId,String orgNo){
		Connection conn = DBUtils.getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal id = new BigDecimal(0);
		try {
			pstmt = conn.prepareStatement(initSql,new String[]{"PL_ID"});
			pstmt.setString(1, pdId);
			pstmt.setString(2, null);
			pstmt.setString(3, null);
			pstmt.setString(4, null);
			pstmt.setString(5, orgNo);
			pstmt.executeUpdate();
			rs = pstmt.getGeneratedKeys();
			rs.next();  
			id=rs.getBigDecimal(1);  
		} catch (SQLException e) {
			log.error("初始化表R_PROCEDURES_LOG异常：", e);
		} finally {
			try {
				if(rs != null)
					rs.close();
				if(pstmt != null)
					pstmt.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("",e);
			}
		} 
		return id;  
	}

	public static void update(BigDecimal plId,int succFlag,String errMsg){
		DBUtils.executeUpdate(updSql, new Object[]{succFlag,errMsg,plId});
	}
}
