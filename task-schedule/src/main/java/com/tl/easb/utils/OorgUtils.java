package com.tl.easb.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OorgUtils {
	private static Logger log = LoggerFactory.getLogger(OorgUtils.class);

	private static String qryOrgSql = "select ORG_NO from o_org oo where oo.org_type = ?";
	
	/**
	 * 供电单位类别：01 国网公司、02 省公司、03 地市公司 、04 区县公司、05 分公司、06 供电所。
	 * @param orgType
	 * @return
	 */
	public static List<String> getOrgNo(String orgType){
		List<String> list = new ArrayList<String>();
		try {
			ResultSet rs = DBUtils.executeQuery(qryOrgSql, new Object[]{orgType});
			while(rs.next()){
				list.add(rs.getString("ORG_NO"));
			}
		} catch (SQLException e) {
			log.error("查询机构信息异常：", e);
		}
		return list;
	}
}
