package com.ls.pf.common.dataCatch.bz.bo;

import java.io.Serializable;
import java.util.Map;

/**
 * 数据采集业务对象
 * 
 * @author jinzhiqiang
 * 
 */
public class ObjectBzUserType implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 区域码
	 */
	private String areaCode;
	/**
	 * 终端地址
	 */
	private String termAdd;

	/**
	 * 测量点号 P
	 */
	private Map pointNum;

	/**
	 * 终端用户类型
	 */
	private String userType;

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	public String getTermAdd() {
		return termAdd;
	}

	public void setTermAdd(String termAdd) {
		this.termAdd = termAdd;
	}

	public Map getPointNum() {
		return pointNum;
	}

	public void setPointNum(Map pointNum) {
		this.pointNum = pointNum;
	}

	public String getUserType() {
		return userType;
	}

	public void setUserType(String userType) {
		this.userType = userType;
	}

}
