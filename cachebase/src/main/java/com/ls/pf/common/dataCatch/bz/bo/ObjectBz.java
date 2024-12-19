package com.ls.pf.common.dataCatch.bz.bo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据采集业务对象
 * 
 * @author JinZhiQiang
 * 
 */
public class ObjectBz implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 业务数据标识
	 */
	private String dataItemId;
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
	 * 电表地址 COMMADDR
	 */
	private Map commAddr;

	/**
	 * 总加组序号 T
	 */
	private Map totalNum;

	/**
	 *端口号 D
	 */
	private Map portNum;

	/**
	 * 终端标识
	 */
	private String tmnlId;


	/**
	 * 部门编号
	 */
	private String departCode;

	/**
	 * 用户大类号
	 */
	private Map userFlag;
	
	/**
	 * 终端加密
	 */
	private String schemeNo;

	/**
	 * 电表属性
	 */
	private List<Map<String,String>> meterAttribute;

	/**
	 * 多规约电表属性
	 */
	private List<Map<String,String>> meterMP;

	/**
	 * 数据项list
	 */
	private List<Map<String,String>> dataItemList;

	/**
	 * 规约编号
	 * 100开始的表示终端的规约；
	 * 200开始的表示电表的规约；
	 */
	private String protocolId;

	private Map meterIdMap;



	public Map getMeterIdMap() {
		return meterIdMap;
	}

	public void setMeterIdMap(Map meterIdMap) {
		this.meterIdMap = meterIdMap;
	}

	public List<Map<String, String>> getMeterAttribute() {
		return meterAttribute;
	}

	public void setMeterAttribute(List<Map<String, String>> meterAttribute) {
		this.meterAttribute = meterAttribute;
	}

	public List<Map<String, String>> getMeterMP() {
		return meterMP;
	}

	public void setMeterMP(List<Map<String, String>> meterMP) {
		this.meterMP = meterMP;
	}

	public String getProtocolId() {
		return protocolId;
	}

	public void setProtocolId(String protocolId) {
		this.protocolId = protocolId;
	}

	public Map getUserFlag() {
		return userFlag;
	}

	public void setUserFlag(Map userFlag) {
		this.userFlag = userFlag;
	}

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

	public String getDepartCode() {
		return departCode;
	}

	public void setDepartCode(String departCode) {
		this.departCode = departCode;
	}

	public String getTmnlId() {
		return tmnlId;
	}

	public void setTmnlId(String tmnlId) {
		this.tmnlId = tmnlId;
	}

	public Map getPointNum() {
		return pointNum;
	}

	public void setPointNum(Map pointNum) {
		this.pointNum = pointNum;
	}

	public Map getCommAddr() {
		return commAddr;
	}

	public void setCommAddr(Map commAddr) {
		this.commAddr = commAddr;
	}

	public Map getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(Map totalNum) {
		this.totalNum = totalNum;
	}

	public Map getPortNum() {
		return portNum;
	}

	public void setPortNum(Map portNum) {
		this.portNum = portNum;
	}

	public String getDataItemId() {
		return dataItemId;
	}

	public void setDataItemId(String dataItemId) {
		this.dataItemId = dataItemId;
	}

	public List<Map<String, String>> getDataItemList() {
		return dataItemList;
	}

	public void setDataItemList(List<Map<String, String>> dataItemList) {
		this.dataItemList = dataItemList;
	}
	
	
	private String mpedId;
	private String val;


	public String getMpedId() {
		return mpedId;
	}

	public void setMpedId(String mpedId) {
		this.mpedId = mpedId;
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}

	public String getSchemeNo() {
		return schemeNo;
	}

	public void setSchemeNo(String schemeNo) {
		this.schemeNo = schemeNo;
	}

	

}