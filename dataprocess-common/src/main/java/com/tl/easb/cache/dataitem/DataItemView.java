package com.tl.easb.cache.dataitem;

import java.io.Serializable;

/**
 * 数据项结构体
 * @author JinZhiQiang
 * @date 2014年3月11日
 */
public class DataItemView implements Serializable{
	private static final long serialVersionUID = 4891434010069046087L;
	private String businessDataitemId;
	private String dataItemId ;
	private String cmd;
	public String getAFN_FN() {
		return AFN_FN;
	}
	public void setAFN_FN(String aFN_FN) {
		AFN_FN = aFN_FN;
	}
	private String AFN_FN;
	public String getCmd() {
		return cmd;
	}
	public void setCmd(String cmd) {
		this.cmd = cmd;
	}
	private String protocolId ;
	private String pid ;
	private String name ;
	private String objFlag ;
	private String dataFlag ;
	private String debugOrder ;
	/**
	 * 终端标识
	 */
	private String terminalId;

	public String getBusinessDataitemId() {
		return businessDataitemId;
	}
	public void setBusinessDataitemId(String businessDataitemId) {
		this.businessDataitemId = businessDataitemId;
	}
	public String getDataItemId() {
		return dataItemId;
	}
	public void setDataItemId(String dataItemId) {
		this.dataItemId = dataItemId;
	}

	public String getProtocolId() {
		return protocolId;
	}
	public void setProtocolId(String protocolId) {
		this.protocolId = protocolId;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getObjFlag() {
		return objFlag;
	}
	public void setObjFlag(String objFlag) {
		this.objFlag = objFlag;
	}
	public String getDataFlag() {
		return dataFlag;
	}
	public void setDataFlag(String dataFlag) {
		this.dataFlag = dataFlag;
	}
	public String getDebugOrder() {
		return debugOrder;
	}
	public void setDebugOrder(String debugOrder) {
		this.debugOrder = debugOrder;
	}
	public String getTerminalId() {
		return terminalId;
	}
	public void setTerminalId(String terminalId) {
		this.terminalId = terminalId;
	}
}
