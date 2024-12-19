package com.ls.athena.framew.terminal.archivesmanager;

import java.io.Serializable;

/**
 * 终端档案
 * @author Administrator
 *
 */
public class TerminalArchivesObject implements Serializable{

	private static final long serialVersionUID = -201403062006L;
	
	private String powerUnitNumber; //供电单位编号
	private String terminalId;//终端标识
	private String ID;//测量点标识
	private String meterId;//电能表id
	
	public TerminalArchivesObject(){}
	public TerminalArchivesObject(String powerUnitNumber,String terminalId ){//供电单位编号ORGNO,终端标识
		this.powerUnitNumber=powerUnitNumber;
		this.terminalId=terminalId;
	}
	public TerminalArchivesObject(String terminalId ){//测量点标识
		this.terminalId=terminalId;
	}
	
	public String getPowerUnitNumber() {
		return powerUnitNumber;
	}
	public void setPowerUnitNumber(String powerUnitNumber) {
		this.powerUnitNumber = powerUnitNumber;
	}
	public String getTerminalId() {
		return terminalId;
	}
	public void setTerminalId(String terminalId) {
		this.terminalId = terminalId;
	}
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getMeterId() {
		return meterId;
	}
	public void setMeterId(String meterId) {
		this.meterId = meterId;
	}
}
