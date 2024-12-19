package com.ls.athena.framew.terminal.archivesmanager;

import java.io.Serializable;

/**
 * 终端档案
 * @author Administrator
 *
 */
public class TerminalArchivesObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -201403062006L;
	
	private String powerUnitNumber;
	private String terminalId;
	private String ID;
	
	public TerminalArchivesObject(){}
	public TerminalArchivesObject(String powerUnitNumber,String terminalId ){
		this.powerUnitNumber=powerUnitNumber;
		this.terminalId=terminalId;
	}
	public TerminalArchivesObject(String terminalId ){
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
	
}
