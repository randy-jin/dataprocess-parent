package com.tl.eas.archives;

import java.io.Serializable;

public class ArchivesObject implements Serializable {
//	private static final long serialVersionUID = -201403062006L;
	private String TERMINAL_ID;
	private String SHARD_NO;
	private String APP_NO;
	public String getTERMINAL_ID() {
		return TERMINAL_ID;
	}
	public void setTERMINAL_ID(String tERMINAL_ID) {
		TERMINAL_ID = tERMINAL_ID;
	}
	public String getSHARD_NO() {
		return SHARD_NO;
	}
	public void setSHARD_NO(String sHARD_NO) {
		SHARD_NO = sHARD_NO;
	}
	public String getAPP_NO() {
		return APP_NO;
	}
	public void setAPP_NO(String aPP_NO) {
		APP_NO = aPP_NO;
	}
}