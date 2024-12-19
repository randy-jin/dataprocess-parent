package com.tl.easb.task.handle.mainschedule.view;


public class TaskInfoView {
	/**
	 * 附加标识
	 */
	private String autotaskId;
	private String startExecTime;
	private String endExecTime;
	private String ifBroadcast;
	private int itemTotal;
	private int itemSuccess;
	private int itemFail;
	private double onceSuccessRatio;
	private double cycleSuccessRatio;
	private int itemFinished;
	private String dataDate;
	public String getAutotaskId() {
		return autotaskId;
	}
	public void setAutotaskId(String autotaskId) {
		this.autotaskId = autotaskId;
	}
	public String getStartExecTime() {
		return startExecTime;
	}
	public void setStartExecTime(String startExecTime) {
		this.startExecTime = startExecTime;
	}
	public String getEndExecTime() {
		return endExecTime;
	}
	public void setEndExecTime(String endExecTime) {
		this.endExecTime = endExecTime;
	}
	public String getIfBroadcast() {
		return ifBroadcast;
	}
	public void setIfBroadcast(String ifBroadcast) {
		this.ifBroadcast = ifBroadcast;
	}
	public int getItemTotal() {
		return itemTotal;
	}
	public void setItemTotal(int itemTotal) {
		this.itemTotal = itemTotal;
	}
	public int getItemSuccess() {
		return itemSuccess;
	}
	public void setItemSuccess(int itemSuccess) {
		this.itemSuccess = itemSuccess;
	}
	public int getItemFail() {
		return itemFail;
	}
	public void setItemFail(int itemFail) {
		this.itemFail = itemFail;
	}
	public double getOnceSuccessRatio() {
		return onceSuccessRatio;
	}
	public void setOnceSuccessRatio(double onceSuccessRatio) {
		this.onceSuccessRatio = onceSuccessRatio;
	}
	public double getCycleSuccessRatio() {
		return cycleSuccessRatio;
	}
	public void setCycleSuccessRatio(double cycleSuccessRatio) {
		this.cycleSuccessRatio = cycleSuccessRatio;
	}
	public int getItemFinished() {
		return itemFinished;
	}
	public void setItemFinished(int itemFinished) {
		this.itemFinished = itemFinished;
	}
	public String getDataDate() {
		return dataDate;
	}
	public void setDataDate(String dataDate) {
		this.dataDate = dataDate;
	}
}
