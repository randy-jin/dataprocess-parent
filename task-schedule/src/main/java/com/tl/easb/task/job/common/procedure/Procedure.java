package com.tl.easb.task.job.common.procedure;

import java.math.BigDecimal;
import java.util.Date;

import com.tl.easb.task.manage.view.AutoTaskConfig;

/**
 * 存储过程信息对象
 * @author JinZhiQiang
 * @date 2014年4月9日
 */
public class Procedure extends AutoTaskConfig{

	private static final long serialVersionUID = 7984241197670797950L;

	/**
	 * 唯一标识
	 */
	private BigDecimal spId;

	/**
	 * 包名称（中文名称）
	 */
	private String packName;

	/**
	 * 存储过程名称（中文名称）
	 */
	private String proName;

	/**
	 * 存储过程名称（中文名称）
	 */
	private String proCode;

	/**
	 * 包编码（英文名称）
	 */
	private String packCode;

	/**
	 * 唯一标识
	 */
	private BigDecimal pdId;

	/**
	 * 执行方式 0：定时执行；1：自动任务完成后执行；2：其他存储过程完成后执行；
	 */
	private String runMode;

	/**
	 * 依靠标识
	 * 如果执行方式为1，则为采集任务标识；
	 * 如果执行方式是2，则为本表唯一标识；
	 */
	private int relyId;

	/**
	 * 执行周期
	 * 1：年；
	 * 2：月；
	 * 3：周；
	 * 4：日； 
	 */
	private int runCycle;

	/**
	 * 执行时间_月
	 */
	private int runTimeMonth;

	/**
	 * 执行时间_周
	 * 0~6分别表示  周日、周一~周六
	 */
	private int runTimeWeek;

	/**
	 * 执行时间_日
	 */
	private int runTimeDay;

	/**
	 * 每周执行日，
	 * 用8位字符串表示，从左到右依次为星期日、星期一...星期六，
	 * 0：不执行  1：执行
	 */
	private String weekRun;

	/**
	 * 每日执行时间
	 *当执行周期为“日”时，保存选择的执行时间：用48位字符串表示48个时间点，从左到右依次表示00:00、00:30......23:30
	 *每一个时间点
	 *0：未选中 1：选中
	 */
	private String dayHours;

	/**
	 * 状态
	 *01：启用
	 *02：停用
	 */
	private String state;

	/**
	 * 操作人
	 */
	private String optCode;
	
	/**
	 * 是否支持机构：0：不支持  1：支持
	 */
	private String isOrg;
	
	/**
	 * 是否并发执行:0:否  1:是
	 */
	private String isSyn;

	public String getIsSyn() {
		return isSyn;
	}

	public void setIsSyn(String isSyn) {
		this.isSyn = isSyn;
	}

	public String getIsOrg() {
		return isOrg;
	}

	public void setIsOrg(String isOrg) {
		this.isOrg = isOrg;
	}

	public BigDecimal getSpId() {
		return spId;
	}

	public void setSpId(BigDecimal spId) {
		this.spId = spId;
	}

	public String getPackName() {
		return packName;
	}

	public void setPackName(String packName) {
		this.packName = packName;
	}

	public String getProName() {
		return proName;
	}

	public void setProName(String proName) {
		this.proName = proName;
	}

	public String getProCode() {
		return proCode;
	}

	public void setProCode(String proCode) {
		this.proCode = proCode;
	}

	public String getPackCode() {
		return packCode;
	}

	public void setPackCode(String packCode) {
		this.packCode = packCode;
	}

	public BigDecimal getPdId() {
		return pdId;
	}

	public void setPdId(BigDecimal pdId) {
		this.pdId = pdId;
	}

	public String getRunMode() {
		return runMode;
	}

	public void setRunMode(String runMode) {
		this.runMode = runMode;
	}

	public int getRelyId() {
		return relyId;
	}

	public void setRelyId(int relyId) {
		this.relyId = relyId;
	}

	public int getRunCycle() {
		return runCycle;
	}

	public void setRunCycle(int runCycle) {
		this.runCycle = runCycle;
	}

	public int getRunTimeMonth() {
		return runTimeMonth;
	}

	public void setRunTimeMonth(int runTimeMonth) {
		this.runTimeMonth = runTimeMonth;
	}

	public int getRunTimeWeek() {
		return runTimeWeek;
	}

	public void setRunTimeWeek(int runTimeWeek) {
		this.runTimeWeek = runTimeWeek;
	}

	public int getRunTimeDay() {
		return runTimeDay;
	}

	public void setRunTimeDay(int runTimeDay) {
		this.runTimeDay = runTimeDay;
	}

	public String getWeekRun() {
		return weekRun;
	}

	public void setWeekRun(String weekRun) {
		this.weekRun = weekRun;
	}

	public String getDayHours() {
		return dayHours;
	}

	public void setDayHours(String dayHours) {
		this.dayHours = dayHours;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getOptCode() {
		return optCode;
	}

	public void setOptCode(String optCode) {
		this.optCode = optCode;
	}

	public Date getOptTime() {
		return optTime;
	}

	public void setOptTime(Date optTime) {
		this.optTime = optTime;
	}

	/**
	 * 操作时间
	 */
	private Date optTime;

}
