package com.tl.easb.task.manage.view;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class AutoTaskConfig implements Serializable {

	private static final long serialVersionUID = 7567416606598279545L;

	private String autoTaskId ;
	// 采集任务名称
	private String name ;
	// 供电单位编号
	private String orgNo ;
	// 任务类型
	private String ifBroadCast ;

	// 优先级
	private int pri ;

	// 任务状态
	private String state ;

	// 补测条件
//	private String retryFlag ;

	// 补采次数
	private int retryCount ;

	// 补采间隔(MIN)
	private int retryInterval ;

	// 最大执行时长(MIN)
	private int runMaxTime ;

	// 执行周期
	private int runCycle ;

	// 执行时间_月
	private int runTimeMonth ;

	// 执行时间_周
	private int runTimeWeek ;

	// 执行时间_日
	private int runTimeDay ;
	
	// 日冻结时间范围
	private String dayScope;
	
	//采集范围SQL配置类型
	private String taskScopeType;

	public String getTaskScopeType() {
		if ("".equals(taskScopeType)||taskScopeType==null) {
			taskScopeType="01";
		}
		return taskScopeType;
	}
	public void setTaskScopeType(String taskScopeType) {
		this.taskScopeType = taskScopeType;
	}

	// 执行时间_时
	private int runTimeHour ;

	private String byHours;

	// 执行时间_分
	private int runTimeMinite ;

	// 每周执行日
	private String weekRun ;

	// 执行周期限制_起点
	private int runCycleLimitStart ;

	// 执行周期限制_止点
	private int runCycleLimitEnd ;

	// 采集点选取SQL
	private String cpSql ;

	// 采集任务立即执行标志
	private boolean runImmediately ;

	// 采集数据项目的方式
	private int dataItemStyle ;

	// 数据项范围
	private int itemsScope ;

	// 采集范围
	private int rScope;

	// 采集任务数据模板标识
	private String itemFlag ;

	// 数据日期（冻结的日期）
	private String dataDate;
	
	private Date dataDateByDate;

	// 日冻结数据时间范围类型 DAY_STYLE NUMBER(3) 3 FALSE FALSE FALSE
	private int dayStyle;
	// 日冻结数据时间范围条件 DAY_CONDITION VARCHAR2(5) 5 FALSE FALSE FALSE
	private String dayCondition;
	// 曲线数据时间范围类型 CURVE_STYLE NUMBER(3) 3 FALSE FALSE FALSE
	private int curveType;
	// 曲线数据时间范围条件 CURVE_CONDITION VARCHAR2(5) 5 FALSE FALSE FALSE
	private String curveCondition;
	// 月冻结数据时间范围类型 MONTH_STYLE NUMBER(3) 3 FALSE FALSE FALSE
	private int monthStyle;
	// 月冻结数据时间范围条件 MONTH_CONDITION VARCHAR2(5) 5 FALSE FALSE FALSE
	private String monthCondition;
	// 删除标志 IF_DEL NUMBER(1) 1 FALSE FALSE FALSE
	private boolean ifDel;
	//延迟时间
	private int delay;
	//是否全量补采，0：否，1：是
	private String retryAll;
	
	private String optCode;
	private Date optTime;

	//	private DataItems dataItems ;
	// 采集任务采集点范围表达式key
	List<String> expressionKey;

	// 采集任务采集点范围表达式VALUE
	List<String> expressionValue;

	public List<String> getExpressionKey() {
		return expressionKey;
	}
	public void setExpressionKey(List<String> expressionKey) {
		this.expressionKey = expressionKey;
	}
	public List<String> getExpressionValue() {
		return expressionValue;
	}
	public void setExpressionValue(List<String> expressionValue) {
		this.expressionValue = expressionValue;
	}
	
	public String getRetryAll() {
		return retryAll;
	}
	public void setRetryAll(String retryAll) {
		this.retryAll = retryAll;
	}
	public String getName() {
		return name;
	}
	public int getrScope() {
		return rScope;
	}
	public void setrScope(int rScope) {
		this.rScope = rScope;
	}
	public int getItemsScope() {
		return itemsScope;
	}
	public void setItemsScope(int itemsScope) {
		this.itemsScope = itemsScope;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOrgNo() {
		return orgNo;
	}
	public void setOrgNo(String orgNo) {
		this.orgNo = orgNo;
	}
	public String getIfBroadCast() {
		return ifBroadCast;
	}
	public void setIfBroadCast(String ifBroadCast) {
		this.ifBroadCast = ifBroadCast;
	}
	public int getPri() {
		return pri;
	}
	public void setPri(int pri) {
		this.pri = pri;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
//	public String getRetryFlag() {
//		return retryFlag;
//	}
//	public void setRetryFlag(String retryFlag) {
//		this.retryFlag = retryFlag;
//	}
	public int getRetryCount() {
		return retryCount;
	}
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
	public int getRetryInterval() {
		return retryInterval;
	}
	public void setRetryInterval(int retryInterval) {
		this.retryInterval = retryInterval;
	}
	public int getRunMaxTime() {
		return runMaxTime;
	}
	public void setRunMaxTime(int runMaxTime) {
		this.runMaxTime = runMaxTime;
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
	public int getRunTimeHour() {
		return runTimeHour;
	}
	public void setRunTimeHour(int runTimeHour) {
		this.runTimeHour = runTimeHour;
	}
	public int getRunTimeMinute() {
		return runTimeMinite;
	}
	public void setRunTimeMinute(int runTimeMinite) {
		this.runTimeMinite = runTimeMinite;
	}
	public String getWeekRun() {
		return weekRun;
	}
	public void setWeekRun(String weekRun) {
		this.weekRun = weekRun;
	}
	public int getRunCycleLimitStart() {
		return runCycleLimitStart;
	}
	public void setRunCycleLimitStart(int runCycleLimitStart) {
		this.runCycleLimitStart = runCycleLimitStart;
	}
	public int getRunCycleLimitEnd() {
		return runCycleLimitEnd;
	}
	public void setRunCycleLimitEnd(int runCycleLimitEnd) {
		this.runCycleLimitEnd = runCycleLimitEnd;
	}
	public String getCpSql() {
		return cpSql;
	}
	public void setCpSql(String cpSql) {
		this.cpSql = cpSql;
	}
	public boolean getRunImmediately() {
		return runImmediately;
	}
	public void setRunImmediately(boolean runImmediately) {
		this.runImmediately = runImmediately;
	}
	public int getDataItemStyle() {
		return dataItemStyle;
	}
	public void setDataItemStyle(int dataItemStyle) {
		this.dataItemStyle = dataItemStyle;
	}
	public String getItemsFlag() {
		return itemFlag;
	}
	public void setItemsFlag(String itemFlag) {
		this.itemFlag = itemFlag;
	}
	public int getDayStyle() {
		return dayStyle;
	}
	public void setDayStyle(int dayStyle) {
		this.dayStyle = dayStyle;
	}
	public String getDayCondition() {
		return dayCondition;
	}
	public void setDayCondition(String dayCondition) {
		this.dayCondition = dayCondition;
	}
	public int getCurveType() {
		return curveType;
	}
	public void setCurveType(int curveType) {
		this.curveType = curveType;
	}
	public String getCurveCondition() {
		return curveCondition;
	}
	public void setCurveCondition(String curveCondition) {
		this.curveCondition = curveCondition;
	}
	public int getMonthStyle() {
		return monthStyle;
	}
	public void setMonthStyle(int monthStyle) {
		this.monthStyle = monthStyle;
	}
	public String getMonthCondition() {
		return monthCondition;
	}
	public void setMonthCondition(String monthCondition) {
		this.monthCondition = monthCondition;
	}
	public boolean isIfDel() {
		return ifDel;
	}
	public void setIfDel(boolean ifDel) {
		this.ifDel = ifDel;
	}
	public int getDelay() {
		return delay;
	}
	public void setDelay(int delay) {
		this.delay = delay;
	}
	public String getAutoTaskId() {
		return autoTaskId;
	}
	public void setAutoTaskId(String autoTaskId) {
		this.autoTaskId = autoTaskId;
	}

	public boolean isToRun(){
		if( "0".equals(this.state )){
			return false ;
		}else{
			return true ;
		}
	}
	
	public Date getDataDateByDate() throws ParseException {
		DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String dataDateStr = getDataDate();
		if(dataDateStr.length() <= 8){
			dataDateStr = dataDateStr + "000000";
		}
		return format.parse(dataDateStr);
	}
	public void setDataDateByDate(Date dataDateByDate) {
		this.dataDateByDate = dataDateByDate;
	}

	public String getDataDate() {
		if(null == dataDate){
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			cal.add(Calendar.DAY_OF_MONTH, -1);
			dataDate = df.format(cal.getTime());
		}
		return dataDate;
	}

	public void setDataDate(String dataDate) {
		this.dataDate = dataDate;
	}
	
	public String getDayScope() {
		return dayScope;
	}
	public void setDayScope(String dayScope) {
		this.dayScope = dayScope;
	}
	public String getByHours() {
		return byHours;
	}
	public void setByHours(String byHours) {
		this.byHours = byHours;
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
	
}