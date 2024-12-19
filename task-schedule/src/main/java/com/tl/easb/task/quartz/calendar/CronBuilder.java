package com.tl.easb.task.quartz.calendar;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.quartz.CronExpression;

import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cron表达式解析创建类
 * @author Administrator
 *
 */
public class CronBuilder {
	static Logger log = LoggerFactory.getLogger(CronBuilder.class) ;
	private AutoTaskConfig cron = null;
	private CronBuilder(AutoTaskConfig taskConfig){
		this.cron = taskConfig ;
	}
	/**
	 * 获取初始化实例
	 * @param taskConfig
	 * @return
	 */
	public static CronBuilder getInstance(AutoTaskConfig taskConfig){
		return new CronBuilder(taskConfig);
	}
	/**
	 * 获取表达式
	 * @param taskConfig
	 * @return
	 */
	public static List<CronExpression> getCronExpresion(AutoTaskConfig taskConfig){
		return getInstance(taskConfig).getCronExpresion();
	}
	/**
	 * 获取日期表达式
	 * @return
	 */
	public List<CronExpression> getCronExpresion(){
		int runCycle = this.cron.getRunCycle();
		int delay = this.cron.getDelay();//延迟时间
		List<CronExpression> expresion = new ArrayList<CronExpression>();
		switch (runCycle) {
		case 1:			// 年
			expresion.add(buildYearExpresion());
			break;
		case 2:			// 月
			expresion.add(buildMonthExpresion());
			break;
		case 3:			// 周
			expresion.add(buildWeekExpresion());
			break;
		case 4:			// 日
			expresion = buildDayExpresion();
			break;
		case 5:			// 时
			expresion.add(buildMinExpresion(cron.getRunTimeMinute() + ""));
			break;
		case 6:			// 30分
			expresion.add(buildMinExpresion(delay+"/30"));
			break;
		case 7:			// 15分
			expresion.add(buildMinExpresion(delay+"/15"));
			break;
		case 8:			// 5分
			expresion.add(buildMinExpresion(delay+"/5"));
			break;
		case 9:			// 1分
			expresion.add(buildMinExpresion("*"));
			break;
		}
		return expresion ;
	}
	/**
	 * 创建年CRON表达式
	 * @return
	 */
	private CronExpression buildYearExpresion(){
		//		 0/10 * * * * ?
		return this.buildExpresion(
				"0", 
				"0", 
				String.valueOf(cron.getRunTimeHour()), 
				String.valueOf(cron.getRunTimeDay()), 
				String.valueOf(cron.getRunTimeMonth()), 
				"?", 
				"*") ;
	}
	/**
	 * 创建月CRON表达式
	 * @return
	 */
	private CronExpression buildMonthExpresion(){
		return this.buildExpresion(
				"0",
				String.valueOf(cron.getRunTimeMinute()) , 
				String.valueOf(cron.getRunTimeHour()), 
				String.valueOf(cron.getRunTimeDay()),
				"*", 
				"?",
				"*") ;
	}
	/**
	 * 创建周CRON表达式（供按周任务使用）
	 * @return
	 */
	private CronExpression buildWeekExpresion(){
		String weekStr = (cron.getRunTimeWeek()+1 )+ "";
		return this.buildExpresion(
				"0",
				String.valueOf(cron.getRunTimeMinute()) , 
				String.valueOf(cron.getRunTimeHour()), 
				"?",
				"*", 
				weekStr,
				"*") ;
	}

	/**
	 * 获取周表达式(供按日、按小时、按分钟使用)；注：
	 * @return
	 */
	private String getWeekExpresion(){
		String weekStr = null;
		String weekRun = cron.getWeekRun() ;
		StringBuffer weekBuffer = new StringBuffer();
		if(StringUtil.isNotEmpty(weekRun)){
			// 如果全选，返回*
			if("1111111".equals(weekRun)){
				return "*";
			}
			// 表达式小于7位，异常
			if(weekRun.length() != 7){
				//				return weekStr;
				throw new CronParseException("任务编号为[" + this.cron.getAutoTaskId() + "] 任务为按周，但是week_run[" + weekRun + "]字段长度不为7！");
			}
			for(int i = 0 ; i < weekRun.length() ; i ++){
				if('1' == weekRun.charAt(i)){
					weekBuffer.append(i + 1).append(",") ;
				}
			}
			// 去掉最后一个,
			if(weekBuffer.length() > 0){
				weekStr = weekBuffer.substring(0, weekBuffer.length() - 1) ;
			}else{
				throw new CronParseException("任务编号为[" + this.cron.getAutoTaskId() + "] 任务为按周，但是week_run[" + weekRun + "]没有一天是可执行的时间（全为0）！");
			}
		}else{
			;
		}
		if(weekStr == null){
			return "*";
			//			throw new CronParseException("任务编号为[" + this.cron.getAutoTaskId() + "]任务为按周， 解析week_run[" + weekRun + "]返回结果为空，！");
		}
		return weekStr ;
	}


	//	private String getTimeByHours(){
	//		String byHours = taskConfig.getByHours();
	//		byHours.indexOf("1");
	//	}

	public static void main(String[] args) {
		//		String str = "000000000000000000000000000000000000000000000000";
		//		int idx = 0;
		//		int i = 0;
		//		StringBuffer hour = new StringBuffer();
		//		StringBuffer minutes = new StringBuffer();
		//		while(idx >= 0){
		//			idx = str.indexOf("1",idx);
		//			if(idx >= 0){
		//				i ++;
		//				if(i == 1){
		//					hour.append(idx/2);
		//					minutes.append(idx%2==1?30:0);
		//				} else {
		//					hour.append(",");
		//					hour.append(idx/2);
		//					minutes.append(",");
		//					minutes.append(idx%2==1?30:0);
		//				}
		//				idx ++;
		//			}
		//		}
		//		List<String> list = new ArrayList<String>(48);
		//		list.add("aa");
		//		list.add("bb");
	}

	private List<String> buildTime(String byHour){
		List<String> times = new ArrayList<String>(48);
		int idx = 0;
		while(idx >= 0){
			idx = byHour.indexOf("1",idx);
			if(idx >= 0){
				StringBuffer time = new StringBuffer();
				time.append(idx/2).append(",").append(idx%2==1?30:0);
				times.add(time.toString());
				idx ++;
			}
		}
		return times;
	}

	/**
	 * 创建日CRON表达式
	 * @return
	 */
	private List<CronExpression> buildDayExpresion(){
		int delay = this.cron.getDelay();//延迟时间
		List<CronExpression> expressions = null;
		String byHours = cron.getByHours();
		List<String> times = null;
		if(null != byHours){
			times = buildTime(cron.getByHours());
		}
		if(null != byHours && times.size() > 0){
			expressions = new ArrayList<CronExpression>(times.size());
			for(String time:times){
				String[] tm = time.split(",");
				String minutes = String.valueOf(Integer.parseInt(tm[1])+delay);
				expressions.add(this.buildExpresion("0",minutes,tm[0],"?","*",getWeekExpresion(),"*"));
			}
		} else {
			expressions = new ArrayList<CronExpression>(1);
			String weekDayExp = getWeekExpresion();
			String monthDayExp = weekDayExp.equals("*") ? "?":"*";
			expressions.add(this.buildExpresion("0",cron.getRunTimeMinute()+"",cron.getRunTimeHour()+"",monthDayExp,"*",weekDayExp,"*"));
		}
		return expressions;
	}
	/**
	 * 创建小时CRON表达式
	 * @return
	 */
	private CronExpression buildMinExpresion(String minitExpresion){
		// 处理允许在x时-x时执行
		// 获取开始时
		int start = this.cron.getRunCycleLimitStart() ;
		int end = this.cron.getRunCycleLimitEnd() ;
		String hourExp = "*";
		if(start == 0 && end == 23){
			hourExp = "*" ;
		}else if(start == end){
			hourExp = start + "";
		}else if(start > end){
			throw new CronParseException("任务编号为[" + this.cron.getAutoTaskId() + "]任务为按分钟， 解析时发现允许开始时[" + start + "]小于允许结束时[" + end + "]，本次任务不安排！");
		}else {
			hourExp = start + "-" + end ;
		}
		return this.buildExpresion(
				"0",
				minitExpresion , 
				hourExp, 
				"?",
				"*", 
				getWeekExpresion(),
				"*") ;

	}

	/**
	 * 组装CRON表达式
	 * @return
	 */
	private CronExpression buildExpresion(String s,String min,String h,String d,String mon,String w,String y){
		String expr =  s + " " + min + " " + h + " " + d + " " + mon + " " + w + " " + y  ;
		try { 
			log.info("任务编号为[" + this.cron.getAutoTaskId() + "],解析表达式:[" + expr + "]") ;
			return new CronExpression(expr);
		} catch (ParseException e) {
			log.error("任务编号为[" + this.cron.getAutoTaskId() + "],解析表达式:[" + expr + "]异常" ,e) ;
		} 
		return null ;
	}
	//	public static void main(String[] args) {
	//		String weekRun = "1110111";
	//		StringBuffer weekBuffer = new StringBuffer();
	//		String rlt = null ;
	//		for(int i = 0 ; i < weekRun.length() ; i ++){
	//			if('1' == weekRun.charAt(i)){
	//				weekBuffer.append(i + 1).append(",") ;
	//			}
	//		}
	//		if(weekBuffer.length() > 0){
	//			rlt = weekBuffer.substring(0, weekBuffer.length() - 1) ;
	//		}else{
	//			throw new CronParseException("任务编号为 任务为按周，但是week_run没有一天是可执行的时间（全为0）！");
	//		}
	//	}
}
