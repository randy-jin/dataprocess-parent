package com.tl.easb.task.handle.mainschedule.control;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import com.tl.easb.utils.SpringUtils;

import com.tl.easb.cache.dataitem.DataItemCache;
import com.tl.easb.coll.api.ZcDataManager;
import com.tl.easb.coll.api.ZcTaskManager;
import com.tl.easb.task.handle.mainschedule.exec.MainTaskDao;
import com.tl.easb.task.handle.rautotaskhistory.RautotaskHistoryHandle;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunHandle;
import com.tl.easb.task.handle.rautotaskrun.RautotaskRunInfoHandle;
import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.task.job.detection.JobDetectionStart;
import com.tl.easb.task.manage.AutoTaskManage;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.utils.CacheUtil;
import com.tl.easb.utils.DateUtil;
import com.tl.easb.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainTaskManage {
	private static Logger log = LoggerFactory.getLogger(MainTaskManage.class);
	private static int itemTotal = 0;  //应采总数
	private static String startExecTime = null;  //开始时间
	private static String taskStatus = null;  //任务状态
	private static IOperateBzData operateBzData;

	static {
		operateBzData = (IOperateBzData) SpringUtils.getBean("operateBzData");
	}
	public static int getItemTotal(){
		return itemTotal;
	}
	/**
	 * 根据任务信息，拆分并组织一个子任务列表发送到管道的右侧
	 * @param autoTaskId
	 * @param collectDate
	 * @param startExecTime2
	 * @param taskConfig
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public synchronized static boolean collectInfo(String autoTaskId, String collectDate, String startExecTime2,AutoTaskConfig taskConfig) {
		/*****qin 2018年10月22日 解决任务修改后内存中不是最新状态问题start*****/
//		if(RautotaskRunHandle.ifFirstExe(autoTaskId)){
//			log.info("任务【"+autoTaskId+"】当天第一次执行");
//			// 任务第一次执行将任务信息从内存中清除。
//			AutoTaskManage.removeTaskConfig(autoTaskId);
//			// 获取任务对应的配置信息
//			taskConfig = AutoTaskManage.findTaskConfig(autoTaskId);
//		}
		/*****                         end                           *****/
		int itemTotal = 0;  //应采总数
		startExecTime = startExecTime2;
		String areaCode = "";
		String terminalAddr = "";
		String terminalId = "";
		int pri = taskConfig.getPri();
		String redoNum = "0";
		// 子任务执行标识
		String exeFlag = SubTaskDefine.STATUS_EXEC_FLAG_STOP;
		int generateWay = 0;
		Set<String> subtasks = new HashSet<String>();
//		Set<String> subtaskMeasures = new HashSet<String>();

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		Date tmpDate = new Date(startExecTime);
		if(null == collectDate || "".equals(collectDate)){
			log.error("任务【"+autoTaskId+"】采集数据日期为空");
			return false;
		}
		Date tmpDate2 = new Date(collectDate);
		String startExecTimeTmp= df.format(tmpDate.getTime());
		String collectDateTmp= df.format(tmpDate2.getTime());

		if(null != CacheUtil.getTaskStatus(autoTaskId)){
			log.info("前一轮采集任务【"+autoTaskId+"】尚未执行完成，将被当轮任务强制结束！！！！！");
// 			未完成，强制结束
			JobDetectionStart.taskFinishDeal(autoTaskId, collectDate, taskConfig);
		}

		if(taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)&&taskConfig.getrScope()==MainTaskDefine.R_SCOPE_CP){//杭州：普通任务采集范围为测量点
			if((taskConfig.getCpSql().toLowerCase().indexOf("mped_id")<=-1)){//如果结果集中没MPED_ID,就是终端下所有测量点
				if(taskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ){// 抄当日曲线
					log.info("任务【"+autoTaskId+"】数据范围为当日曲线");
					CacheUtil.clearTaskCpData(autoTaskId,collectDateTmp);
					log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
					subtasks = generateFromConfig(collectDateTmp,taskConfig);
				} else if(DateUtil.beforeYesterday(collectDate)){// 抄历史数据
					log.info("任务【"+autoTaskId+"】要采集的数据日期为："+collectDate);
					if(checkHisFull(autoTaskId, collectDate)){//检查历史执行初始化子任务缓存是否完整
						log.info("任务【"+autoTaskId+"】历史执行初始化子任务缓存完整");
						log.info("任务【"+autoTaskId+"】从历史任务明细表中获取应采总数");
						itemTotal = RautotaskHistoryHandle.getItemTotal(autoTaskId,collectDate);//获取应采总数
						log.info("任务【"+autoTaskId+"】应采总数为：" + itemTotal);
						if(itemTotal == 0){
							return false;
						}
						ResultSet rs = RautotaskHistoryHandle.getAreaAddr(autoTaskId,collectDate);//获取终端信息
						log.info("任务【"+autoTaskId+"】从历史采集任务运行情况明细表获取终端信息");
						try {
							while(rs.next()){
								terminalId = rs.getString("TERMINAL_ID");
								ResultSet rs2 = RautotaskHistoryHandle.getAreaAddrFromTerminalID(terminalId);//获取终端area_code addr
								if (rs2.next()) {
									areaCode = rs2.getString("AREA_CODE");
									terminalAddr = rs2.getString("TERMINAL_ADDR");
								}
								subtasks.add(StringUtil.arrToStr(MainTaskDefine.SEPARATOR, autoTaskId,areaCode,terminalAddr,collectDateTmp,terminalId));
							}
						} catch (SQLException e) {
							log.error("",e);
						}
						generateWay = SubTaskDefine.STATUS_COLLINFO_FROM_HISTORY;
						//开始初始化任务状态
						taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDateTmp,pri,redoNum,generateWay,exeFlag,itemTotal);
					}else{//历史执行初始化子任务缓存不完整
						log.info("任务【"+autoTaskId+"】历史执行初始化子任务缓存不完整");
						log.info("任务【"+autoTaskId+"】删除采集任务运行情况明细表历史信息");
						RautotaskHistoryHandle.del(autoTaskId,collectDate);
						subtasks = generateFromConfig(collectDateTmp,taskConfig);
					}
				}else{// 抄当日数据
					if(RautotaskRunHandle.ifFirstExe(autoTaskId)){
						log.info("任务【"+autoTaskId+"】当天第一次执行");
						//根据配置生成应采总数与终端信息
						subtasks = generateFromConfig(collectDateTmp,taskConfig);
						CacheUtil.clearTaskAllCpData(autoTaskId);
					}else{
						log.info("任务【"+autoTaskId+"】并非当天第一次执行");
						/****2019-06-17 15:10 qinsong 添加判断是否全量补采，0：否，1：是****/
						if("1".equals(taskConfig.getRetryAll())){//需要全量补采
							log.info("任务【"+autoTaskId+"】需要每次全量补采");
							CacheUtil.clearTaskCpData(autoTaskId, collectDateTmp);
							log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
							subtasks = generateFromConfig(collectDateTmp,taskConfig);

							/****2019-06-17 15:10 qinsong 添加判断是否全量补采，0：否，1：是 end****/
						}else if(checkHisFull(autoTaskId,collectDate)){
							log.info("任务【"+autoTaskId+"】初始化子任务缓存执行完整");
							String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId,collectDateTmp);
							List<String> list = ZcTaskManager.drawSubtasks(wrapAutoTaskId,ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
							int itemTotaltmp = 0;
							while(list.size()>0){
								itemTotaltmp += ZcTaskManager.getCps(list).size();
								list = ZcTaskManager.drawSubtasks(wrapAutoTaskId,ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
							}
							if(itemTotaltmp == 0){
								log.info("任务【"+autoTaskId+"】从内存中无法获取子任务信息（可能是当日数据已100%采集成功），本次采集略过！");
								// 删除子任务偏移量
								ZcDataManager.removeTaskOffset(wrapAutoTaskId);
								log.info("clear TaskOffset ok!");
								return false;
							}
							itemTotal = itemTotaltmp;
							log.info("任务【"+autoTaskId+"】从缓存中获取的应采总数为："+itemTotal);
							generateWay = SubTaskDefine.STATUS_COLLINFO_FROM_CACHE;
							taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDateTmp,pri,0,generateWay,exeFlag,itemTotal);
							log.info("任务【"+autoTaskId+"】状态为："+taskStatus);
							CacheUtil.initTaskAbout(autoTaskId, taskStatus, collectDateTmp);
							// 主任务--子任务缓存初始化完毕后,启动子任务
							CacheUtil.setTaskExec(autoTaskId);
							log.info("任务【"+autoTaskId+"】启动子任务");
							return true;
						}else{
							log.info("任务【"+autoTaskId+"】初始化子任务缓存执行不完整");
							CacheUtil.clearTaskCpData(autoTaskId, collectDateTmp);
							log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
							subtasks = generateFromConfig(collectDateTmp,taskConfig);
						}
					}
				}
			}else {//采集部分测点，需要初始化临时缓存
				if (taskConfig.getItemsScope() == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ) {// 抄当日曲线
					log.info("任务【"+autoTaskId+"】数据范围为当日曲线");
					CacheUtil.clearTaskCpData(autoTaskId,collectDateTmp);
					log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
					List<String[]> list = new ArrayList<String[]>();
					list = customListFromConfig(collectDateTmp, taskConfig,list);
					subtasks = customSetFromConfig(collectDateTmp, taskConfig);
					CacheUtil.clearTaskAllCpData(autoTaskId);
					log.info("任务【"+autoTaskId+"】初始化当日曲线任务测量点临时缓存"+list.size());
					CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化
				} else if (DateUtil.beforeYesterday(collectDate)) {// 抄历史数据
					log.info("任务【"+autoTaskId+"】要采集的数据日期为："+collectDate);
					if(checkHisFull(autoTaskId, collectDate)){//检查历史执行初始化子任务缓存是否完整
						log.info("任务【"+autoTaskId+"】历史执行初始化子任务缓存完整");
						log.info("任务【"+autoTaskId+"】从历史任务明细表中获取应采总数");
						itemTotal = RautotaskHistoryHandle.getItemTotal(autoTaskId,collectDate);//获取应采总数
						log.info("任务【"+autoTaskId+"】应采总数为：" + itemTotal);
						if(itemTotal == 0){
							return false;
						}
						ResultSet rs = RautotaskHistoryHandle.getAreaAddr(autoTaskId,collectDate);//获取终端信息
						log.info("任务【"+autoTaskId+"】从历史采集任务运行情况明细表获取终端信息");
						try {
							while(rs.next()){
								terminalId = rs.getString("TERMINAL_ID");
								//todo hch add
//								String areaTerminalAdd = TMCache.getInstance().getADDR(terminalId);

								String areaTerminalAdd = operateBzData.getTmnl(terminalId,"ADDR");
								String[] areaTerminal = areaTerminalAdd.split("\\|");
								areaCode = areaTerminal[0];
								terminalAddr = areaTerminal[1];
//								ResultSet rs2 = RautotaskHistoryHandle.getAreaAddrFromTerminalID(terminalId);//获取终端area_code addr
//								if (rs2.next()) {
//									areaCode = rs2.getString("AREA_CODE");
//									terminalAddr = rs2.getString("TERMINAL_ADDR");
//								}
								subtasks.add(StringUtil.arrToStr(MainTaskDefine.SEPARATOR, autoTaskId,areaCode,terminalAddr,collectDateTmp,terminalId));
							}
						} catch (SQLException e) {
							log.error("",e);
						}
						generateWay = SubTaskDefine.STATUS_COLLINFO_FROM_HISTORY;
						//开始初始化任务状态
						taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDateTmp,pri,redoNum,generateWay,exeFlag,itemTotal);
					}else{//历史执行初始化子任务缓存不完整
						log.info("任务【"+autoTaskId+"】历史执行初始化子任务缓存不完整");
						log.info("任务【"+autoTaskId+"】删除采集任务运行情况明细表历史信息");
						RautotaskHistoryHandle.del(autoTaskId,collectDate);
						//根据配置生成应采总数与终端信息
						List<String[]> list = new ArrayList<String[]>();
						list = customListFromConfig(collectDateTmp, taskConfig,list);
						subtasks = customSetFromConfig(collectDateTmp, taskConfig);
						CacheUtil.clearTaskAllCpData(autoTaskId);
						log.info("任务【"+autoTaskId+"】初始化采集历史任务测量点临时缓存"+list.size());
						CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化
					}
				} else {// 抄当日数据
					if(RautotaskRunHandle.ifFirstExe(autoTaskId)){
						log.info("任务【"+autoTaskId+"】当天第一次执行");
						//根据配置生成应采总数与终端信息
						List<String[]> list = new ArrayList<String[]>();
						list = customListFromConfig(collectDateTmp, taskConfig,list);
						subtasks = customSetFromConfig(collectDateTmp, taskConfig);
						CacheUtil.clearTaskAllCpData(autoTaskId);
						log.info("任务【"+autoTaskId+"】初始化自定义SQL任务执行测量点临时缓存"+list.size());
						CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化
					}else{
						log.info("任务【"+autoTaskId+"】并非当天第一次执行");
						/****2019-06-17 15:10 qinsong 添加判断是否全量补采，0：否，1：是****/
						if("1".equals(taskConfig.getRetryAll())){//需要全量补采
							log.info("任务【"+autoTaskId+"】需要每次全量补采");
							CacheUtil.clearTaskCpData(autoTaskId, collectDateTmp);
							log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
							//根据配置生成应采总数与终端信息
							List<String[]> list = new ArrayList<String[]>();
							list = customListFromConfig(collectDateTmp, taskConfig,list);
							subtasks = customSetFromConfig(collectDateTmp, taskConfig);
							log.info("任务【"+autoTaskId+"】初始化自定义SQL任务执行测量点临时缓存"+list.size());
							CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化

							/****2019-06-17 15:10 qinsong 添加判断是否全量补采，0：否，1：是 end****/
						}else if(checkHisFull(autoTaskId,collectDate)){
							log.info("任务【"+autoTaskId+"】初始化子任务缓存执行完整");
							String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId,collectDateTmp);
							List<String> list = ZcTaskManager.drawSubtasks(wrapAutoTaskId,ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
							int itemTotaltmp = 0;
							while(list.size()>0){
								itemTotaltmp += ZcTaskManager.getCps(list).size();
								list = ZcTaskManager.drawSubtasks(wrapAutoTaskId, ParamConstants.REDIS_GAIN_SUBTASK_STEP_LENGTH);
							}
							if(itemTotaltmp == 0){
								log.info("任务【"+autoTaskId+"】从内存中无法获取子任务信息（可能是当日数据已100%采集成功），本次采集略过！");
								// 删除子任务偏移量
								ZcDataManager.removeTaskOffset(wrapAutoTaskId);
								log.info("clear TaskOffset ok!");
								return false;
							}
							itemTotal = itemTotaltmp;
							log.info("任务【"+autoTaskId+"】从缓存中获取的应采总数为："+itemTotal);
							generateWay = SubTaskDefine.STATUS_COLLINFO_FROM_CACHE;
							taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDateTmp,pri,0,generateWay,exeFlag,itemTotal);
							log.info("任务【"+autoTaskId+"】状态为："+taskStatus);
							CacheUtil.initTaskAbout(autoTaskId, taskStatus, collectDateTmp);
							// 主任务--子任务缓存初始化完毕后,启动子任务
							CacheUtil.setTaskExec(autoTaskId);
							log.info("任务【"+autoTaskId+"】启动子任务");
							return true;
						}else{
							log.info("任务【"+autoTaskId+"】初始化子任务缓存执行不完整");
							CacheUtil.clearTaskCpData(autoTaskId, collectDateTmp);
							log.info("任务【"+autoTaskId+"】根据配置生成应采总数与终端信息");
							//根据配置生成应采总数与终端信息
							List<String[]> list = new ArrayList<String[]>();
							list = customListFromConfig(collectDateTmp, taskConfig,list);
							subtasks = customSetFromConfig(collectDateTmp, taskConfig);
							log.info("任务【"+autoTaskId+"】初始化自定义SQL任务执行测量点临时缓存"+list.size());
							CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化
						}
					}
				}
			}
		}else if(taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)&&taskConfig.getrScope()==MainTaskDefine.R_SCOPE_TMNL){//杭州：普通任务采集范围为终端
			collectDateTmp = "00000000000000";
			subtasks = generateFromConfig(collectDateTmp, taskConfig);
		}else if(taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)&&taskConfig.getrScope()==MainTaskDefine.R_SCOPE_TOTAL){//杭州：普通任务采集范围为总加组
			//普通任务采集范围为总加组

		}else if(taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)){//杭州：透传任务
			//TODO huangchunhuai 因透传曲线修改
			if(taskConfig.getItemsScope() != SubTaskDefine.ITEMS_SCOPE_TC_CURVE_DATA ) {
				collectDateTmp = "00000000000000";
			}
			//TODO huangchunhuai 直抄月冻结
			if(taskConfig.getItemsScope() == MainTaskDefine.DATA_FLAG_DIRECT_MON_FREEZE_DATA ) {
				collectDateTmp = "00000000000000";
			}
			if((taskConfig.getCpSql().toLowerCase().indexOf("mped_id")<=-1)){//如果结果集中没MPED_ID,就是终端下所有测量点
				log.info("透传任务【"+autoTaskId+"】开始执行");
				//根据配置生成应采总数与终端信息
				subtasks = generateFromConfig(collectDateTmp,taskConfig);
				CacheUtil.clearTaskAllCpData(autoTaskId);
			}else {//采集部分测点，需要初始化临时缓存
				// 任务开始时将任务信息从临时缓存中清除。
				CacheUtil.delTaskScope(autoTaskId);
				CacheUtil.clearTaskAllCpData(autoTaskId);
				List<String[]> list = new ArrayList<String[]>();
				//组织任务执行测量点缓存
				list = measureListFromConfig(collectDateTmp, taskConfig,list);//临时缓存list
				subtasks = measureSetFromConfig(collectDateTmp, taskConfig);//子任务列表
				log.info("任务【"+autoTaskId+"】初始化透传任务执行测量点缓存"+list.size());
				CacheUtil.initTaskScope(list, autoTaskId);//临时缓存初始化

			}
		}
		//调整判断顺序，应采数为0直接跳出，不走初始化缓存状态，taskStatus为上次任务状态值 qin 2021-09-16
		if(null == subtasks || subtasks.size() == 0){
			log.error("任务【"+autoTaskId+"】任务的子任务列表初始化失败任务无法启动");
			return false;
		}

		//初始化缓存等信息
		if(null != taskStatus){
			CacheUtil.initTaskAbout(autoTaskId, taskStatus, collectDateTmp);
		}

		log.info("任务【"+autoTaskId+"】初始化任务与子任务队列["+subtasks.size()+"]开始");
		CacheUtil.initTask2Subtask(autoTaskId, subtasks, collectDateTmp);
		log.info("任务【"+autoTaskId+"】初始化任务与子任务队列["+subtasks.size()+"]完毕");
		// 主任务--子任务缓存初始化完毕后,启动子任务
		CacheUtil.setTaskExec(autoTaskId);
		log.info("任务【"+autoTaskId+"】启动子任务");
		return true;
	}

	@SuppressWarnings("deprecation")
	private static Set<String> generateFromConfig(String collectDate,AutoTaskConfig taskConfig) {
		String sql = taskConfig.getCpSql();
		String autoTaskId = taskConfig.getAutoTaskId();
		int rScope = taskConfig.getrScope();
		int mpNum = 0;
		int dataItemNum = 1;
		Set<String> subtaskList = new HashSet<String>();
		try {
			if(taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_TMNL)){//普通任务
				if(MainTaskDefine.R_SCOPE_TMNL==rScope){
					//通过配置sql，获取测量点总数
					mpNum = MainTaskDao.getTotalMeasureNum(taskConfig);
				} else if(MainTaskDefine.R_SCOPE_CP==rScope){
					//通过测量点信息表，获取测量点总数
					mpNum = MainTaskDao.getMeasurementNum(taskConfig);
				} else if(MainTaskDefine.R_SCOPE_TOTAL==rScope){
					//通过总加组信息表，获取总加组总数
//					mpNum = MainTaskDao.getTotalGroupNum(taskConfig);
				}
			}
			if (taskConfig.getIfBroadCast().equals(MainTaskDefine.IF_BROADCAST_MPED)) {//透传任务
				if((taskConfig.getCpSql().toLowerCase().indexOf("mped_id")<=-1)){//sql配置到终端
					mpNum = MainTaskDao.getMeasurementNum(taskConfig);
				}else {//sql配置到电表
					//通过配置sql，获取测量点总数
					mpNum = MainTaskDao.getTotalMeasureNum(taskConfig);
				}
			}
			if(mpNum == 0){
				log.error("任务【"+autoTaskId+"】的测量点数为："+mpNum);
				// 任务结束时将任务信息从内存中清除。
//				AutoTaskManage.removeTaskConfig(autoTaskId);
				return null;
			}

			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			Date tmpDate = new Date(startExecTime);
			String startExecTimeTmp= df.format(tmpDate.getTime());

			MainTaskDao.getSubTaskList(sql, autoTaskId, subtaskList, collectDate);//组织子任务列表subtaskList
			//获取应采数据项总数
			dataItemNum = DataItemCache.getDataitemIdByAutotaskId(autoTaskId).get(autoTaskId).size();
			itemTotal = mpNum * dataItemNum;
			log.info("任务【"+autoTaskId+"】应采总数为:"+itemTotal);
			taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDate,taskConfig.getPri(),0,SubTaskDefine.STATUS_COLLINFO_FROM_CONFIG,SubTaskDefine.STATUS_EXEC_FLAG_STOP,itemTotal);
			log.info("任务【"+autoTaskId+"】状态为："+taskStatus);
		} catch (Exception e) {
			log.error("",e);
		}
		return subtaskList ;
	}
	@SuppressWarnings("deprecation")
	private static List<String[]> customListFromConfig(String collectDate,AutoTaskConfig taskConfig, List<String[]> list) {
		String sql = taskConfig.getCpSql();
		String autoTaskId = taskConfig.getAutoTaskId();
//		int rScope = taskConfig.getrScope();
		int mpNum = 0;
		int dataItemNum = 1;
		Set<String> subtaskList = new HashSet<String>();
		try {
			//通过配置sql，获取测量点总数
			mpNum = MainTaskDao.getTotalMeasureNum(taskConfig);
			if(mpNum == 0){
				log.error("配置SQL任务【"+autoTaskId+"】的测量点数为："+mpNum);
				// 任务结束时将任务信息从内存中清除。
//				AutoTaskManage.removeTaskConfig(autoTaskId);
				return null;
			}

			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			Date tmpDate = new Date(startExecTime);
			String startExecTimeTmp= df.format(tmpDate.getTime());

			list = MainTaskDao.getSubTaskListForPenetrate(sql, autoTaskId, subtaskList,collectDate);
			//获取应采数据项总数
			dataItemNum = DataItemCache.getDataitemIdByAutotaskId(autoTaskId).get(autoTaskId).size();
			itemTotal = mpNum*dataItemNum;
			log.info("配置SQL任务【"+autoTaskId+"】应采总数为:"+itemTotal);
			taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDate,taskConfig.getPri(),0,SubTaskDefine.STATUS_COLLINFO_FROM_CONFIG,SubTaskDefine.STATUS_EXEC_FLAG_STOP,itemTotal);
			log.info("配置SQL任务【"+autoTaskId+"】状态为："+taskStatus);
		} catch (Exception e) {
			log.error("",e);
		}
		return list;
	}
	@SuppressWarnings("deprecation")
	private static List<String[]> measureListFromConfig(String collectDate,AutoTaskConfig taskConfig, List<String[]> list) {
		String sql = taskConfig.getCpSql();
		String autoTaskId = taskConfig.getAutoTaskId();
		int mpNum = 0;
		int dataItemNum = 1;
		Set<String> subtaskList = new HashSet<String>();
		try {
			//通过配置sql，获取测量点总数
			mpNum = MainTaskDao.getTotalMeasureNum(taskConfig);
			if(mpNum == 0){
				log.error("任务【"+autoTaskId+"】的测量点数为："+mpNum);
				// 任务结束时将任务信息从内存中清除。
//				AutoTaskManage.removeTaskConfig(autoTaskId);
				return null;
			}

			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			Date tmpDate = new Date(startExecTime);
			String startExecTimeTmp= df.format(tmpDate.getTime());

			list = MainTaskDao.getSubTaskListForPenetrate(sql, autoTaskId, subtaskList,collectDate);
			//获取应采数据项总数
			dataItemNum = DataItemCache.getDataitemIdByAutotaskId(autoTaskId).get(autoTaskId).size();
			itemTotal = mpNum*dataItemNum;
			log.info("任务【"+autoTaskId+"】应采总数为:"+itemTotal);
			taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, startExecTimeTmp,collectDate,taskConfig.getPri(),0,SubTaskDefine.STATUS_COLLINFO_FROM_CONFIG,SubTaskDefine.STATUS_EXEC_FLAG_STOP,itemTotal);
			log.info("任务【"+autoTaskId+"】状态为："+taskStatus);
		} catch (Exception e) {
			log.error("",e);
		}
		return list;
	}


	private static Set<String> customSetFromConfig(String collectDate,AutoTaskConfig taskConfig) {
		String sql = taskConfig.getCpSql();
		String autoTaskId = taskConfig.getAutoTaskId();
		Set<String> subtaskList = new HashSet<String>();
		try {
			subtaskList = MainTaskDao.getSubTaskSetForPenetrate(sql, autoTaskId, subtaskList, collectDate);
		} catch (Exception e) {
			log.error("",e);
		}
		return subtaskList;
	}
	private static Set<String> measureSetFromConfig(String collectDate,AutoTaskConfig taskConfig) {
		String sql = taskConfig.getCpSql();
		String autoTaskId = taskConfig.getAutoTaskId();
		Set<String> subtaskList = new HashSet<String>();
		try {
			subtaskList = MainTaskDao.getSubTaskSetForPenetrate(sql, autoTaskId, subtaskList, collectDate);
		} catch (Exception e) {
			log.error("",e);
		}
		return subtaskList;
	}

	private static boolean checkHisFull(String autoTaskId, String collectDate) {
//		return true;
		//由于重复采集问题太严重，暂时注释掉该部分判断--靳志强2018.8.27
		ResultSet rs = RautotaskRunHandle.getStartEndTime(autoTaskId,collectDate);
		java.sql.Date startExecTimeTmp = null;
		java.sql.Date endExecTime = null;
		boolean flag = false;
		try {
			while(rs.next()){
				startExecTimeTmp = rs.getDate("START_EXEC_TIME");
				endExecTime = rs.getDate("END_EXEC_TIME");
				if("".equals(startExecTimeTmp)||startExecTimeTmp == null||"".equals(endExecTime)||endExecTime==null){
					log.info("----历史执行不完整----");
				}else{
					flag = true;
					/*ResultSet rs2 = RautotaskRunInfoHandle.getTotalNum(autoTaskId,startExecTimeTmp,endExecTime);
					while(rs2.next()){
						int sumTotel = Integer.parseInt(rs2.getString("SUMTOTAL"));
						int sumFinished = Integer.parseInt(rs2.getString("SUMFINISHED"));
						if(0!=sumFinished){
							if((sumTotel-sumFinished)*100/sumFinished<=1){
								flag = true;
							}
						}else{
							log.info("-----完成数为0-----");
						}
					}*/
				}
			}
		} catch (SQLException e) {
			log.error("",e);
		}
		return flag;
	}

	public static boolean recollent(String autoTaskId, Integer hasRedoCount) {
		int itemTotal = 0;  //应采总数
		//获取补采的应采总数
		CacheUtil.setCurrentTaskOffset(autoTaskId, null, 0);
		// 从缓存中获取应采总数
		itemTotal = CacheUtil.getLeftMpCounter(autoTaskId,null).intValue();
		if(itemTotal == 0){
			return false;
		}
		log.info("任务【"+autoTaskId+"】补采的应采总数为："+itemTotal);
		//开始修改任务状态
		String[] status=CacheUtil.getTaskStatus(autoTaskId);
		String taskStatus = StringUtil.arrToStr(MainTaskDefine.SEPARATOR, status[0],status[1],status[2],Integer.parseInt(status[3])+1,2,SubTaskDefine.STATUS_EXEC_FLAG_EXECING,itemTotal);
		//初始化缓存等信息
		CacheUtil.initTaskAbout(autoTaskId, taskStatus, null);
		return true;
	}

	/**
	 * 计算采集时间
	 * @param taskConfig
	 * @return
	 */
	public static String getCollectDate(AutoTaskConfig taskConfig) {
		String collectDate = null;
		Calendar cal = Calendar.getInstance();
		int itemsScope = taskConfig.getItemsScope();
		// 数据项范围
		if(itemsScope == MainTaskDefine.ITEMS_SCOPE_CURRENT_TIME){// 3：实时数据
//			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
			collectDate= df.format(cal.getTime());
		}else if(itemsScope == MainTaskDefine.ITEMS_SCOPE_DAY_READ||itemsScope == MainTaskDefine.ITEMS_SCOPE_CURVE_READ||itemsScope == MainTaskDefine.ITEMS_SCOPE_TRANS_READ){//4：日冻结数据 7：曲线数据
			int dayScope = 0;
			if(null == taskConfig.getDayScope() || "".equals(taskConfig.getDayScope())){
				cal.add(Calendar.DAY_OF_MONTH, -1);
			} else {
				dayScope = Integer.parseInt(taskConfig.getDayScope());
				cal.add(Calendar.DAY_OF_MONTH, -dayScope);
			}
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
			collectDate= df.format(cal.getTime());
		}else if(itemsScope==MainTaskDefine.ITEMS_SCOPE_MON_READ){
			int dayScope = 0;
			if(null == taskConfig.getDayScope() || "".equals(taskConfig.getDayScope())){
				cal.add(Calendar.MONTH, -1);
			} else {
				dayScope = Integer.parseInt(taskConfig.getDayScope());
				cal.add(Calendar.MONTH, -dayScope);
			}
			cal.set(Calendar.DAY_OF_MONTH,1);
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
			collectDate= df.format(cal.getTime());
		}else if(itemsScope==MainTaskDefine.DATA_FLAG_DIRECT_MON_FREEZE_DATA){//直抄月冻结
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
			collectDate= df.format(cal.getTime());
		}else if(itemsScope == MainTaskDefine.ITEMS_SCOPE_CURRENT_CURVE_READ){//71：当日曲线数据
//			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
//			collectDate= df.format(cal.getTime());
			//			int runCycle = taskConfig.getRunCycle();
			int curveType = taskConfig.getCurveType();//曲线冻结密度  1：15分钟	2：30分钟3：60分钟
			int curveCondition = Integer.parseInt(taskConfig.getCurveCondition());
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			switch (curveType) {
				case 1:
					collectDate= df.format(getCurveRT(15,curveCondition));
					break;
				case 2:
					collectDate= df.format(getCurveRT(30,curveCondition));
					break;
				case 3:
					collectDate= df.format(getCurveRT(60,curveCondition));
					break;
			}
		}
		return collectDate;
	}

	/**
	 * 根据采集频率计算曲线时间
	 * @param interval
	 * @return
	 */
	private static Date getCurveRT(int interval, int curveCondition){
		Calendar calendar = Calendar.getInstance();
		int minute = calendar.get(Calendar.MINUTE);
		int mod = minute%interval;
		if(mod > 0){
			int divide = minute/interval;
			calendar.set(Calendar.MINUTE, divide*interval);
			calendar.add(Calendar.MINUTE, -interval*(curveCondition-1));
		} else if(mod == 0){
			calendar.add(Calendar.MINUTE, -interval*curveCondition);
		}
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		Calendar today = Calendar.getInstance();
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);
		today.set(Calendar.MILLISECOND, 0);

		if(today.getTime().compareTo(calendar.getTime()) < 0){
			return calendar.getTime();
		}
		return calendar.getTime();//会出现跨天
//		return today.getTime();
	}
}
