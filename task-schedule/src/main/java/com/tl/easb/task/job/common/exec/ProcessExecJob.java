package com.tl.easb.task.job.common.exec;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.task.job.common.CommonBaseJob;
import com.tl.easb.task.job.common.CommonJobDefine;
import com.tl.easb.task.job.common.procedure.Procedure;
import com.tl.easb.task.job.common.procedure.ProcedureHandle;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.task.thread.TaskThreadPool;
import com.tl.easb.utils.OorgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储过程调用
 * @author JinZhiQiang
 * @date 2014年4月15日
 */
public class ProcessExecJob extends CommonBaseJob {

	private static Logger log = LoggerFactory.getLogger(ProcessExecJob.class);
	//	private static ProcExecJob procExecJob;

	@Override
	protected void onExecute() {
		execProc(params,pdId,dataDate);
	}

	public ProcessExecJob(){

	}

	//	public static synchronized ProcExecJob getInstance(){
	//		if(null == procExecJob){
	//			procExecJob = new ProcExecJob();
	//		}
	//		return procExecJob;
	//	}

	/**
	 * 存储过程执行成功返回1，失败返回0
	 * @param procName
	 * @param orgNo
	 * @param isOrg
	 * @param pdId
	 * @return
	 */
	public static int doExecute(String procName, String orgNo, String isOrg, String pdId, String dataDate){
		int flag = 1;
		if(null == orgNo){
			orgNo = OorgUtils.getOrgNo("02").get(0);
		}
		BigDecimal plId = ProceduresLogHandle.init(pdId, orgNo);
		Connection conn = DBUtils.getConnection(ParamConstants.PROCEDURE_DATASOURCE_NAME);
		boolean isAutoCommit = true;
		CallableStatement proc = null;
		int result = 0;
		String outMessage = null;
		try {
			isAutoCommit = conn.getAutoCommit() ;
			conn.setAutoCommit(false);
			if(null == isOrg || "0".equals(isOrg)){
				log.info("存储过程【"+procName+"】（非机构），参数：{dataDate="+dataDate+"}开始执行~");
				proc = conn.prepareCall("{call " + procName + "(?,?,?)}");
				proc.setQueryTimeout(ParamConstants.PROCEDURE_RUN_TIMEOUT);// 设置执行超时时间
				proc.setString(1, dataDate);
				proc.registerOutParameter(2, oracle.jdbc.OracleTypes.NUMBER);
				proc.registerOutParameter(3, oracle.jdbc.OracleTypes.VARCHAR);
				proc.execute();
				conn.commit();
				// 获取执行结果
				result = proc.getInt(2);
				// 获取异常信息
				outMessage = proc.getString(3);
			} else if("1".equals(isOrg)){
				log.info("存储过程【"+procName+"】（按机构），参数：{dataDate="+dataDate+";orgNo="+orgNo+"}开始执行~");
				proc = conn.prepareCall("{call " + procName + "(?,?,?,?)}");
				proc.setQueryTimeout(ParamConstants.PROCEDURE_RUN_TIMEOUT);// 设置执行超时时间
				proc.setString(1, dataDate);
				proc.setString(2, orgNo);
				proc.registerOutParameter(3, oracle.jdbc.OracleTypes.NUMBER);
				proc.registerOutParameter(4, oracle.jdbc.OracleTypes.VARCHAR);
				proc.execute();
				conn.commit();
				// 获取执行结果
				result = proc.getInt(3);
				// 获取异常信息
				outMessage = proc.getString(4);
			}
			if(result == 1){
				ProceduresLogHandle.update(plId, result, "");
			} else {
				flag = 0;
				ProceduresLogHandle.update(plId, result, outMessage);
			}
			log.info("存储过程【"+procName+"】，参数：{dataDate="+dataDate+";orgNo="+orgNo+"}执行完毕~");
		} catch (Exception e) {
			flag = 0;
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error("",e1);
			}
			log.error("存储过程【" + procName + "】执行异常@ProcessExecJob.doExecute:", e);
			String errMsg = e.getMessage();
			outMessage = errMsg.length()>4000?errMsg.substring(0, 4000):errMsg;
			ProceduresLogHandle.update(plId, 0, outMessage);
		} finally {
			try {
				conn.setAutoCommit(isAutoCommit);
			} catch (SQLException e) {
				log.error("",e);
			}
			DBUtils.close(conn);
		}
		return flag;
	}

	private static int execute(String procName, String orgNo, String isOrg, String pdId, String dataDate){
		return doExecute(procName, orgNo, isOrg, pdId, dataDate);
	}

	/**
	 * 调用存储过程
	 * @param procName
	 * @param pdId
	 * @param dataDate
     */
	public static void execProc(final String procName, String pdId, String... dataDate) {

		Procedure procedure = ProcedureHandle.getProcedureByPdId(pdId);
		final String isOrg = procedure.getIsOrg();// 是否支持机构：0：不支持  1：支持
		final String isSyn = procedure.getIsSyn();// 是否并发执行:0:否  1:是
		String dataDateStr = null;
		if(null != dataDate && dataDate.length>0){
			dataDateStr = dataDate[0];
		}
		int succNum = 0;// 执行成功的存储过程数
		int oorgSize = 1;
		if(null == isOrg || "0".equals(isOrg)){
			succNum = doExecute(procName, null, isOrg, pdId, dataDateStr);
		} else if("1".equals(isOrg)){
			List<String> orgNos = OorgUtils.getOrgNo("03");
			oorgSize = orgNos.size();
			if(null == isSyn || "0".equals(isSyn)){
				for(int j=0;j<oorgSize;j++){
					succNum += execute(procName, orgNos.get(j), isOrg, pdId, dataDateStr);
				}
			} else if("1".equals(isSyn)){
				final String pdIdStr = pdId;
				final String dateStr = dataDateStr;
				int threadPoolSize = orgNos.size();
				ArrayList<Future<Integer>> results = new ArrayList<Future<Integer>>(threadPoolSize);
				ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize, new TaskThreadPool("ProcedureExecThreadPool"));
				for (int i = 0; i < threadPoolSize; i++) {
					final String orgNo = orgNos.get(i);
					results.add(
							threadPool.submit(
									new Callable<Integer>() {
										public Integer call() {
											return ProcessExecJob.execute(procName,orgNo,isOrg,pdIdStr,dateStr);
										}
									})
							);
				}
				
				while(true){
					if(!isAllThreadsDone(results)){
						try {
							TimeUnit.SECONDS.sleep(1);
						} catch (InterruptedException e) {
							log.error("",e);
						}
					} else {
						break;
					}
				}
				succNum = getSuccThreadNum(results);
				threadPool.shutdown();
			}
		}
		// 只有当各地市存储过程执行成功后，才调用其后置存储过程
		// succNum >= oorgSize
		if(succNum >= 1){
			if(null != pdId/* && !"1".equals(isSyn)*/){
				nextExec(pdId,CommonJobDefine.RUN_MODE_AFTER_PROCEDURE,dataDateStr);
			}
		}
	}

	/**
	 * 统计存储过程执行成功的线程数
	 * @param results
	 * @return
	 */
	private static int getSuccThreadNum(ArrayList<Future<Integer>> results){
		int succNum = 0;
		for(Future<Integer> future : results){
			try {
				succNum += future.get();
			} catch (InterruptedException e) {
				log.error("",e);
			} catch (ExecutionException e) {
				log.error("",e);
			}
		}
		return succNum;
	}

	/**
	 * 判断所有线程是否执行完成
	 * @param results
	 * @return true：所有线程均已完成，false：存在未完成的线程
	 */
	private static boolean isAllThreadsDone(ArrayList<Future<Integer>> results){
		for(Future<Integer> future : results){
			if(!future.isDone()){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 获取跟随在本存储过程之后的存储过程，并执行
	 * @param id
	 */
	public static void nextExec(String id, int runMode, String dataDate){
		if(null != dataDate && dataDate.length() > 6){
			dataDate = dataDate.substring(0, 8);
		}
		log.info("任务ReplyId【"+id+"】的后置存储过程执行方式【"+runMode+"】，数据日期【"+dataDate+"】开始执行...");
		List<Procedure> procedures = ProcedureHandle.getProcedureByReplyId(id,runMode);
		if(null == procedures || procedures.size() == 0){
			return;
		}

		int threadPoolSize= procedures.size();
		ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize,new TaskThreadPool("NextProcThreadPool"));

		for(Procedure procedure : procedures){
			final String proces = procedure.getPackCode()+"."+procedure.getProCode();
			final String pdId = procedure.getPdId().toString();
			final String dataDateStr = dataDate;
			threadPool.execute(new Runnable() {
				public void run() {
					execProc(proces, pdId, dataDateStr);
				}
			});
		}
		threadPool.shutdown();
	}
	
	/**
	 * 执行去重存储过程
	 * @return 1 成功 0 失败
	 * @param procName 存储过程名称
	 */
	public static int execProc_meger(final String procName, String dataDate){
		int flag=0;
		log.info("存储过程[" + procName + "]开始执行！冻结日期："+dataDate);
		long startTime = System.currentTimeMillis();
		Connection conn=null;
		boolean isAutoCommit = true;
		try {
			conn=DBUtils.getConnection();
			isAutoCommit = conn.getAutoCommit() ;
			CallableStatement proc = conn.prepareCall("{call " + procName + "(?,?,?)"+"}");
			proc.setString(1, dataDate);
			proc.registerOutParameter(2, oracle.jdbc.OracleTypes.NUMBER);
			proc.registerOutParameter(3, oracle.jdbc.OracleTypes.VARCHAR);
			proc.execute();
			flag=proc.getInt(2);
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error("",e1);
			}
			log.error("执行存储【" + procName + "】过程异常@ProcExecuter.execProc！", e);
		}finally{
			try {
				conn.setAutoCommit(isAutoCommit);
			} catch (SQLException e) {
				log.error("",e);
			}
			DBUtils.close(conn);
		}
		log.info("存储过程[" + procName + "]执行完成，执行耗时：" + (System.currentTimeMillis() - startTime)/1000/60 + "分钟!");
		
		return flag;
	}



	/**
	 * 调用存储过程
	 * @param proces
	 * @param params
	 */
//	@Deprecated
//	protected void execProc_bak(final String proces, String pdId, Object... params) {
//		final String[] procs = proces.split(",");
//		String _paramsTmp = "";
//		Object[] _objects = null;
//		if (params != null) {
//			_objects = new Object[params.length];
//			for (int index = 0; index < params.length; index++) {
//				_objects[index] = params[index];
//				_paramsTmp += "?";
//				if (index < params.length - 1) {
//					_paramsTmp += ",";
//				}
//			}
//		}
//		String procName = "";
//		for (int i = 0; i < procs.length; i++) {
//			procName = procs[i];
//			if (StringUtil.isBlank(procName)) {
//				log.info("存储过程[" + proces + "]中，第[" + i + "]个过程配置为空，忽略执行该过程！");
//				continue;
//			}
//			long startTime = System.currentTimeMillis();
//			log.info("存储过程[" + procName + "]开始执行！");
//			Connection conn = null;
//			boolean isAutoCommit = true;
//			try {
//				conn = DBUtils.getConnection();
//				isAutoCommit = conn.getAutoCommit() ;
//				conn.setAutoCommit(false);
//				CallableStatement proc = conn.prepareCall("{call " + procName + "(" + _paramsTmp + ",?,?)}");
//				for (int index = 0; index < _objects.length; index++) {
//					Object _obj = _objects[index];
//					if (_obj instanceof String) {
//						proc.setString(index + 1, String.valueOf(_obj));
//					} else if (_obj instanceof Integer) {
//						proc.setInt(index + 1, Integer.parseInt(_obj.toString()));
//					} else if (_obj instanceof java.sql.Date) {
//						proc.setDate(index + 1, (java.sql.Date) _obj);
//					} else if (_obj instanceof Double) {
//						proc.setDouble(index + 1, Double.parseDouble(_obj.toString()));
//					} else {
//						proc.setObject(index + 1, _obj);
//					}
//				}
//				proc.registerOutParameter(_objects.length, oracle.jdbc.OracleTypes.NUMBER);
//				proc.registerOutParameter(_objects.length+1, oracle.jdbc.OracleTypes.VARCHAR);
//				proc.execute();
//				conn.commit();
//				// 获取执行结果
//				int result = proc.getInt(_objects.length);
//				// 获取异常信息
//				String outMessage = proc.getString(_objects.length+1);
//
//				System.out.println(result);
//				System.out.println(outMessage);
//			} catch (Exception e) {
//				try {
//					conn.rollback();
//				} catch (SQLException e1) {
//					log.error(e1);
//				}
//				log.error("执行存储【" + proces + "】过程异常@ProcExecuter.execProc！", e);
//			} finally {
//				try {
//					conn.setAutoCommit(isAutoCommit);
//				} catch (SQLException e) {
//					log.error(e);
//				}
//				DBUtils.close(conn);
//			}
//			log.info("存储过程[" + procName + "]执行完成，执行耗时：" + (System.currentTimeMillis() - startTime) + "毫秒!");
//			// 执行跟随在此存储过程之后的存储过程
//			if(null != pdId){
//				nextExec(pdId,CommonJobDefine.RUN_MODE_AFTER_PROCEDURE);
//			}
//		}
//	}
}
