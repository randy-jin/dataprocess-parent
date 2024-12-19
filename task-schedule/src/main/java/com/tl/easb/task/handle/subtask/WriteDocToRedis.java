package com.tl.easb.task.handle.subtask;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.ls.pf.base.exception.PlatformError;
import com.ls.pf.common.dataCatch.bz.bo.ObjectBz;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import com.ls.pf.common.dataCath.bz.service.impl.OperateBzData;
import com.tl.easb.utils.SpringUtils;
import com.tl.easb.utils.StringUtil;

/**
 * 档案加载测试类
 * 
 * @date 2014年4月12日
 */
public class WriteDocToRedis {
	private static Logger log = LoggerFactory.getLogger(WriteDocToRedis.class);
	private static final String SEPARATOR = "#";

	// 根据本机内存配置可修改该值，建议如果jvm最大内存为512M的情况下配置为1000~3000，如果为1024M可配置为5000~10000
	private static final int step = 100;

	public static final String GET_MPED_INFO_SQL = "select rm.IMPORTANT,rm.USERFLAG,rm.MPED_ID,rm.MPED_INDEX,rm.PROTOCOL_ID,rm.BAUD,rm.BYTESIZE,rm.PARITY,rm.STOPBITS,rm.port_no,rm.meter_id,rm.COMM_ADDR,  rm.ORG_NO,  rm.CHECK_FLAG,  rm.LIMIT_TIME,  rm.TERMINAL_ID from R_MPED rm where rm.SHARD_NO=? and rm.TERMINAL_ID=?";
	private static IOperateBzData operateBzData = null;

	public static List<ObjectBz> getDocInfo(String shardNo, int startIdx) {
		List<ObjectBz> doclist = new ArrayList<ObjectBz>();
		ObjectBz objBz = null;
		String GET_CP_AND_TMNL_RUN_INFO_SQL = "SELECT RUN.ORG_NO, RUN.TERMINAL_ID, RUN.CP_NO, RUN.TERMINAL_ADDR, RUN.AREA_CODE, RUN.PROTOCOL_ID FROM R_TMNL_INFO RUN WHERE RUN.SHARD_NO = ? order by RUN.ORG_NO,  RUN.TERMINAL_ID,  RUN.CP_NO,  RUN.TERMINAL_ADDR,  RUN.AREA_CODE LIMIT "
				+ startIdx + " , " + step;
		long startTimeQueryTmnl = System.currentTimeMillis();
		ResultSet tmnlInfoRs = DBUtils.executeQuery(GET_CP_AND_TMNL_RUN_INFO_SQL, new Object[] { shardNo });
		log.info("Execute GET_CP_AND_TMNL_RUN_INFO_SQL took " + (System.currentTimeMillis() - startTimeQueryTmnl)
				+ " Millisecond");
		try {
			while (tmnlInfoRs.next()) {
				objBz = new ObjectBz();
				if (!StringUtils.hasLength(tmnlInfoRs.getString("AREA_CODE"))
						|| !StringUtils.hasLength(tmnlInfoRs.getString("TERMINAL_ADDR"))) {
					continue;
				}
				objBz.setAreaCode(tmnlInfoRs.getString("AREA_CODE"));
				objBz.setTermAdd(tmnlInfoRs.getString("TERMINAL_ADDR"));
				objBz.setTmnlId(tmnlInfoRs.getString("TERMINAL_ID"));
				objBz.setDepartCode(tmnlInfoRs.getString("ORG_NO"));
				objBz.setProtocolId(tmnlInfoRs.getString("PROTOCOL_ID"));

				String tmnlId = tmnlInfoRs.getString("TERMINAL_ID");
				// 获取测量点信息
				Map<String, String> pointNum2Map = new HashMap<String, String>();
				Map<String, String> commAddr2Map = new HashMap<String, String>();
				Map<String, String> userFlagMap = new HashMap<String, String>();
				Map<String, String> meterIdMap = new HashMap<String, String>();
				List<Map<String, String>> meterAttributeList = new ArrayList<Map<String, String>>();
				// 2016-7-6杭州start 多规约测量点档案缓存
				List<Map<String, String>> meterMPList = new ArrayList<Map<String, String>>();
				// long startTimeQueryMped = System.currentTimeMillis();
				ResultSet mpedInfoRs = DBUtils.executeQuery(GET_MPED_INFO_SQL, new String[] { shardNo, tmnlId });
				// log.info("Execute GET_MPED_INFO_SQL took " +
				// (System.currentTimeMillis()-startTimeQueryMped)+"
				// Millisecond");
				try {
					while (mpedInfoRs.next()) {
						Map<String, String> meterAttribute = new HashMap<String, String>();
						String mpedIndex = mpedInfoRs.getString("MPED_INDEX");
						if (StringUtil.isBlank(mpedIndex)) {
							continue;
						}
						String COMM_ADDR1 = mpedInfoRs.getString("COMM_ADDR");
						String MPED_ID = mpedInfoRs.getString("MPED_ID");
						pointNum2Map.put(mpedIndex, MPED_ID);
						meterIdMap.put(mpedIndex, mpedInfoRs.getString("meter_id"));
						if (COMM_ADDR1 != null && !COMM_ADDR1.equals("")) {
							commAddr2Map.put(COMM_ADDR1, MPED_ID);
						}
						userFlagMap.put(mpedIndex, mpedInfoRs.getString("USERFLAG"));
						meterAttribute.put("ADDR" + SEPARATOR + mpedIndex, COMM_ADDR1);
						meterAttribute.put("STOPBITS" + SEPARATOR + mpedIndex, mpedInfoRs.getString("STOPBITS"));
						meterAttribute.put("PARITY" + SEPARATOR + mpedIndex, mpedInfoRs.getString("PARITY"));
						meterAttribute.put("BYTESIZE" + SEPARATOR + mpedIndex, mpedInfoRs.getString("BYTESIZE"));
						meterAttribute.put("BAUDRATE" + SEPARATOR + mpedIndex, mpedInfoRs.getString("BAUD"));
						meterAttribute.put("PORT" + SEPARATOR + mpedIndex, mpedInfoRs.getString("PORT_NO"));
						meterAttribute.put("MPT" + SEPARATOR + mpedIndex, mpedInfoRs.getString("IMPORTANT"));
						meterAttribute.put("MPID" + SEPARATOR + mpedIndex, mpedInfoRs.getString("PROTOCOL_ID"));
						meterAttributeList.add(meterAttribute);

						// 2016-7-6杭州start 多规约测量点档案缓存
						Map<String, String> meterMPAttribute = new HashMap<String, String>();
						String mpedId = mpedInfoRs.getString("MPED_ID");
						if (StringUtil.isBlank(mpedId)) {
							continue;
						}
						String param = mpedInfoRs.getString("BAUD") + "|" + mpedInfoRs.getString("STOPBITS") + "|"
								+ mpedInfoRs.getString("CHECK_FLAG") + "|" + mpedInfoRs.getString("PARITY") + "|"
								+ mpedInfoRs.getString("LIMIT_TIME") + "|" + "1";// 波特率|停止位|无/有校验|偶/奇校验|
						// 5-8
						// 位数（无）|报文超时时间|报文超时时间单位
						meterMPAttribute.put("MPINDEX" + SEPARATOR + mpedId, mpedInfoRs.getString("MPED_INDEX"));
						meterMPAttribute.put("ORG" + SEPARATOR + mpedId, mpedInfoRs.getString("ORG_NO"));
						meterMPAttribute.put("ADDR" + SEPARATOR + mpedId, mpedInfoRs.getString("COMM_ADDR"));
						meterMPAttribute.put("PTYPE" + SEPARATOR + mpedId, "1");
						meterMPAttribute.put("PTL" + SEPARATOR + mpedId, mpedInfoRs.getString("PROTOCOL_ID"));
						meterMPAttribute.put("PARAM" + SEPARATOR + mpedId, param);
						meterMPAttribute.put("PORT" + SEPARATOR + mpedId, mpedInfoRs.getString("PORT_NO"));
						meterMPAttribute.put("TMID" + SEPARATOR + mpedId, mpedInfoRs.getString("TERMINAL_ID"));
						meterMPAttribute.put("MPT" + SEPARATOR + mpedId, mpedInfoRs.getString("IMPORTANT"));
						// meterAttribute.put("DJ" + SEPARATOR + mpedId, json);
						meterMPList.add(meterMPAttribute);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				// log.info("GET_MPED_INFO took " +
				// (System.currentTimeMillis()-startTimeQueryMped)+"
				// Millisecond");

				objBz.setPointNum(pointNum2Map);
				objBz.setCommAddr(commAddr2Map);
				objBz.setUserFlag(userFlagMap);
				// objBz.setMeterIdMap(meterIdMap);
				objBz.setMeterAttribute(meterAttributeList);
				// 2016-7-6杭州start 多规约测量点档案缓存
				objBz.setMeterMP(meterMPList);

				Map<String, String> portNum2Map = new HashMap<String, String>();
				portNum2Map.put("1", "1");
				objBz.setPortNum(portNum2Map);

				if (pointNum2Map.size() >= 1) {
					doclist.add(objBz);
				}
			}
		} catch (PlatformError e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		log.info("Execute GET_CP_AND_TMNL_RUN_INFO took " + (System.currentTimeMillis() - startTimeQueryTmnl)
				+ " Millisecond");
		return doclist;
	}

	public static void main(String args[]) {
		// 组成：开始时间_采集日期_优先级_补采次数_采集信息生成方式_执行标识_应采总数
		// ApplicationContext factory = new
		// FileSystemXmlApplicationContext("classpath:spring/*.xml");
		// operateBzData =factory.getBean("operateBzData",OperateBzData.class);
		operateBzData = (OperateBzData) SpringUtils.getBean("operateBzData");

		ResultSet rs = DBUtils.executeQuery("select shard_no,count(1) from R_TMNL_INFO group by SHARD_NO", null);
		try {
			while (rs.next()) {
				final String shardNo = rs.getString("shard_no");
				Thread thread = new Thread(new Runnable() {
					public void run() {
						int startIdx = 0;
						int endIdx = 0;
						long startTime = System.currentTimeMillis();
						List<ObjectBz> doclist = null;
						int docSum = 0;
						while (true) {
							long nstartTime = System.currentTimeMillis();
							doclist = getDocInfo(shardNo, startIdx);
							log.info("获取doclist[" + doclist.size() + "]耗时："
									+ (System.currentTimeMillis() - nstartTime) / 1000 + "秒");
							if (null == doclist || doclist.size() == 0) {
								break;
							}
							docSum += doclist.size();
							operateBzData.saveObj(doclist);
							endIdx = startIdx + step;
							log.info("从[" + (startIdx + 1) + "]到[" + endIdx + "]加载完毕，加载数量【" + doclist.size() + "】，耗时："
									+ (System.currentTimeMillis() - nstartTime) / 1000 + "秒");
							if (endIdx >= 5000) {
								break;
							}
							startIdx = endIdx;
						}
						log.info("地市" + shardNo + "的[" + docSum + "]条采集点档案信息已全部加载redis完毕，耗时："
								+ (System.currentTimeMillis() - startTime) / 1000 + "秒");
					}
				}, "sync_thread-" + shardNo);
				thread.start();
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}
}
