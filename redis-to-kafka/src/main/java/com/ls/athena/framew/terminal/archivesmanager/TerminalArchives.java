package com.ls.athena.framew.terminal.archivesmanager;

import com.ls.athena.core.ProcessorEvent;
import com.ls.pf.base.api.cache.ICache;
import com.tl.easb.utils.CacheUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * 终端档案
 * 
 * @author Administrator
 * 
 */
public class TerminalArchives implements ProcessorEvent {

	private static Logger logger = Logger.getLogger(TerminalArchives.class);

	private static final String Ter_Archives = "M$";
	private static final String Ter_Archives_Other = "THM$";
	private static final String ORG = "ORG";
	private static final String TMNLID = "TMNLID";

	private static final String freezeStcpHead = "STCP:100000014564_";

	private ICache archivesremoteRedisCache;
	private ICache archiveslocalCache;
	private int refreshTime = 5;

	private static TerminalArchives terminalArchives = null;

	public TerminalArchives() {
	}

	public TerminalArchives(ICache archivesremoteRedisCache,
			ICache archiveslocalCache) {
		if (terminalArchives != null) {
			throw new RuntimeException("对象只能构建一次!");
		}
		this.archivesremoteRedisCache = archivesremoteRedisCache;
		this.archiveslocalCache = archiveslocalCache;
		terminalArchives = this;
	}

	public static TerminalArchives getInstance() {
		if (terminalArchives == null) {
			throw new RuntimeException("terminalArchives系统未初始化，或者初始化失败!");
		}
		return terminalArchives;
	}
	/**
	 * 面向对象获取前置写入的真实表地址
	 * @param areaCode
	 * @param tmnlAddr
	 * @param meterAddr
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public String getRealMeterAddrFromLocalOop(String areaCode,String tmnlAddr,String meterAddr){
		String realAddr=meterAddr;
		String cacheKey="OOP_ATM#"+areaCode+"_"+tmnlAddr+"_"+meterAddr;
		Object obj=archivesremoteRedisCache.get(cacheKey);
		if (obj!=null) {
			realAddr=obj.toString();
		}
		return realAddr;
	}
	@SuppressWarnings("unchecked")
	public Map gettest(String key){
		Map m=archivesremoteRedisCache.hgetAll(key);
		return m;
	}
	/**
	 * 分支箱
	 * @param areaCode
	 * @param terminalAddr
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("all")
	public void putfzx(String areaCode,int terminalAddr,List datatoredis){
		String cachekey="FZX$"+areaCode+"#"+terminalAddr;
		Map<String,String> maps=new HashMap<String,String>();
		maps.put("ZCDB", String.valueOf(datatoredis.get(2)));
		maps.put("SJDB", String.valueOf(datatoredis.get(3)));
		maps.put("ZCBX", String.valueOf(datatoredis.get(4)));
		maps.put("SJBX", String.valueOf(datatoredis.get(5)));
		maps.put("ZCFZX", String.valueOf(datatoredis.get(6)));
		maps.put("SJFZX", String.valueOf(datatoredis.get(7)));
		for(Map.Entry<String, String> obj :maps.entrySet()){
			archivesremoteRedisCache.hput(cachekey,obj.getKey(),obj.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	public String getCmdForRedisOop(String redisName,String dataitemId){
		String key=redisName+"_"+dataitemId;
		Object val=archivesremoteRedisCache.get(key);
		return null==val?null:val.toString();
	}
	/**
	 * 查业务数据向缓存
	 * @param protocol3761ArchivesObject
	 * @param redisName
	 * @param dataitemId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public String getCmdForRedisOopPublic(Protocol3761ArchivesObject protocol3761ArchivesObject,String redisName,String dataitemId){
		int protoclId=protocol3761ArchivesObject.getProtocolId();
		String key=redisName+protoclId+"_"+dataitemId;
		Object val=archivesremoteRedisCache.get(key);
		return null==val?null:val.toString().substring(3);
	}


	/**
	 * 面向对象获取档案信息
	 * @param areaCode
	 * @param terminalAddr
	 * @param commAddr
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public TerminalArchivesObject getTerminalArchivesOop(
			String areaCode, String terminalAddr, String commAddr) {
		List<Object> list = null;
		Map<Object, Object> map = null;
		TerminalArchivesObject terminalArchivesObject = null;
		String powerUnitNumber;
		String terminalId;
		String id;
		String meterId;
		if (commAddr==null) {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#"+ terminalAddr + "#LINE");//M$4105#2779
			if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode+ "#" + terminalAddr, new String[] { ORG, TMNLID });//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识 
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				archiveslocalCache.put(Ter_Archives + areaCode + "#"+ terminalAddr + "#LINE", mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
		}else {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ "COMMADDR"+commAddr);//M$4105#2779
			if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode+ "#" + terminalAddr, new String[] { ORG, TMNLID, "COMMADDR"+commAddr,"MI"+commAddr });//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识 
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				id = (String) list.get(2);
				meterId=(String) list.get(3);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				mapvalue.put("COMMADDR"+commAddr, id);
				mapvalue.put("MI"+commAddr, meterId);
				archiveslocalCache.put(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ "COMMADDR"+commAddr, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
				id = (String) map.get("COMMADDR"+commAddr);
				meterId=(String) map.get("MI"+commAddr);
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
			terminalArchivesObject.setID(id);
			terminalArchivesObject.setMeterId(meterId);
		}
		return terminalArchivesObject;
	}
	@SuppressWarnings("unchecked")
	public TerminalArchivesObject getTerminalArchivesObjectLocal(
			String areaCode, int terminalAddr, String pn) {
		if (null != pn && pn.startsWith("COMM")) {
			logger.info("PN/COMMADDR === " + pn);
		}
		List<Object> list = null;
		Map<Object, Object> map = null;
		TerminalArchivesObject terminalArchivesObject = null;
		String powerUnitNumber;
		String terminalId;
		String id;
		if (null == pn || pn.equals("P0")) {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#" + terminalAddr);//M$
			if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode + "#" + terminalAddr, new String[] { ORG, TMNLID });
				if (null == list || list.isEmpty()) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60);
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
		} else {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ pn);//M$4105#2779
			if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode+ "#" + terminalAddr, new String[] { ORG, TMNLID, pn });//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识 
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				id = (String) list.get(2);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				mapvalue.put(pn, id);
				archiveslocalCache.put(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ pn, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
				id = (String) map.get(pn);
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
			terminalArchivesObject.setID(id);
		}
		return terminalArchivesObject;
	}

	@SuppressWarnings("unchecked")
	public TerminalArchivesObject getTerminalArchivesObjectLocalOther(String areaCode, int terminalAddr, String pn) {
		List<Object> list = null;
		Map<Object, Object> map = null;
		TerminalArchivesObject terminalArchivesObject = null;
		String powerUnitNumber;
		String terminalId;
		String id;
		if (null == pn || pn.equals("P0")) {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives_Other + areaCode + "#" + terminalAddr);
			if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives_Other + areaCode + "#" + terminalAddr, new String[] { ORG, TMNLID });
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives_Other + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				archiveslocalCache.put(Ter_Archives_Other + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60);
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
		} else {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives_Other + areaCode + "#" + terminalAddr);
			if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives_Other + areaCode + "#" + terminalAddr, new String[] { ORG, TMNLID, pn });
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives_Other + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				id = (String) list.get(2);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				mapvalue.put(pn, id);
				archiveslocalCache.put(Ter_Archives_Other + areaCode + "#" + terminalAddr + pn, mapvalue, refreshTime * 60);
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
				id = (String) map.get(pn);
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
			terminalArchivesObject.setID(id);
		}
		return terminalArchivesObject;
	}
	/**
	 * 电表全事件调用接口
	 * @param areaCode
	 * @param terminalAddr
	 * @param pn
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public TerminalArchivesObject getTerminalArchivesObjectMeter(
			String areaCode, int terminalAddr, String pn, String miComm) {
		if (null != pn && pn.startsWith("COMM")) {
		}
		List<Object> list = null;
		Map<Object, Object> map = null;
		TerminalArchivesObject terminalArchivesObject = null;
		String powerUnitNumber;
		String terminalId;
		String id;
		String meterId;
		if (null == pn || pn.equals("P0")) {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#" + terminalAddr);//M$
			if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode + "#" + terminalAddr, new String[] { ORG, TMNLID });
				if (null == list || list.isEmpty()) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				archiveslocalCache.put(Ter_Archives + areaCode + "#" + terminalAddr, mapvalue, refreshTime * 60);
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
		} else {
			map = (Map<Object, Object>) archiveslocalCache.get(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ pn);//M$4105#2779
			if (null == map || map.isEmpty()) {// 本地缓存没有，再从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives + areaCode+ "#" + terminalAddr, new String[] { ORG, TMNLID, pn, miComm });//一次取多个字段值 hmget ：供电单位编号，终端标识，测量点标识 
				if (null == list || list.isEmpty() || null == list.get(0)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				powerUnitNumber = (String) list.get(0);
				terminalId = (String) list.get(1);
				id = (String) list.get(2);
				if (null == list.get(3)) {
					throw new RuntimeException("无法获取终端["+Ter_Archives + areaCode + "#" + terminalAddr+"]的档案。。。");
				}
				meterId = (String) list.get(3);
				Map<String, String> mapvalue = new HashMap<String, String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				mapvalue.put(pn, id);
				mapvalue.put(miComm, meterId);
				archiveslocalCache.put(Ter_Archives + areaCode + "#"+ terminalAddr + "#"+ pn, mapvalue, refreshTime * 60); //那么本地缓存也需要打开  测试：查看本地缓存是否被写入
			} else {
				powerUnitNumber = (String) map.get("ORG");
				terminalId = (String) map.get("TMNLID");
				id = (String) map.get(pn);
				meterId = (String) map.get(miComm);
			}
			terminalArchivesObject = new TerminalArchivesObject(powerUnitNumber, terminalId);
			terminalArchivesObject.setID(id);
			terminalArchivesObject.setMeterId(meterId);
		}
		return terminalArchivesObject;
	}

	//查询测点(判断是否已采集，针对页面保存日冻结数据)2016/04/06
	public boolean isCollected(String areaCode, String terminalAddr, int pn, String cDate) {
		Jedis jedis = CacheUtil.getJedis();
//		Jedis jedis = CacheUtil.getJedis(CacheUtil.redisInstanceId);//2019-05-10 缓存修改
		//入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN]
		String stcpKey = freezeStcpHead + areaCode + "_" + terminalAddr + "_" + cDate;
		Set<String> stcpSet = jedis.smembers(stcpKey);//返回集合中的所有的成员。 不存在的集合 key 被视为空集合
		if(stcpSet == null || stcpSet.size() == 0) {
			return true;
		} else {
			String matcher = areaCode + "_" + terminalAddr + "_" + pn + "_" + cDate;
			Iterator<String> it = stcpSet.iterator();//使用迭代器
			while(it.hasNext()) {
				String collStr = it.next();
				if(collStr.startsWith(matcher)) {
					return false;
				}
			}
		}
		return true;
	}

	public void refreshCache(String key) {
		archiveslocalCache.remove(key);
	}

	@SuppressWarnings("unchecked")
	public Map<Object, Object> getCallInfoByToken(String token) {
		return archivesremoteRedisCache.hgetAll(token);
	}

	/**
	 * 这个方法仅供测试调用
	 * 
	 * @param areaCode
	 * @param terminalAddr
	 */
	public void setCacheByTest(String areaCode, int terminalAddr) {
		// Map hashValue = new HashMap();
		// hashValue.put(ORG, "123");
		// hashValue.put(TMNLID, "234");
		// hashValue.put("P1", "8888");
		// hashValue.put("T1", "9999");
		// remoteCache.hmput(Ter_Archives+areaCode+terminalAddr,hashValue);
	}

	public void start() throws Exception {
	}

	public void stop() throws Exception {
	}

	public int getRefreshTime() {
		return refreshTime;
	}

	public void setRefreshTime(int refreshTime) {
		this.refreshTime = refreshTime;
	}
}
