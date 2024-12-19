package com.ls.athena.framew.terminal.archivesmanager;

import com.ls.athena.core.ProcessorEvent;
import com.ls.pf.base.api.cache.ICache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 终端档案
 * @author Administrator
 *
 */
public class TerminalArchives implements ProcessorEvent{
	
	private static final String Ter_Archives="M$";
	private static final String ORG="ORG";
	private static final String TMNLID="TMNLID";
	
	private  ICache archivesremoteRedisCache;
	private  ICache archiveslocalCache;
	private  int refreshTime=5;
	
	private static TerminalArchives terminalArchives=null;
	
	public TerminalArchives(){}
	public TerminalArchives(ICache archivesremoteRedisCache,ICache archiveslocalCache){
		if (terminalArchives != null){
			throw new RuntimeException("对象只能构建一次!");
		}
		this.archivesremoteRedisCache = archivesremoteRedisCache;
		this.archiveslocalCache=archiveslocalCache;
		terminalArchives = this;
	}

	public static TerminalArchives getInstance(){
		if (terminalArchives == null){
			throw new RuntimeException("terminalArchives系统未初始化，或者初始化失败!");
		}
		return terminalArchives;
	}
	public TerminalArchivesObject getTerminalArchivesObjectLocal(String areaCode, int terminalAddr,String pn ){
		List<Object> list=null;
		Map map=null;
		TerminalArchivesObject terminalArchivesObject=null;
		String powerUnitNumber;
		String terminalId;
		String id;
		if(null==pn||pn.equals("P0")){
			map = (Map) archiveslocalCache.get(Ter_Archives+areaCode+"#"+terminalAddr);
			if(null==map||map.isEmpty()){//本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives+areaCode+"#"+terminalAddr,new String[]{ORG,TMNLID});
				if(null==list||list.isEmpty()){
//					System.out.println("areaCode"+areaCode+"terminalAddr"+terminalAddr+"pn"+pn);
//					throw new RuntimeException("无法获取终端档案。。。");
					return null;
				}
				powerUnitNumber=(String)list.get(0);
				terminalId=(String)list.get(1);
				Map<String,String> mapvalue=new HashMap<String,String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				archiveslocalCache.put(Ter_Archives+areaCode+"#"+terminalAddr, mapvalue,refreshTime*60);
				}else{
					powerUnitNumber=(String) map.get("ORG");
					terminalId=(String) map.get("TMNLID");
				}
			terminalArchivesObject=new TerminalArchivesObject(powerUnitNumber, terminalId);
		}else{
			map = (Map) archiveslocalCache.get(Ter_Archives+areaCode+"#"+terminalAddr+pn);
			if(null==map||map.isEmpty()){//本地缓存没有，从远程取
				list = archivesremoteRedisCache.hmget(Ter_Archives+areaCode+"#"+terminalAddr,new String[]{ORG,TMNLID,pn});
				if(null==list||list.isEmpty()){
//					System.out.println("areaCode"+areaCode+"terminalAddr"+terminalAddr+"pn"+pn);
//					throw new RuntimeException("无法获取终端档案。。。");
					return null;
				}
				powerUnitNumber=(String)list.get(0);
				terminalId=(String)list.get(1);
				id=(String)list.get(2);
				Map<String,String> mapvalue=new HashMap<String,String>();
				mapvalue.put("ORG", powerUnitNumber);
				mapvalue.put("TMNLID", terminalId);
				mapvalue.put(pn, id);
				archiveslocalCache.put(Ter_Archives+areaCode+"#"+terminalAddr+pn, mapvalue,refreshTime*60);
				}else{
					powerUnitNumber=(String) map.get("ORG");
					terminalId=(String) map.get("TMNLID");
					id=(String) map.get(pn);
				}
			terminalArchivesObject=new TerminalArchivesObject(powerUnitNumber, terminalId);
			terminalArchivesObject.setID(id);
		}
		return terminalArchivesObject;
	}
	public void refreshCache(String key){
		archiveslocalCache.remove(key);
	}
	
	public Map getCallInfoByToken(String token){
		return archivesremoteRedisCache.hgetAll(token);
	}
	/**
	 * 这个方法仅供测试调用
	 * @param areaCode
	 * @param terminalAddr
	 */
	public void setCacheByTest(String areaCode, int terminalAddr){
//		Map hashValue = new HashMap();
//		hashValue.put(ORG, "123");
//		hashValue.put(TMNLID, "234");
//		hashValue.put("P1", "8888");
//		hashValue.put("T1", "9999");
//		remoteCache.hmput(Ter_Archives+areaCode+terminalAddr,hashValue);
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
