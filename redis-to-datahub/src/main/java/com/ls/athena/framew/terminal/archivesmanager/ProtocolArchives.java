package com.ls.athena.framew.terminal.archivesmanager;

import com.ls.athena.core.ProcessorEvent;
import com.ls.pf.base.api.cache.ICache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ProtocolArchives implements ProcessorEvent {

	private static Logger logger = LoggerFactory.getLogger(ProtocolArchives.class);

	private static final String PROTOCOL_Archives = "PT$";

	private ICache archivesremoteRedisCache;
	private ICache archiveslocalCache;
	private int refreshTime = 5;

	private static ProtocolArchives protocolArchives = null;

	public ProtocolArchives() {
	}

	public ProtocolArchives(ICache archivesremoteRedisCache, ICache archiveslocalCache) {
		if (protocolArchives != null) {
			throw new RuntimeException("对象只能构建一次!");
		}
		this.archivesremoteRedisCache = archivesremoteRedisCache;
		this.archiveslocalCache = archiveslocalCache;
		protocolArchives = this;
	}

	public static ProtocolArchives getInstance() {
		if (protocolArchives == null) {
			throw new RuntimeException("protocolArchives系统未初始化，或者初始化失败!");
		}
		return protocolArchives;
	}
	
	public static void main(String[] args) {
		System.out.println(Integer.parseInt("0D", 16));
		System.out.println(Integer.toHexString(13));
	}
	
	/**
	 * jiaocai
	 * protocolId=1  protocol288Id =1288
	 * @param protocolArchivesObject
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public ProtocolArchivesObject getProtocolArchivesObject288(ProtocolArchivesObject protocolArchivesObject) {
		String field = null;
		int protocolId = protocolArchivesObject.getProtocolId();
		if (protocolArchivesObject instanceof Protocol3761ArchivesObject) {
			Protocol3761ArchivesObject protocol3861ArchivesObject = (Protocol3761ArchivesObject) protocolArchivesObject;
			String afn_str = Integer.toHexString(protocol3861ArchivesObject.getAfn());
			if(afn_str.length()==1){
				afn_str = "0" + afn_str.toUpperCase();
			}
			field = protocolId + "-" + afn_str + "-" + protocol3861ArchivesObject.getFn();
		} else {
			throw new RuntimeException("没有匹配的PROTOCOL_ID为[" + protocolId + "]的规约对象类型");
		}
		String protocol288Id=String.valueOf(protocolId)+"288";
		Object obj = null;
		Map<Object, Object> map = null;
		String busiDataItemId = null;
		logger.info("vol_288  hget "+PROTOCOL_Archives + protocol288Id+" "+field);
		map = (Map<Object, Object>) archiveslocalCache.get(PROTOCOL_Archives + protocol288Id);
		if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
			obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocol288Id, field);
			if (null == obj) {
				throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocol288Id + "]的规约档案。。。");
			}
			Map<String, String> mapvalue = new HashMap<String, String>();
			busiDataItemId = String.valueOf(obj);
			mapvalue.put(field, busiDataItemId);
			archiveslocalCache.put(PROTOCOL_Archives + protocol288Id, mapvalue, refreshTime * 60);
		} else {
			if(map.get(field) == null){
				obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocol288Id, field);
				if (null == obj) {
					throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocol288Id + "]的规约档案。。。");
				}
				busiDataItemId = String.valueOf(obj);
				map.put(field, busiDataItemId);
				archiveslocalCache.put(PROTOCOL_Archives + protocol288Id, map, refreshTime * 60);
			} else {
				busiDataItemId = (String) map.get(field);
			}
		}
		protocolArchivesObject.setBusiDataItemId(busiDataItemId);
		return protocolArchivesObject;
	}


	@SuppressWarnings("unchecked")
	public ProtocolArchivesObject getProtocolArchivesObject(ProtocolArchivesObject protocolArchivesObject) {
		String field = null;
		int protocolId = protocolArchivesObject.getProtocolId();
		if (protocolArchivesObject instanceof Protocol3761ArchivesObject) {
			Protocol3761ArchivesObject protocol3861ArchivesObject = (Protocol3761ArchivesObject) protocolArchivesObject;
			String afn_str = Integer.toHexString(protocol3861ArchivesObject.getAfn());
			if(afn_str.length()==1){
				afn_str = "0" + afn_str.toUpperCase();
			}else {
				afn_str=afn_str.toUpperCase();
			}
			field = protocolId + "-" + afn_str + "-" + protocol3861ArchivesObject.getFn();
		} else {
			throw new RuntimeException("没有匹配的PROTOCOL_ID为[" + protocolId + "]的规约对象类型");
		}
		Object obj = null;
		Map<Object, Object> map = null;
		String busiDataItemId = null;
		map = (Map<Object, Object>) archiveslocalCache.get(PROTOCOL_Archives + protocolId);
		if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
			obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, field);
			if (null == obj) {
				throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
			}
			Map<String, String> mapvalue = new HashMap<String, String>();
			busiDataItemId = String.valueOf(obj);
			mapvalue.put(field, busiDataItemId);
			archiveslocalCache.put(PROTOCOL_Archives + protocolId, mapvalue, refreshTime * 60);
		} else {
			if(map.get(field) == null){
				obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, field);
				if (null == obj) {
					throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
				}
				busiDataItemId = String.valueOf(obj);
				map.put(field, busiDataItemId);
				archiveslocalCache.put(PROTOCOL_Archives + protocolId, map, refreshTime * 60);
			} else {
				busiDataItemId = (String) map.get(field);
			}
		}
		protocolArchivesObject.setBusiDataItemId(busiDataItemId);
		return protocolArchivesObject;
	}
	@SuppressWarnings("unchecked")
	public ProtocolArchivesObject getProtocolArchivesObjectByOop(ProtocolArchivesObject protocolArchivesObject, String cmd) {
		cmd=cmd.trim();
		int protocolId = 9;
		String key=protocolId+"-"+cmd+"-"+cmd;
		Object obj = null;
		Map<Object, Object> map = null;
		String busiDataItemId = null;
		map = (Map<Object, Object>) archiveslocalCache.get(PROTOCOL_Archives + protocolId);
		if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
			obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, key);
			if (null == obj) {
				throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
			}
			Map<String, String> mapvalue = new HashMap<String, String>();
			busiDataItemId = String.valueOf(obj);
			mapvalue.put(key, busiDataItemId);
			archiveslocalCache.put(PROTOCOL_Archives + protocolId, mapvalue, refreshTime * 60);
		} else {
			if(map.get(key) == null){
				obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, key);
				if (null == obj) {
					throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
				}
				busiDataItemId = String.valueOf(obj);
				map.put(key, busiDataItemId);
				archiveslocalCache.put(PROTOCOL_Archives + protocolId, map, refreshTime * 60);
			} else {
				busiDataItemId = (String) map.get(key);
			}
		}
		protocolArchivesObject.setBusiDataItemId(busiDataItemId);
		return protocolArchivesObject;
	}
	//直抄 8
		@SuppressWarnings("unchecked")
		public ProtocolArchivesObject getProtocolArchivesObjectByOop698(ProtocolArchivesObject protocolArchivesObject, String cmd) {
			cmd=cmd.trim();
			int protocolId =protocolArchivesObject.getProtocolId();
			String key;
			if(protocolId==8){
				//直抄查档案
				key= 9 +"-"+cmd+"-"+cmd+"|"+ 9 +"-"+cmd+"-"+cmd;
			}else{
				//预抄查档案
				key=9 +"-"+cmd+"-"+cmd;
			}
			Object obj = null;
			Map<Object, Object> map = null;
			String busiDataItemId = null;
			map = (Map<Object, Object>) archiveslocalCache.get(PROTOCOL_Archives + protocolId);
			if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
				obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, key);
				if (null == obj) {
					throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
				}
				Map<String, String> mapvalue = new HashMap<String, String>();
				busiDataItemId = String.valueOf(obj);
				mapvalue.put(key, busiDataItemId);
				archiveslocalCache.put(PROTOCOL_Archives + protocolId, mapvalue, refreshTime * 60);
			} else {
				if(map.get(key) == null){
					obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, key);
					if (null == obj) {
						throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
					}
					busiDataItemId = String.valueOf(obj);
					map.put(key, busiDataItemId);
					archiveslocalCache.put(PROTOCOL_Archives + protocolId, map, refreshTime * 60);
				} else {
					busiDataItemId = (String) map.get(key);
				}
			}
			protocolArchivesObject.setBusiDataItemId(busiDataItemId);
			return protocolArchivesObject;
		}

	
	
	@SuppressWarnings("unchecked")
	public ProtocolArchivesObject getProtocolArchivesObjectBy645(ProtocolArchivesObject protocolArchivesObject, String dataItemId) {
		String field = null;
		int protocolId = 1;
		if (protocolArchivesObject instanceof Protocol3761ArchivesObject) {
			Protocol3761ArchivesObject protocol3861ArchivesObject = (Protocol3761ArchivesObject) protocolArchivesObject;
			String afn_str = Integer.toHexString(protocol3861ArchivesObject.getAfn());
			if(afn_str.length()==1){
				afn_str = "0" + afn_str.toUpperCase();
			}else {
				afn_str=afn_str.toUpperCase();
			}
			field = protocolId + "-" + afn_str + "-" + protocol3861ArchivesObject.getFn()+"-11"+"-"+dataItemId;
		} else {
			throw new RuntimeException("没有匹配的PROTOCOL_ID为[" + protocolId + "]的规约对象类型");
		}
		Object obj = null;
		Map<Object, Object> map = null;
		String busiDataItemId = null;
		map = (Map<Object, Object>) archiveslocalCache.get(PROTOCOL_Archives + protocolId);
		if (null == map || map.isEmpty()) {// 本地缓存没有，从远程取
			obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, field);
			if (null == obj) {
				throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
			}
			Map<String, String> mapvalue = new HashMap<String, String>();
			busiDataItemId = String.valueOf(obj);
			mapvalue.put(field, busiDataItemId);
			archiveslocalCache.put(PROTOCOL_Archives + protocolId, mapvalue, refreshTime * 60);
		} else {
			if(map.get(field) == null){
				obj = archivesremoteRedisCache.hget(PROTOCOL_Archives + protocolId, field);
				if (null == obj) {
					throw new RuntimeException("无法获取PROTOCOL_ID为[" + protocolId + "]的规约档案。。。");
				}
				busiDataItemId = String.valueOf(obj);
				map.put(field, busiDataItemId);
				archiveslocalCache.put(PROTOCOL_Archives + protocolId, map, refreshTime * 60);
			} else {
				busiDataItemId = (String) map.get(field);
			}
		}
		protocolArchivesObject.setBusiDataItemId(busiDataItemId);
		return protocolArchivesObject;
	}

	@SuppressWarnings("unchecked")
	public int getMeterPtl(String mpedId){
		int ptl=0;
		Object obj=archivesremoteRedisCache.hget("MP$" + mpedId, "PTL");
		if(obj!=null){
			ptl=Integer.parseInt(obj.toString());
		}
		return ptl;

	}
	@SuppressWarnings("unchecked")
	public String getMeterCommAddr(String mpedId){
		String commaddr =null;
		Object obj=archivesremoteRedisCache.hget("MP$" + mpedId, "ADDR");
		if(obj!=null){
			commaddr=obj.toString();
		}
		return commaddr;

	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub

	}

	public int getRefreshTime() {
		return refreshTime;
	}

	public void setRefreshTime(int refreshTime) {
		this.refreshTime = refreshTime;
	}

}
