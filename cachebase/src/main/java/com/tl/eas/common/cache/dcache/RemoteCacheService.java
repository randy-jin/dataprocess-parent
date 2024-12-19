package com.tl.eas.common.cache.dcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ls.pf.base.api.cache.ICache;

/**
 * 远程缓存服务
 * @author jinzhiqiang
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class RemoteCacheService implements ICache {

	private IRemoteCacheService remoteCacheService;

	public void setRemoteCacheService(IRemoteCacheService remoteCacheService) {
		this.remoteCacheService = remoteCacheService;
	}

	public void put(String key, Object value) {
		remoteCacheService.put(key, value);
	}

	public void put(String key, Object value, int TTL) {
		remoteCacheService.put(key, value, TTL);
	}

	public Object get(String key) {
		return remoteCacheService.get(key);
	}

	public boolean containsKey(String key) {
		return remoteCacheService.containsKey(key);
	}

	public Set<String> getKeysByPattern(String pattern) {
		return remoteCacheService.getKeysByPattern(pattern);
	}

	public List getList(String key) {
		return remoteCacheService.getList(key);
	}

	public long hdel(String key, String field) {
		return remoteCacheService.hdel(key, field);
	}

	public Boolean hexists(String key, String field) {
		return remoteCacheService.hexists(key, field);
	}

	public Object hget(String key, String field) {
		return remoteCacheService.hget(key, field);
	}

	public Map hgetAll(String key) {
		return remoteCacheService.hgetAll(key);
	}

	public void hput(String key, String field, Object value, int ttl) {
		remoteCacheService.hput(key, field, value, ttl);
	}

	public long incr(String key) {
		return remoteCacheService.incr(key);
	}

	public long lpush(String key, Object value) {
		return remoteCacheService.lpush(key, value);
	}

	public Object rpop(String key) {
		return remoteCacheService.rpop(key);
	}

	public long sadd(String key, Object value) {
		return remoteCacheService.sadd(key, value);
	}

	public long srem(String key, Object value) {
		return remoteCacheService.srem(key, value);
	}

	public long batchHDel(Map<String, String> kkmap) {
		Map tmpMap = new HashMap();
		for (String key : kkmap.keySet()) {
			tmpMap.put(key, kkmap.get(key));
		}

		return remoteCacheService.batchHDelete(tmpMap);
	}

	public long batchHPut(Map<String, Map> maps) {
		Map tmpMap = new HashMap();
		for (String key : maps.keySet()) {
			tmpMap.put(key, maps.get(key));
		}
		return remoteCacheService.batchHPut(tmpMap);
	}

	public long hmput(String key, Map map, int ttl) {
		return remoteCacheService.hmput(key, ttl, map);
	}

	public List<Object> hmget(String key, List<String> fields) {
		return remoteCacheService.hmget(key, fields);

	}

	public long hincr(String key, String field, long addBy) {
		return remoteCacheService.hincr(key, field, addBy);

	}

	public List<Map> batchGet(List<String> keyObjectList) {
		List<Object> klist = new ArrayList();
		for (String key : keyObjectList) {
			klist.add(key);
		}
		return remoteCacheService.batchGet(klist);
	}

	@Override
	public Object remove(String key) {
		remoteCacheService.remove(key);
		return null;
	}

	@Override
	public long hput(String key, String field, Object value) {
		remoteCacheService.hput(key, field, value);
		return 0;
	}

	@Override
	public long hmput(String key, Map map) {
		remoteCacheService.hmput(key, map);
		return 0;
	}
	
	@Override
	public List<Object> hmget(String key, String[] fields) {
		List<String> fieldsList = Arrays.asList(fields) ;
		return remoteCacheService.hmget(key, fieldsList);
	}

	@Override
	public long expire(String key, int ttl) {
		remoteCacheService.expire(key, ttl);
		return 0;
	}
	
	@Override
	public boolean hdelall(String key) {
		// TODO Auto-generated method stub
		remoteCacheService.remove(key);
		return false;
	}

	@Override
	public long smove(String key, String dkey, Object obj) {
		remoteCacheService.smove(key, dkey, obj);
		return 0;
	}
	
	@Override
	public long batchDel(List<String> bkeyList) {
		List<Object> klist = new ArrayList();
		for (String key : bkeyList) {
			klist.add(key);
		}
		remoteCacheService.batchDelete(klist);
		return 0;
	}
}