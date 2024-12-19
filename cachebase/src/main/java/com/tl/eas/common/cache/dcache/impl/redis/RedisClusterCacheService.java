package com.tl.eas.common.cache.dcache.impl.redis;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.BoundValueOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.tl.eas.common.cache.dcache.IRemoteCacheService;
import com.tl.eas.common.cache.util.BytesCoverUtil;

/**
 * redis分布式缓存服务
 * @author jinzhiqiang
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class RedisClusterCacheService implements IRemoteCacheService {
	private static Logger logger = LoggerFactory.getLogger(RedisClusterCacheService.class);

	private RedisTemplate redisTemplate;

	public void setRedisTemplate(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	@Override
	public Object put(String key, Object value) {
		ValueOperations operation = redisTemplate.opsForValue();
		operation.set(key, value);
		return value;
	}

	@Override
	public Object put(String key, Object value, int TTL) {
		ValueOperations operation = redisTemplate.opsForValue();
		operation.set(key, value, TTL, TimeUnit.MILLISECONDS);
		return value;
	}

	@Override
	public boolean add(String key, Object value) {
		redisTemplate.execute(new RedisCallback() {
			public String doInRedis(RedisConnection connection) throws DataAccessException {
//				connection.evalSha("lua/PopulateCpAndTaskDs.mylua", keys, args);
				return "ok";
			}
		});
		ValueOperations operation = redisTemplate.opsForValue();
		operation.set(key, value);
		return false;
	}

	@Override
	public boolean add(String key, Object value, Date expiry) {
		BoundSetOperations operation = redisTemplate.boundSetOps(key);
		operation.add(key, value);
		return false;
	}

	@Override
	public Object get(String key) {
		BoundValueOperations operation = redisTemplate.boundValueOps(key);
		Object obj = operation.get();
		return obj;
	}

	@Override
	public void remove(String key) {
		redisTemplate.delete(key);
	}

	@Override
	public boolean containsKey(String key) {
		return redisTemplate.hasKey(key);
	}

	@Override
	public Set<String> getKeysByPattern(String pattern) {
		return redisTemplate.keys(pattern);
	}

	@Override
	public void hput(String key, String field, Object value) {
		HashOperations operation = redisTemplate.opsForHash();
		operation.put(key, field, value);
	}

	@Override
	public void hput(String key, String field, Object value, int TTL) {
		HashOperations operation = redisTemplate.opsForHash();
		operation.put(key, field, value);
		redisTemplate.expire(key, TTL, TimeUnit.MILLISECONDS);
	}

	@Override
	public Object hget(String key, String hashKey) {
		HashOperations operation = redisTemplate.opsForHash();
		return operation.get(key, hashKey);
	}

	@Override
	public Map<String, Object> hgetAll(String key) {
		HashOperations operation = redisTemplate.opsForHash();
		return operation.entries(key);
	}

	@Override
	public long hdel(String key, String hashKeys) {
		HashOperations operation = redisTemplate.opsForHash();
		long delres = operation.delete(key, hashKeys);
		return delres;
	}

	@Override
	public Boolean hexists(String key, String hashKey) {
		HashOperations operation = redisTemplate.opsForHash();
		return operation.hasKey(key, hashKey);
	}

	@Override
	public long lpush(String key, Object value) {
		ListOperations operation = redisTemplate.opsForList();
		return operation.leftPush(key, value);
	}

	@Override
	public Object rpop(String key) {
		BoundListOperations operation = redisTemplate.boundListOps(key);
		return operation.rightPop();
	}

	@Override
	public List<Object> getList(String key) {
		BoundListOperations operation = redisTemplate.boundListOps(key);
		List<Object> list = operation.range(0, -1);
		return list;
	}

	@Override
	public long sadd(String key, Object values) {
		BoundSetOperations operation = redisTemplate.boundSetOps(key);
		return operation.add(values);
	}

	@Override
	public long srem(String key, Object values) {
		BoundSetOperations operation = redisTemplate.boundSetOps(key);
		return operation.remove(values);
	}

	@Override
	public boolean smove(String srckey, String destKey, Object value) {
		BoundSetOperations operation = redisTemplate.boundSetOps(srckey);
		return operation.move(destKey, value);
	}

	@Override
	public long incr(String key) {
		BoundValueOperations operation = redisTemplate.boundValueOps(key);
		return operation.increment(1);
	}

	@Override
	public long hincr(String key, String field, long delta) {
		BoundHashOperations operation = redisTemplate.boundHashOps(key);
		return operation.increment(field, delta);
	}

	@Override
	public long batchHPut(Map<String, Object> hmapkeys) {
		HashOperations operation = redisTemplate.opsForHash();
		for (String bgkey : hmapkeys.keySet()) {
			Map<Object, Object> tmap = new HashMap<Object, Object>();
			Map map = (Map) hmapkeys.get(bgkey);
			Iterator keyterator = map.entrySet().iterator();
			while (keyterator.hasNext()) {
				Map.Entry entry = (Map.Entry) keyterator.next();
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (null == value || null == key) {
					logger.error("bgkey is " + bgkey + " key or value is null : " + key + "=" + value);
					continue;
				}
				tmap.put(key, value);
			}
			operation.putAll(bgkey, tmap);
		}
		return 1;
	}
	
	@Override
	public void batchDelete(List<Object> keys) {
		redisTemplate.delete(keys);
	}

	@Override
	public long batchHDelete(Map<String, Object> hmapkeys) {
		HashOperations operation = redisTemplate.opsForHash();
		long ret = 0;
		for (String key : hmapkeys.keySet()) {
			ret += operation.delete(key, hmapkeys.get(key));
		}
		return ret;
	}

	@Override
	public boolean expire(String key, int ttl) {
		return redisTemplate.expire(key, ttl, TimeUnit.MILLISECONDS);
	}

	@Override
	public long hmput(String key, int ttl, Map<String, Object> map) {
		HashOperations operation = redisTemplate.opsForHash();
		operation.putAll(key, map);
		redisTemplate.expire(key, ttl, TimeUnit.MILLISECONDS);
		return 1;
	}

	@Override
	public void hmput(String key, Map<String, Object> map) {
		HashOperations operation = redisTemplate.opsForHash();
		operation.putAll(key, map);
	}

	@Override
	public List<Object> hmget(String key, List<String> hashKeys) {
		BoundHashOperations operation = redisTemplate.boundHashOps(key);
		return operation.multiGet(hashKeys);
	}

	@Override
	public List<Map> batchGet(List<Object> keys) {
		HashOperations operation = redisTemplate.opsForHash();
		List<Map> finalList = new ArrayList();
		for (int i = 0; i < keys.size(); i++) {
			Map mp = new HashMap();
			Map<byte[], byte[]> tempmap = operation.entries(keys.get(i));
			Iterator keyterator = tempmap.entrySet().iterator();
			while (keyterator.hasNext()) {
				Map.Entry entry = (Map.Entry) keyterator.next();
				String key = BytesCoverUtil.getString(((byte[]) entry.getKey()));
				Object ob = BytesCoverUtil.coverToValue((byte[]) entry.getValue());
				mp.put(key, ob);
			}
			finalList.add(mp);
		}
		return null;
	}

}
