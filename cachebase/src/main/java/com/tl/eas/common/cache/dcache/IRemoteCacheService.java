package com.tl.eas.common.cache.dcache;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 远程缓存服务接口
 * 
 * @author jinzhiqiang
 *
 */
public interface IRemoteCacheService {

	/**
	 * 保存数值
	 * 
	 * @param key
	 * @param value
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public Object put(String key, Object value);

	/**
	 * 保存数据
	 * 
	 * @param key
	 * @param value
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @param TTL
	 * @return
	 */
	public Object put(String key, Object value, int TTL);

	/**
	 * 保存Cache数据
	 * 
	 * @param key
	 * @param value
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public boolean add(String key, Object value);

	/**
	 * 保存有有效期的数据
	 * 
	 * @param key
	 * @param value
	 * @param 有效时间（日期）
	 * @return
	 */
	public boolean add(String key, Object value, Date expiry);

	/**
	 * 获取缓存数据
	 * 
	 * @param key
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public Object get(String key);

	/**
	 * 移出缓存数据
	 * 
	 * @param key
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public void remove(String key);

	/**
	 * 是否包含了指定key的数据
	 * 
	 * @param key
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public boolean containsKey(String key);

	/**
	 * 是否包含了指定key的数据
	 * 
	 * @param key
	 * @param redisInstance
	 *            redis服务实例
	 * @param isHash
	 *            是否进行hash负载
	 * @return
	 */
	public Set<String> getKeysByPattern(String pattern);

	// ---------------------------------------------------------------------------------------------新增

	/**
	 * @param key
	 * @param field
	 * @param value
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public void hput(String key, String field, Object value);

	/**
	 * @param key
	 * @param field
	 * @param value
	 * @param TTL
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public void hput(String key, String field, Object value, int TTL);

	/**
	 * @param key
	 * @param field
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public Object hget(String key, String field);

	/**
	 * @param key
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public Map<String, Object> hgetAll(String key);

	/**
	 * @param key
	 * @param fields
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public long hdel(String key, String fields);

	/**
	 * @param key
	 * @param field
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public Boolean hexists(String key, String field);

	/**
	 * @param key
	 * @param obj
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public long lpush(String key, Object obj);

	/**
	 * @param key
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public Object rpop(String key);

	/**
	 * @param key
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public List<Object> getList(String key);

	/**
	 * @param key
	 * @param members
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public long sadd(String key, Object members);

	/**
	 * @param key
	 * @param members
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public long srem(String key, Object members);

	/**
	 * @param srckey
	 * @param dstkey
	 * @param member
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public boolean smove(String srckey, String dstkey, Object member);

	/**
	 * @param key
	 * @param redisInstance
	 * @param isHash
	 * @return
	 */
	public long incr(String key);

	public long hincr(String key, String field, long addBy);

	public long batchHPut(Map<String, Object> hmaps);

	public void batchDelete(List<Object> key);// 批量删除hash 结构key 下的map的所有值

	public long batchHDelete(Map<String, Object> mp);// 批量删除hash 结构key
														// 下的map的某个fields 的值

	public boolean expire(String key, int ttl);// 批量删除hash 结构key 下的map的所有值

	public long hmput(String key, int ttl, Map<String, Object> value);// 批量删除hash
																		// 结构key
																		// 下的map的某个fields
																		// 的值

	public void hmput(String key, Map<String, Object> value);// 批量删除hash 结构key
																// 下的map的某个fields
																// 的值

	public List<Object> hmget(String key, List<String> hashKeys);// 批量删除hash 结构key
															// 下的map的某个fields 的值

	public List<Map> batchGet(List<Object> key);// 批量获取一批key对应的hash

}
