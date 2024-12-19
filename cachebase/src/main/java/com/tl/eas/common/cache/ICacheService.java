package com.tl.eas.common.cache;

import java.util.List;
import java.util.Map;

/**
 * cache服务接口
 * @author jinzhiqiang
 *
 */
@Deprecated
public abstract interface ICacheService {
	public abstract void put(String paramString, Object paramObject);

	public abstract void put(String paramString, Object paramObject, int paramInt);

	public abstract Object get(String paramString);

	public abstract void remove(String paramString);

	public abstract boolean containsKey(String paramString);

	public abstract void hput(String paramString1, String paramString2, Object paramObject);

	public abstract long hmput(String paramString, Map paramMap, int paramInt);

	public abstract void hmput(String paramString, Map paramMap);

	public abstract List<Object> hmget(String paramString, List<String> paramArrayOfString);

	public abstract void expire(String paramString, int paramInt);

	public abstract Object hget(String paramString1, String paramString2);

	public abstract Map hgetAll(String paramString);

	public abstract long hdel(String paramString1, String paramString2);

	public abstract Boolean hexists(String paramString1, String paramString2);

	public abstract long lpush(String paramString, Object paramObject);

	public abstract Object rpop(String paramString);

	public abstract List getList(String paramString);

	public abstract long sadd(String paramString, Object paramObject);

	public abstract long srem(String paramString, Object paramObject);

	public abstract boolean smove(String paramString1, String paramString2, Object paramObject);

	public abstract long incr(String paramString);

	public abstract long hincr(String paramString1, String paramString2, long paramLong);

	public abstract long batchHPut(Map<String, Map> paramMap);

	public abstract long batchHDel(Map<String, String> paramMap);

	public abstract void batchDel(List<String> paramList);

	public abstract List<Map> batchGet(List<String> paramList);
}
