package com.ls.pf.base.api.cache;

import java.util.List;
import java.util.Map;

public abstract interface ICache {
	public abstract void put(String paramString, Object paramObject);

	public abstract void put(String paramString, Object paramObject, int paramInt);

	public abstract Object get(String paramString);

	public abstract Object remove(String paramString);

	public abstract boolean containsKey(String paramString);

	public abstract long hput(String paramString1, String paramString2, Object paramObject);

	public abstract long hmput(String paramString, Map paramMap, int paramInt);

	public abstract long hmput(String paramString, Map paramMap);

	public abstract List<Object> hmget(String paramString, String[] paramArrayOfString);

	public abstract long expire(String paramString, int paramInt);

	public abstract Object hget(String paramString1, String paramString2);

	public abstract Map hgetAll(String paramString);

	public abstract long hdel(String paramString1, String paramString2);

	public abstract boolean hdelall(String paramString);

	public abstract Boolean hexists(String paramString1, String paramString2);

	public abstract long lpush(String paramString, Object paramObject);

	public abstract Object rpop(String paramString);

	public abstract List getList(String paramString);

	public abstract long sadd(String paramString, Object paramObject);

	public abstract long srem(String paramString, Object paramObject);

	public abstract long smove(String paramString1, String paramString2, Object paramObject);

	public abstract long incr(String paramString);

	public abstract long hincr(String paramString1, String paramString2, long paramLong);

	public abstract long batchHPut(Map<String, Map> paramMap);

	public abstract long batchHDel(Map<String, String> paramMap);

	public abstract long batchDel(List<String> paramList);

	public abstract List<Map> batchGet(List<String> paramList);
}
