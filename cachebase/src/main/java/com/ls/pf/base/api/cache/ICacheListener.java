package com.ls.pf.base.api.cache;

public abstract interface ICacheListener {
	public abstract void expireEvent(String paramString, Object paramObject);

	public abstract void addEvent(String paramString, Object paramObject);

	public abstract void removeEvent(String paramString, Object paramObject);
}
