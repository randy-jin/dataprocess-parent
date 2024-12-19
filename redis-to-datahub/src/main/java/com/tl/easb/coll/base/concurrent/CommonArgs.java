package com.tl.easb.coll.base.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CommonArgs {
	private static Map<String,Object> store = new HashMap<String, Object>();
	
	public void put(String key,Object val) {
		store.put(key,val);
	}
	
	public Object get(String key){
		return store.get(key);
	}

	 public Iterator iterKeys() {
		 return store.keySet().iterator();
	 }
	
}
