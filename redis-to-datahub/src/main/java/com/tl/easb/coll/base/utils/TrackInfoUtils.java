package com.tl.easb.coll.base.utils;

public class TrackInfoUtils {
	public static String getCurrentMethod(int i) {
		return Thread.currentThread().getStackTrace()[i].getMethodName();
	}
	public static String getCurrentMethod() {
		return getCurrentMethod(1);
	}
}
