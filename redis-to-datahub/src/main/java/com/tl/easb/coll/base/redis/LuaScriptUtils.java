package com.tl.easb.coll.base.redis;


import com.tl.easb.coll.base.utils.FileLoader;

import java.io.InputStream;

public class LuaScriptUtils {
	public static String loadLuaScript(String scriptFile) {
		InputStream is = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(scriptFile);
		String command = FileLoader.load(is);
		return command;
	}
}
