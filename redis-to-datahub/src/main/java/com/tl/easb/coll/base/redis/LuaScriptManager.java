package com.tl.easb.coll.base.redis;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LuaScriptManager {
	private static ConcurrentHashMap<String, String> shaScripts = new ConcurrentHashMap<String, String>();
	
	public static Object evalsha(Jedis jedis, String scriptFile,
                                 List<String> keys, List<String> args) {

		if (args == null) {
			args = new ArrayList<String>(0);
		}
		String shaCmd = getShaCmd(jedis, scriptFile);
		return jedis.evalsha(shaCmd, keys, args);
	}

	public static Object evalsha(Jedis jedis, String scriptFile,
                                 List<String> keys) {
		return evalsha(jedis, scriptFile, keys, null);
	}

	public static Object evalsha(Jedis jedis, String scriptFile, int keyCount,
                                 String... params) {
		String shaCmd = getShaCmd(jedis, scriptFile);
		return jedis.evalsha(shaCmd, keyCount, params);
	}

	public static Object evalsha(Jedis jedis, String scriptFile) {
		String shaCmd = getShaCmd(jedis, scriptFile);
		return jedis.evalsha(shaCmd);
	}

	public static void flushScripts(Jedis jedis) {
		shaScripts.clear();
		jedis.scriptFlush();
	}
	private static String getShaCmd(Jedis jedis, String luaScript) {
		addScript(jedis, luaScript);
		return shaScripts.get(luaScript);
	}

	private static void addScript(Jedis jedis, String luaScript) {
		if (!shaScripts.containsKey(luaScript)) {
			String shaCmd = loadLuaScriptBySha(jedis, luaScript);
			shaScripts.putIfAbsent(luaScript, shaCmd);
		}
	}

	private static String loadLuaScriptBySha(Jedis jedis, String scriptFile) {

		String shaCmd = jedis.scriptLoad(LuaScriptUtils.loadLuaScript(scriptFile));
		return shaCmd;
	}

	
}
