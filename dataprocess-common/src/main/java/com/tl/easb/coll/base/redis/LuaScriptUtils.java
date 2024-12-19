package com.tl.easb.coll.base.redis;

import java.io.InputStream;

import com.tl.easb.coll.base.utils.FileLoader;

public class LuaScriptUtils {
    public static String loadLuaScript(String scriptFile) {
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(scriptFile);
        String command = FileLoader.load(is);
        return command;
    }
}
