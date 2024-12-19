package com.tl.easb.coll.base.test;

import com.tl.easb.coll.base.redis.LuaScriptUtils;
import com.tl.easb.coll.base.redis.RedisConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class JedisCommandTestBase extends JedisTestBase {

    protected Jedis jedis;

    public JedisCommandTestBase() {
        super();
    }

    @Before
    public void setUp() throws Exception {
        int clientTimeout = 1000000;
        String idleTimeout = String.valueOf(clientTimeout);
        jedis = new Jedis(RedisConfig.getProperty("redis.ip"),
                RedisConfig.getInt("redis.port"), clientTimeout);
        jedis.connect();
        jedis.configSet("timeout", idleTimeout);
    }

    @After
    public void tearDown() {
        jedis.disconnect();
    }

    protected String loadLuaScript(String scriptFile) {
        return LuaScriptUtils.loadLuaScript(scriptFile);
    }

    protected Jedis createJedis() {
        Jedis j = new Jedis(RedisConfig.getProperty("redis.ip"),
                RedisConfig.getInt("redis.port"));
        j.connect();
        j.flushAll();
        return j;
    }

    protected void assertEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertArrayEquals(expected.get(n), actual.get(n));
        }
    }

    protected void assertEquals(Set<byte[]> expected, Set<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<byte[]> e = expected.iterator();
        while (e.hasNext()) {
            byte[] next = e.next();
            boolean contained = false;
            for (byte[] element : expected) {
                if (Arrays.equals(next, element)) {
                    contained = true;
                }
            }
            if (!contained) {
                throw new ComparisonFailure("element is missing",
                        Arrays.toString(next), actual.toString());
            }
        }
    }

    protected boolean arrayContains(List<byte[]> array, byte[] expected) {
        for (byte[] a : array) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }

    protected boolean setContains(Set<byte[]> set, byte[] expected) {
        for (byte[] a : set) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }
}
