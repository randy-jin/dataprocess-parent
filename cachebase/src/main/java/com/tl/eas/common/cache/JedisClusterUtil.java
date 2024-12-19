package com.tl.eas.common.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisCluster;

@Component
public class JedisClusterUtil {
//	@Autowired
//	@Qualifier(value="jedisClusterArchives")
	private JedisCluster jedisCluster;

	private static Logger logger = LoggerFactory.getLogger(JedisClusterUtil.class);

	/**
	 * 批量删除对应的value
	 * 
	 * @param keys
	 */
	public void remove(final String... keys) {
		for (String key : keys) {
			remove(key);
		}
	}

	/**
	 * 批量删除key
	 * 
	 * @param pattern
	 */
	public void removePattern(final String pattern) {
		jedisCluster.del(pattern);
		logger.debug("del key >" + pattern);
	}

	/**
	 * 删除对应的value
	 * 
	 * @param key
	 */
	public void remove(final String key) {
		if (exists(key)) {
			jedisCluster.del(key);
			logger.debug("del key >" + key);
		} else {
			logger.debug("del key >" + key + "not exist");
		}
	}

	/**
	 * 判断缓存中是否有对应的value
	 * 
	 * @param key
	 * @return
	 */
	public boolean exists(final String key) {
		return jedisCluster.exists(key);
	}

	/**
	 * 读取缓存
	 * 
	 * @param key
	 * @return
	 */
	public Object get(final String key) {
		return jedisCluster.get(key);
	}

	/**
	 * 写入缓存
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean set(final String key, String value) {
		boolean result = false;
		try {
			jedisCluster.set(key, value);
			logger.debug("set key >" + key + "  value >" + value);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 写入缓存并设置缓存有效期
	 * 
	 * @param key
	 * @param value
	 * @param expireTime
	 *            单位是秒
	 * @return
	 */
	public boolean set(final String key, String value, int expireTime) {
		boolean result = false;
		try {
			jedisCluster.setex(key, expireTime, value);
			logger.debug("set key >" + key + "  value >" + value + "  expireTime>" + expireTime);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

}
