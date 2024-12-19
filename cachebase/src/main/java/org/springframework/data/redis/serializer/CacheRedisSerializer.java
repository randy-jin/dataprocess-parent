package org.springframework.data.redis.serializer;

import com.tl.eas.common.cache.util.BytesCoverUtil;

public class CacheRedisSerializer implements RedisSerializer<Object> {

	@Override
	public byte[] serialize(Object object) throws SerializationException {
		if (object == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}
		System.out.println("******************value序列化前："+object);
		System.out.println("******************value序列化后："+BytesCoverUtil.coverToData(object));
		return BytesCoverUtil.coverToData(object);
	}

	@Override
	public Object deserialize(byte[] bytes) throws SerializationException {
		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}
		System.out.println("******************value反序列化前："+bytes);
		System.out.println("******************value反序列化后："+BytesCoverUtil.coverToValue(bytes));
		return BytesCoverUtil.coverToValue(bytes);
	}

}
