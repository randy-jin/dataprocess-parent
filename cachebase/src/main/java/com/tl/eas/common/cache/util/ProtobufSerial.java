package com.tl.eas.common.cache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

public class ProtobufSerial implements ObjectSerial {

	static private Class<?>[] parseFromParams = new Class[] { CodedInputStream.class };
	private HashMap<String, Method> methodCache = new HashMap<String, Method>();

	public Method getParseMethod(String className) {
		Method m = methodCache.get(className);
		if (m == null) {
			try {
				Class<?> c = Class.forName(className);
				m = c.getMethod("parseFrom", parseFromParams);
				synchronized (methodCache) {
					methodCache.put(className, m);
				}
			} catch (Exception e) {
				methodCache.put(className, null);
			}
		}
		return m;
	}

	public Object decode(byte[] data) throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		in.read();// skip sign
		CodedInputStream codein = CodedInputStream.newInstance(in);

		String className = codein.readString();
		Method m = getParseMethod(className);
		Object instance = m.invoke(null, new Object[] { codein });
		in.close();
		return instance;
	}

	public byte[] encode(Object value) throws Exception {
		byte[] result = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(ObjectSerial.PROTOBUF_CODE);
		CodedOutputStream codeout = CodedOutputStream.newInstance(out, 1024);
		codeout.writeStringNoTag(value.getClass().getName());
		((MessageLite) value).writeTo(codeout);
		codeout.flush();
		result = out.toByteArray();
		out.close();
		return result;
	}

	public boolean isEncodedData(byte[] data) {
		return (data[0] == ObjectSerial.PROTOBUF_CODE);
	}

	public boolean isEncodedObject(Object value) {
		return MessageLite.class.isInstance(value);
	}

}
