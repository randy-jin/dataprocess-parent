package com.tl.eas.common.cache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class JavaSerial implements ObjectSerial {

	public Object decode(byte[] data) throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		in.read();// skip sign
		ObjectInputStream stream = new ObjectInputStream(in);
		Object result = stream.readObject();
		stream.close();
		in.close();
		return result;
	}

	public byte[] encode(Object value) throws Exception {
		byte[] result = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(ObjectSerial.JDK_CODE);
		ObjectOutputStream stream = new ObjectOutputStream(out);
		stream.writeObject(value);
		stream.close();
		result = out.toByteArray();
		out.close();
		return result;
	}

	public boolean isEncodedData(byte[] data) {
		return (data[0] == ObjectSerial.JDK_CODE);
	}

	public boolean isEncodedObject(Object value) {
		return Serializable.class.isInstance(value);
	}

}
