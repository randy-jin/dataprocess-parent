package com.tl.eas.common.cache.util;

import java.io.IOException;
import java.util.ArrayList;

public class SerialFactory {

	static private SerialFactory serialFactory;
	private ArrayList<ObjectSerial> SerialFactorys = new ArrayList<ObjectSerial>();

	private SerialFactory() {
		SerialFactorys.add(new PrimTypeSerial());
		SerialFactorys.add(new ProtobufSerial());
		SerialFactorys.add(new JavaSerial());
	}

	static public SerialFactory getInstance() {
		if (serialFactory == null) {
			synchronized (SerialFactory.class) {
				if (serialFactory == null) {
					serialFactory = new SerialFactory();

				}
			}
		}
		return serialFactory;
	}

	public byte[] encode(Object value) throws Exception {
		for (ObjectSerial s : SerialFactorys) {
			if (s.isEncodedObject(value)) {
				return s.encode(value);
			}
		}
		throw new IOException("Can not find Serial type!");
	}

	public Object decode(byte[] data) throws Exception {
		if (data == null || data.length == 0)
			return null;

		return SerialFactorys.get(2).decode(data);
//		for (ObjectSerial s : SerialFactorys) {
//			if (s.isEncodedData(data)) {
//				return s.decode(data);
//			}
//		}
//		throw new IOException("Can not find Serial type!");
	}
}
