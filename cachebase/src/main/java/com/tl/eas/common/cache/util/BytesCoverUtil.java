package com.tl.eas.common.cache.util;

public class BytesCoverUtil {
	private static SerialFactory serialTools = SerialFactory.getInstance();

	static public byte[] getBytes(String s) {
		try {
			if (s != null)
				return s.getBytes("UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	static public String getString(byte[] data) {
		try {
			if (data != null)
				return new String(data, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	static public Object coverToValue(byte[] data) {
		try {
			return serialTools.decode(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	static public byte[] coverToData(Object value) {
		try {
			return serialTools.encode(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
