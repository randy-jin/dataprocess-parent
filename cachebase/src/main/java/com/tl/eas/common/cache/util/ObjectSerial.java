package com.tl.eas.common.cache.util;

public interface ObjectSerial {
	final static int JDK_CODE = 0;
	final static int PROTOBUF_CODE = 1;
	final static int PRIM_CODE = 2;
	final static int PROTOBUF_RESUlTSET = 3;

	public boolean isEncodedData(byte[] data);

	public boolean isEncodedObject(Object value);

	public byte[] encode(Object value) throws Exception;

	public Object decode(byte[] data) throws Exception;
}
