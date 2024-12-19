package com.tl.eas.common.cache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class PrimTypeSerial implements ObjectSerial {
    final static int INT_TYPE =1;
    final static int LONG_TYPE =2;
    final static int DOUBLE_TYPE=3;
    final static int FLOAT_TYPE=4;
    
    final static int SHORT_TYPE=5;
    final static int BYTE_TYPE=6;
    final static int BYTES_TYPE=7;
    final static int BOOLEAN_TYPE=8;
    final static int STRING_TYPE=9;
    final static int NULL_TYPE=0x7f;
    
    
	private HashMap <Class<?>,Integer>  primTypeMap;
	public PrimTypeSerial(){
		primTypeMap = new HashMap <Class<?>,Integer>();
		primTypeMap.put(Integer.class, INT_TYPE);
		primTypeMap.put(Long.class, LONG_TYPE);
		primTypeMap.put(Double.class, DOUBLE_TYPE);
		primTypeMap.put(Float.class, FLOAT_TYPE);
		primTypeMap.put(Short.class,SHORT_TYPE);
		primTypeMap.put(Byte.class, BYTE_TYPE);
		primTypeMap.put(byte [].class, BYTES_TYPE);
		primTypeMap.put(Boolean.class,BOOLEAN_TYPE);
		primTypeMap.put(String.class,STRING_TYPE);
		primTypeMap.put(Void.class,NULL_TYPE);
		
	}
	public Object decode(byte[] data) throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		in.read();//skip sign
		CodedInputStream codein=CodedInputStream.newInstance(in);
		Object instance=readData(codein);
		in.close();
		return instance;
	}
	
	private static Object readData(
		      final CodedInputStream input
	) throws IOException {
		 int type=input.readInt32();
		 Object value=null;
		 
		    switch (type) {
		      case DOUBLE_TYPE : value=input.readDouble();break;
		      case FLOAT_TYPE  : value=input.readFloat();break;
		      case LONG_TYPE   : value=input.readInt64();break;
		      case INT_TYPE    : value=input.readInt32();break;
		      case SHORT_TYPE  : value=(short)input.readInt32(); break;
		      case BYTE_TYPE  : value=(byte)input.readInt32(); break;
		      case BOOLEAN_TYPE: value=input.readBool();break;
		      case STRING_TYPE: value=input.readString();break;
		      case BYTES_TYPE: value=input.readBytes().toByteArray(); break;
		      case NULL_TYPE  :value=null;  break;
		    }
		    
		   return value;
		  }
	
	private static void writeData(
		      final CodedOutputStream output,
		      int  type,
		      final Object value) throws IOException {
		    output.writeInt32NoTag(type);
		    switch (type) {
		      case DOUBLE_TYPE : output.writeDoubleNoTag  ((Double     ) value); break;
		      case FLOAT_TYPE  : output.writeFloatNoTag   ((Float      ) value); break;
		      case LONG_TYPE   : output.writeInt64NoTag   ((Long       ) value); break;
		      case SHORT_TYPE  : output.writeInt32NoTag   ((Short    ) value); break;
		      case BYTE_TYPE  : output.writeInt32NoTag   ((Byte    ) value); break;
		      case INT_TYPE    : output.writeInt32NoTag   ((Integer    ) value); break;
		      case BOOLEAN_TYPE: output.writeBoolNoTag    ((Boolean    ) value); break;
		      case STRING_TYPE: output.writeStringNoTag  ((String     ) value); break;
		      case BYTES_TYPE: output.writeBytesNoTag   (ByteString.copyFrom((byte[])value)); break;
		      case NULL_TYPE  :  break;
		    }
		  }
	

	public Integer getPrimTypeMap(Object value){
		if(value==null) 
			return NULL_TYPE;
		else 
		   return primTypeMap.get(value.getClass());
	}
	public byte[] encode(Object value) throws Exception {
		byte[] result = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(ObjectSerial.PRIM_CODE);
		CodedOutputStream codeout= CodedOutputStream.newInstance(out,1024);
		Integer type=getPrimTypeMap(value);
		writeData(codeout,type,value);
		codeout.flush();
		result = out.toByteArray();
		out.close();
		return result;
	}
	public boolean isEncodedData(byte[] data) {
		return (data[0] == ObjectSerial.PRIM_CODE);
	}

	public boolean isEncodedObject(Object value) {
		return getPrimTypeMap(value) != null;
	}

}
