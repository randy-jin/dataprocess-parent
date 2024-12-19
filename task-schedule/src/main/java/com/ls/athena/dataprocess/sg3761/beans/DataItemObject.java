package com.ls.athena.dataprocess.sg3761.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

public class DataItemObject implements Serializable, ByteDataReader, ByteDataWriter{
	private static final long serialVersionUID = -3690458579049871461L;
	private int fn; //功能码
	private int pn;  //测量点
	private boolean success = true;
	private List list = null;   //数据项列表,有具体子类创建及维护
	
	public DataItemObject(){
		this(0,0);
	}
	
	public DataItemObject(int fn, int pn){
		this.fn = fn;
		this.pn = pn;
		list = new ArrayList();
	}
	
	public DataItemObject(int fn, int pn, boolean success){
		this.fn = fn;
		this.pn = pn;
		this.success = success;
		list = new ArrayList();
	}
	
	public int getFn(){
		return this.fn;
	}
	
	public int getPn(){
		return this.pn;
	}
	
	public boolean isSuccess(){
		return success;
	}
	
	public List getList(){
		return list;
	}
	
	public void setList(List list){
		this.list = list;
	}
	
	public void addValue(Object value){
		this.list.add(value);
	}

	private void encodeList(List list, ByteDataBuffer buf) throws Exception {
		buf.writeInt32(list.size());
		for (Object o : list) {
			if (o instanceof List) {
				buf.writeInt8(TYPE_List);
				encodeList((List) o, buf);
			} else if (o instanceof String) {
				buf.writeInt8(TYPE_STRING);
				buf.writeVarString((String) o);
			} else if (o instanceof Date) {
				buf.writeInt8(TYPE_Date);
				buf.writeInt64(((Date) o).getTime());
			} else if (o instanceof Double) {
				buf.writeInt8(TYPE_Double);
				buf.writeDouble((Double) o);
			} else if (o instanceof Long) {
				buf.writeInt8(TYPE_Long);
				buf.writeInt64((Long) o);
			} else if (o instanceof Integer) {
				buf.writeInt8(TYPE_Integer);
				buf.writeInt32((Integer) o);
			}else if(o ==null){
				buf.writeInt8(TYPE_NULL);
			}else if(o instanceof byte[]){
				byte [] b=(byte[])o;
				buf.writeInt8(TYPE_BYTE);
				buf.writeInt16(b.length);
				buf.writeBytes(b);
			}else{
				throw new RuntimeException("未定义的数据类型。"+o.getClass().getSimpleName());
			}
		}
	}

	private void decodeList(List list, ByteDataBuffer b) throws Exception {
		int listsize = b.readInt32();
		for (int i = 0; i < listsize; i++) {
			byte type = b.readInt8();
			if (TYPE_List == type) {
				List subList = new ArrayList();
				decodeList(subList, b);
				list.add(subList);
			} else if (TYPE_STRING == type) {
				list.add(b.readVarString());
			} else if (TYPE_Date == type) {
				list.add(new Date(b.readInt64()));
			} else if (TYPE_Double == type) {
				list.add(b.readDouble());
			} else if (TYPE_Long == type) {
				list.add(b.readInt64());
			} else if (TYPE_Integer == type) {
				list.add(b.readInt32());
			} else if(TYPE_NULL ==type){
				list.add(null);
			} else if(TYPE_BYTE==type){
				int len=b.readInt16();
				byte []data=new byte[len];
				b.readBytes(data);
				list.add(data);
			}
			else {
				throw new RuntimeException("找不到对应的解析对象。");
			}
		}

	}
	private static final byte TYPE_STRING = 1;
	private static final byte TYPE_Date=2;
	private static final byte TYPE_Double=3;
	private static final byte TYPE_Long=4;
	private static final byte TYPE_Integer=5;
	private static final byte TYPE_List=6;
	private static final byte TYPE_NULL=7;
	private static final byte TYPE_BYTE=8;

	public void write(ByteDataBuffer buf) throws Exception {
		buf.writeInt16(fn);
		buf.writeInt16(pn);
		encodeList(list, buf);
	}

	public void read(ByteDataBuffer buf) throws Exception {
		this.fn = buf.readInt16()&0xFFFF;
		this.pn = buf.readInt16()&0xFFFF;
		decodeList(list, buf);
	}
}