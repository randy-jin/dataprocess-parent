package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 采集召测对象，
 * 用在数据传输。如通过Redis或JMS等需要序列化场景，也可以直接通过数据流传递
 * @author fortune
 *
 */
public class AcquireMessageObject  implements Serializable, ByteDataReader, ByteDataWriter{
	private static final long serialVersionUID = -5677359808335231615L;
	
	private String areaCode;
	private String terminalAddr;
	private int pfc;
	private int sendTimeout = 0;  //发送超时时间，默认不超时
	private int responseTimeout = 0;
	private int afn;
	private String password;
	private int priority = 4;
	private String stipulationName;
	private boolean writeHbase = false;
	private boolean writeDatabase = false;
	private List<DataItemObject> dataItemList;
	
	public AcquireMessageObject(){
		this.areaCode = null;
		this.terminalAddr = null;
		this.pfc = 0;
		this.afn = 0;
		this.password = null;
		this.stipulationName = null;
		this.dataItemList = new ArrayList<DataItemObject>();
	}
	
	public AcquireMessageObject(String areaCode, String terminalAddr, int afn){
		this(areaCode, terminalAddr, afn, null);
	}
	
	public AcquireMessageObject(String areaCode, String terminalAddr, int afn, int sendTimeout, int responseTimeout){
		this(areaCode, terminalAddr, afn, sendTimeout, responseTimeout, null);
	}
	
	public AcquireMessageObject(String areaCode, String terminalAddr, int afn, String password){
		this(areaCode, terminalAddr, afn, 60, 60, password);
	}
	
	public AcquireMessageObject(String areaCode, String terminalAddr, int afn, int sendTimeout, int responseTimeout, String password){
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
		this.afn = afn;
		this.password = password;
		this.sendTimeout = sendTimeout;
		this.responseTimeout = responseTimeout;
		this.dataItemList = new ArrayList<DataItemObject>();
	}
	
	public String getAreaCode(){
		return this.areaCode;
	}
	
	public String getTerminalAddr(){
		return this.terminalAddr;
	}
	
	public int getAFN(){
		return this.afn;
	}
	
	public String getPassword(){
		return this.password;
	}
	
	public int getPriority(){
		return this.priority;
	}
	
	public void setPriority(int priority){
		this.priority = priority;
	}
	
	public int getSendTimeout(){
		return this.sendTimeout;
	}
	
	public int getResponseTimeout(){
		return this.responseTimeout;
	}
	
	public boolean isWriteDatabase(){
		return this.writeDatabase;
	}
	
	public void setWriteDatabase(boolean writeDB){
		this.writeDatabase = writeDB;
	}
	
	public boolean isWriteHbase(){
		return this.writeHbase;
	}
	
	public void setWriteHbase(boolean writeHbase){
		this.writeHbase = writeHbase;
	}
	
	public String getStipulationName(){
		return this.stipulationName;
	}
	
	public void setStipulationName(String stipulationName){
		this.stipulationName = stipulationName;
	}
	
	public int getPFC(){
		return this.pfc;
	}
	
	public void setPFC(int pfc){
		this.pfc = pfc;
	}
	
	public String getToken(){
		return areaCode+"_"+terminalAddr+"_"+pfc;
	}
	
	public List<DataItemObject> getDataItemList(){
		return dataItemList;
	}
	
	public void setDataItemList(List<DataItemObject> dataItemList){
		this.dataItemList = dataItemList;
	}
	
	public void addDataItem(DataItemObject dataItem){
		this.dataItemList.add(dataItem);
	}
	
	public void write(ByteDataBuffer buf)  {
		try{
			buf.writeVarString(areaCode);
			buf.writeVarString(terminalAddr);
			buf.writeInt8((byte)pfc);
			buf.writeInt16(sendTimeout);
			buf.writeInt16(responseTimeout);
			buf.writeInt8((byte) afn);
			buf.writeVarString(password);
			buf.writeInt8((byte)priority);
			buf.writeVarString(stipulationName);
			buf.writeInt8((byte)(writeHbase?1:0));
			buf.writeInt8((byte)(writeDatabase?1:0));
			buf.writeInt8((byte) dataItemList.size());
			for (DataItemObject data : dataItemList) {
				data.write(buf);
			}
		}catch (Exception e){
			System.out.println(this.toString());
		}

	}
	
	/**
	private List<DataItemObject> dataItemList;
	 */
	
	public void read(ByteDataBuffer buf)  {
		try{
			this.areaCode = buf.readVarString();
			this.terminalAddr = buf.readVarString();
			this.pfc = buf.readInt8()&0xFF;
			this.sendTimeout = buf.readInt16()&0xFFFF;
			this.responseTimeout = buf.readInt16()&0xFFFF;
			this.afn = buf.readInt8()&0xFF;
			this.password = buf.readVarString();
			this.priority = buf.readInt8()&0xFF;
			this.stipulationName = buf.readVarString();
			this.writeHbase = (buf.readInt8()==1)?true:false;
			this.writeDatabase = (buf.readInt8()==1)?true:false;
			int listsize = buf.readInt8()&0xFF;
			for (int i = 0; i < listsize; i++) {
				DataItemObject itemObj = new DataItemObject();
				itemObj.read(buf);
				dataItemList.add(itemObj);
			}
		}catch (Exception e){
			System.out.println(this.toString());
		}


	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("areaCode=");
		sb.append(areaCode);
		sb.append("terminalAddr");
		sb.append(terminalAddr);
		sb.append("pfc=");
		sb.append(pfc);
		return sb.toString();
	}
}
