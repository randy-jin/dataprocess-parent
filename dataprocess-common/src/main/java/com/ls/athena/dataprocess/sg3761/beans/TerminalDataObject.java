package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * 终端数据对象：实时数据、曲线数据、日冻结数据、月冻结数据、小时冻结数据、事件等。
 * 用在数据传输。如通过Redis或JMS等需要序列化场景，也可以直接通过数据流传递
 * 
 * @author fortune
 * 
 */
public class TerminalDataObject implements Serializable, ByteDataReader, ByteDataWriter {
	private static final long serialVersionUID = 3418049200601524227L;

	private String areaCode;
	private int terminalAddr;
	private int pfc;
	private int afn;
	private int importantEvCount; // 当前重要事件数量
	private int normalEvCount;// 当前一般事件数量
	private Date upTime;
	private boolean auto;// 主动上报
	private List<DataItemObject> list;
	private boolean writeHBase;
	private volatile boolean isFinish = true;
	private volatile int seq = -1;

	public TerminalDataObject(){
		list = new ArrayList<DataItemObject>();
	}
	public TerminalDataObject(String areaCode, int terminalAddr, int pfc,
			int afn, int importantEvCount, int normalEvCount, Date upTime,
			boolean auto) {
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
		this.pfc = pfc;
		this.afn = afn;
		this.importantEvCount = importantEvCount;
		this.normalEvCount = normalEvCount;
		this.upTime = upTime;
		this.auto = auto;
		list = new ArrayList<DataItemObject>();
	}

	public int getSeq(){
		return this.seq;
	}
	
	public void setSeq(int seq){
		this.seq = seq;
	}
	
	public boolean isFinish(){
		return this.isFinish;
	}
	
	public void setFinish(boolean isFin){
		this.isFinish = isFin;
	}

	public String getAreaCode() {
		return this.areaCode;
	}

	public int getTerminalAddr() {
		return this.terminalAddr;
	}

	public int getPfc() {
		return this.pfc;
	}

	public int getAFN() {
		return this.afn;
	}
	public boolean isWriteHBase() {
		return writeHBase;
	}
	public void setWriteHBase(boolean writeHBase) {
		this.writeHBase = writeHBase;
	}
	public String getToken(){
		return areaCode+"_"+terminalAddr+"_"+pfc;
	}
	
	public int getImportantEvCount() {
		return this.importantEvCount;
	}

	public int getNormalEvCount() {
		return this.normalEvCount;
	}

	public Date getUpTime() {
		return this.upTime;
	}

	public boolean isAuto() {
		return this.auto;
	}

	public List<DataItemObject> getList() {
		return list;
	}

	public void setList(List<DataItemObject> fnList) {
		this.list = fnList;
	}

	public void addList(DataItemObject fnParams) {
		this.list.add(fnParams);
	}

	@Override
	public TerminalDataObject clone() {
		TerminalDataObject terminalDataObject = new TerminalDataObject(
				this.areaCode, this.terminalAddr, this.pfc, this.afn,
				this.importantEvCount, this.normalEvCount, this.upTime,
				this.auto);
		return terminalDataObject;
	}

	public void write(ByteDataBuffer buf) throws Exception {
		buf.writeVarString(areaCode);
		buf.writeInt32(terminalAddr);
		buf.writeInt8((byte) pfc);
		buf.writeInt8((byte) afn);
		buf.writeInt16(importantEvCount);
		buf.writeInt16(normalEvCount);
		buf.writeInt8((byte)(auto?1:0));
		buf.writeInt8((byte)(writeHBase?1:0));
		buf.writeInt64(upTime.getTime());
		buf.writeInt8((byte) list.size());
		for (DataItemObject data : list) {
			data.write(buf);
		}
	}

	public void read(ByteDataBuffer buf) throws Exception {
		this.areaCode = buf.readVarString();
		this.terminalAddr = buf.readInt32();
		this.pfc = buf.readInt8()&0xFF;
		this.afn = buf.readInt8()&0xFF;
		this.importantEvCount = buf.readInt16()&0xFFFF;
		this.normalEvCount =  buf.readInt16()&0xFFFF;
		this.auto = (buf.readInt8()==1)?true:false;
		this.writeHBase = (buf.readInt8()==1)?true:false;
		this.upTime = new Date(buf.readInt64());
		int listsize = buf.readInt8()&0xFF;
		for (int i = 0; i < listsize; i++) {
			DataItemObject itemObj = new DataItemObject();
			itemObj.read(buf);
			list.add(itemObj);
		}
	}
}