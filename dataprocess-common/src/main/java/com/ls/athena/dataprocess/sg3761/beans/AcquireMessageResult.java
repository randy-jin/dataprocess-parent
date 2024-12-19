package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 采集召测返回结果对象， 用在数据传输。如通过Redis或JMS等需要序列化场景，也可以直接通过数据流传递
 * 
 * @author fortune
 * 
 */
public class AcquireMessageResult implements Serializable, ByteDataReader, ByteDataWriter{
	private static final long serialVersionUID = -8844361329334833949L;

	private String areaCode;
	private int terminalAddr;
	private int afn;// 应用功能码
	private int importantEvCount; // 当前重要事件数量
	private int normalEvCount;// 当前一般事件数量
	private List<DataItemObject> dataItemList;
	
	public AcquireMessageResult(){
		this.areaCode = null;
		this.terminalAddr = 0;
		this.afn = 0;
		this.importantEvCount = 0;
		this.normalEvCount = 0;
		this.dataItemList = new ArrayList<DataItemObject>();
	}

	public AcquireMessageResult(String areaCode, int terminalAddr, int afn,
			int importantEvCount, int normalEvCount) {
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
		this.afn = afn;
		this.importantEvCount = importantEvCount;
		this.normalEvCount = normalEvCount;
		dataItemList = new ArrayList<DataItemObject>();
	}

	public String getAreaCode() {
		return this.areaCode;
	}

	public int getTerminalAddr() {
		return this.terminalAddr;
	}

	public int getAFN() {
		return this.afn;
	}

	public int getImportantEvCount() {
		return this.importantEvCount;
	}

	public int getNormalEvCount() {
		return this.normalEvCount;
	}

	public List<DataItemObject> getDataItemList() {
		return dataItemList;
	}

	public void setList(List<DataItemObject> dataItemList) {
		this.dataItemList = dataItemList;
	}

	public void addDataItem(DataItemObject dataItem) {
		this.dataItemList.add(dataItem);
	}
	
	public void write(ByteDataBuffer buf) throws Exception {
		buf.writeVarString(areaCode);
		buf.writeInt32(terminalAddr);
		buf.writeInt8((byte) afn);
		buf.writeInt8((byte)importantEvCount);
		buf.writeInt8((byte)normalEvCount);
		buf.writeInt8((byte) dataItemList.size());
		for (DataItemObject data : dataItemList) {
			data.write(buf);
		}
	}
	
	public void read(ByteDataBuffer buf) throws Exception {
		this.areaCode = buf.readVarString();
		this.terminalAddr = buf.readInt32();
		this.afn = buf.readInt8()&0xFF;
		this.importantEvCount = buf.readInt8()&0xFF;
		this.normalEvCount = buf.readInt8()&0xFF;
		int listsize = buf.readInt8()&0xFF;
		for (int i = 0; i < listsize; i++) {
			DataItemObject itemObj = new DataItemObject();
			itemObj.read(buf);
			dataItemList.add(itemObj);
		}
	}
}
