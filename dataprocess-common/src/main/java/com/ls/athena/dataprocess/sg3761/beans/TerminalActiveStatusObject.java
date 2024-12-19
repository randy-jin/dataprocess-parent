package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;
import java.util.Date;

/**
 * 终端在线状态对象，
 * 用在数据传输。如通过Redis或JMS等需要序列化场景，也可以直接通过数据流传递
 * @author fortune
 *
 */
public class TerminalActiveStatusObject  implements Serializable, ByteDataReader, ByteDataWriter{
	private static final long serialVersionUID = -8534560212122271363L;
	
	public static final int ACTIVE = 1;  //在线
	public static final int INACTIVE = 0; //离线
	
	private String areaCode;
	private int terminalAddr;
	private Date statusTime;
	private int status;//终端状态(1表示在线，0表示离线,2重新上线)
	private String terminalCommAddr;
	private String stipulationName;
	private int gateCode;
	
	public TerminalActiveStatusObject(){
		this.areaCode = null;
		this.terminalAddr = 0;
		this.statusTime = null;
		this.terminalCommAddr = null;
		this.stipulationName = null;
		this.gateCode = 0;
	}
	
	public TerminalActiveStatusObject(String areaCode, int terminalAddr, int status, Date statusTime, 
			String terminalCommAddr, String stipulationName, int gateCode){
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
		this.status = status;
		this.statusTime = statusTime;
		this.terminalCommAddr = terminalCommAddr;
		this.stipulationName = stipulationName;
		this.gateCode = gateCode;
	}

	public String getAreaCode(){
		return this.areaCode;
	}
	
	public int getTerminalAddr(){
		return this.terminalAddr;
	}
	
	public Date getStatusTime(){
		return this.statusTime;
	}
	
	public int getStatus(){
		return this.status;
	}
	
	public String getTerminalCommAddr(){
		return this.terminalCommAddr;
	}
	
	public String getStipulationName(){
		return this.stipulationName;
	}
	
	public int getGateCode(){
		return this.gateCode;
	}
	
	public void write(ByteDataBuffer buf) throws Exception {
		buf.writeVarString(areaCode);
		buf.writeInt32(terminalAddr);
		buf.writeInt8((byte) status);
		buf.writeInt64(statusTime.getTime());
		buf.writeInt8((byte) gateCode);
		buf.writeVarString(terminalCommAddr);
		buf.writeVarString(stipulationName);
	}
	
	public void read(ByteDataBuffer buf) throws Exception {
		this.areaCode = buf.readVarString();
		this.terminalAddr = buf.readInt32();
		this.status = buf.readInt8()&0xFF;
		this.statusTime = new Date(buf.readInt64());
		this.gateCode = buf.readInt8()&0xFF;
		this.terminalCommAddr =  buf.readVarString();
		this.stipulationName = buf.readVarString();
	}
}