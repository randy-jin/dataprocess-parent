package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.log.ILogger;
import com.ls.pf.base.utils.log.LoggerHelper;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;

public class ManageTerminalObject implements Serializable, ByteDataReader, ByteDataWriter{
	private static final long serialVersionUID = 1L;
    private final ILogger logger = LoggerHelper
            .getLogger(ManageTerminalObject.class);
	
	private String areaCode;
	private String terminalAddr;
	
	public ManageTerminalObject(){
	}
	
	public ManageTerminalObject(String areaCode, String terminalAddr){
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
	}
	
	public String getAreaCode(){
		return this.areaCode;
	}
	
	public String getTerminalAddr(){
		return this.terminalAddr;
	}
	
	public void write(ByteDataBuffer buf) throws Exception {
		try{
			buf.writeVarString(areaCode);
			buf.writeVarString(terminalAddr);
		}catch (Exception e){
			logger.error(this.toString());
		}

	}

	public void read(ByteDataBuffer buf) throws Exception {
		try{
			this.areaCode = buf.readVarString();
			this.terminalAddr = buf.readVarString();
		}catch (Exception e){
			logger.error(this.toString());
		}
	}

}
