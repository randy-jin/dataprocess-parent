package com.ls.athena.dataprocess.sg3761.beans;

import com.ls.athena.framework.protocol.base.ByteDataReader;
import com.ls.athena.framework.protocol.base.ByteDataWriter;
import com.ls.pf.base.utils.log.ILogger;
import com.ls.pf.base.utils.log.LoggerHelper;
import com.ls.pf.base.utils.tools.ByteDataBuffer;

import java.io.Serializable;

/**
 * 终端升级时，主站下发解绑或则绑定网关通道命令的对象
 * @author chenfeng
 *
 */
public class UpgradeManageFepObject implements Serializable, ByteDataReader, ByteDataWriter{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3499228463855006033L;

	private final ILogger logger = LoggerHelper
			.getLogger(ManageTerminalObject.class);

	private String areaCode;
	private String terminalAddr;
	private String fepControl; /*网关控制命令，5表示绑定，6表示解绑*/
	private String preNumber; /*前置序号*/

	public UpgradeManageFepObject() {
	}

	public UpgradeManageFepObject(String areaCode, String terminalAddr,String fepControl, String preNumber) {
		this.areaCode = areaCode;
		this.terminalAddr = terminalAddr;
		this.fepControl = fepControl;
		this.preNumber = preNumber;
	}

	

	public String getPreNumber() {
		return preNumber;
	}

	public String getFepControl() {
		return fepControl;
	}

	public String getAreaCode() {
		return this.areaCode;
	}

	public String getTerminalAddr() {
		return this.terminalAddr;
	}

	public void write(ByteDataBuffer buf) throws Exception {
		try {
			buf.writeVarString(areaCode);
			buf.writeVarString(terminalAddr);
			buf.writeVarString(fepControl);
			buf.writeVarString(preNumber);
		} catch (Exception e) {
			logger.error(this.toString());
		}

	}

	public void read(ByteDataBuffer buf) throws Exception {
		try {
			this.areaCode = buf.readVarString();
			this.terminalAddr = buf.readVarString();
			this.fepControl = buf.readVarString();
			this.preNumber = buf.readVarString();
		} catch (Exception e) {
			logger.error(this.toString());
		}
	}
}
