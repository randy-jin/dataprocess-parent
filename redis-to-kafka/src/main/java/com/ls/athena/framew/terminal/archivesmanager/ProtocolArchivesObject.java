package com.ls.athena.framew.terminal.archivesmanager;

import java.io.Serializable;

/**
 * 多规约对象父类（后续新添加的规约类型均继承此类）
 * @author jinzhiqiang
 *
 */
public class ProtocolArchivesObject implements Serializable {

	private static final long serialVersionUID = 6546852307590926938L;

	private int protocolId;
	private String busiDataItemId;

	public ProtocolArchivesObject(int protocolId) {
		this.protocolId = protocolId;
	}

	public int getProtocolId() {
		return protocolId;
	}

	public void setProtocolId(int protocolId) {
		this.protocolId = protocolId;
	}

	public String getBusiDataItemId() {
		return busiDataItemId;
	}

	public void setBusiDataItemId(String busiDataItemId) {
		this.busiDataItemId = busiDataItemId;
	}

}
