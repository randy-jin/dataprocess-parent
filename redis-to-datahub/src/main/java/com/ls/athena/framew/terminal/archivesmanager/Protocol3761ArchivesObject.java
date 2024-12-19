package com.ls.athena.framew.terminal.archivesmanager;

/**
 * 376.1规约对象
 * @author jinzhiqiang
 *
 */
public class Protocol3761ArchivesObject extends ProtocolArchivesObject {

	private static final long serialVersionUID = 416070438390589616L;

	private int afn;
	private int fn;

	public Protocol3761ArchivesObject(int protocolId) {
		super(protocolId);
	}

	public int getAfn() {
		return afn;
	}

	public void setAfn(int afn) {
		this.afn = afn;
	}

	public int getFn() {
		return fn;
	}

	public void setFn(int fn) {
		this.fn = fn;
	}

}
