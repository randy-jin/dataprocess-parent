package com.tl.dataprocess.param;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParamConstants {
	private static Logger logger = LoggerFactory.getLogger(ParamConstants.class);
	private String orgStartWith;//地域区分

	public String getOrgStartWith() {
		return orgStartWith;
	}

	public void setOrgStartWith(String orgStartWith) {
		this.orgStartWith = orgStartWith;
	}

	public static String startWith = null;
	public  void init() {
		startWith=orgStartWith;
	}

}
