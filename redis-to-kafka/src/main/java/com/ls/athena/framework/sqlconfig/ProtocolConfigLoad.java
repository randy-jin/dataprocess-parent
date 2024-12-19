package com.ls.athena.framework.sqlconfig;

import com.tl.dataprocess.param.ParamConstants;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class ProtocolConfigLoad implements BundleActivator {

	public void start(BundleContext context) throws Exception {
		SG3761SqlMapping.init();
		ParamConstants.init();
	}

	public void stop(BundleContext context) throws Exception {
	}

	public void refresh(BundleContext context) throws Exception {
	}

	public void init() {
		SG3761SqlMapping.init();
		ParamConstants.init();
	}

}
