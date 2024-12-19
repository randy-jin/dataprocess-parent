package com.ls.athena.framework.sqlconfig;

import com.tl.dataprocess.param.ParamConstants;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

@Data
@Getter
@Setter
public class ProtocolConfigLoad implements BundleActivator {
	public void start(BundleContext context) throws Exception {
//			SG3761SqlMapping.init();
		SqlMappingByApollo.init(apolloToken,portaUrl,appid,namespaseName);
//		ParamConstants.init();
	}

	public void stop(BundleContext context) throws Exception {
	}

	public void refresh(BundleContext context) throws Exception {
	}

	public void init() {
//			SG3761SqlMapping.init();
		SqlMappingByApollo.init(apolloToken,portaUrl,appid,namespaseName);
//		ParamConstants.init();
	}


	private String apolloToken;
	private String portaUrl;
	private String appid;
	private String namespaseName;



}
