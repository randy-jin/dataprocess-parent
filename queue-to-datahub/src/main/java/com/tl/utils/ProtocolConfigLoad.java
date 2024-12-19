package com.tl.utils;

import com.tl.queueMapping.QueueMappingInit;
import com.tl.sqlMapping.SG3761SqlMapping;
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
		SG3761SqlMapping.init();
	}

	public void stop(BundleContext context) throws Exception {
	}

	public void refresh(BundleContext context) throws Exception {
	}

	public void init() {
		SG3761SqlMapping.init();
		QueueMappingInit.init();
	}

}
