package datahubcommon.sqlconfig;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.thoughtworks.xstream.XStream;

public class SG3761SqlMapping {
	private static SqlMappingConfig sqlMapping;

	private static class SingletonHolder {
		private static final SG3761SqlMapping INSTANCE = new SG3761SqlMapping();
	}

	private SG3761SqlMapping() {
	}

	public static final SG3761SqlMapping getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public static void init() {
		XStream x = new XStream();
		x.alias("SqlMappingConfig", SqlMappingConfig.class);
		x.alias("SqlMapping", SqlMapping.class);
		ConfigFile configFile= ConfigService.getConfigFile("sqlMapping", ConfigFileFormat.XML);
		sqlMapping = (SqlMappingConfig) x.fromXML(configFile.getContent());
//		sqlMapping = (SqlMappingConfig) x.fromXML(SG3761SqlMapping.class.getClassLoader().getResourceAsStream("sql-mapping.xml"));
		sqlMapping.fillMap();
		System.out.println("------------------------------------------------------------------------sql配置文件加载完毕。。。。。");
	}

	public static void refreshConfig() {
		init();
	}

	public static void clear() {
		sqlMapping = null;
	}

	public String getPnType(String businessDataitemId) {
		return sqlMapping.getPnType(businessDataitemId);
	}

	public String getProjectName(String businessDataitemId) {
		return sqlMapping.getProjectName(businessDataitemId);
	}

	public String getTopicName(String businessDataitemId) {
		return sqlMapping.getTopicName(businessDataitemId);
	}

	public int getShardCount(String businessDataitemId){
		return sqlMapping.getShardCount(businessDataitemId);
	}

	public int getLifeCycle(String businessDataitemId){
		return sqlMapping.getLifeCycle(businessDataitemId);
	}

	public String getFields(String businessDataitemId){
		return sqlMapping.getFields(businessDataitemId);
	}

}
