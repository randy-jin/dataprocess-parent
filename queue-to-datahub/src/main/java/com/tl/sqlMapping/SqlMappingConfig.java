package com.tl.sqlMapping;


import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class SqlMappingConfig {

	private  List<SqlMapping> listSql;
	private Map<String, Object> afnFnSQLMap;

	public SqlMappingConfig() {
	}

	protected String getPnType(String businessDataitemId) {
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getPnType();
	}

	protected String getProjectName(String businessDataitemId) {
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getProjectName();
	}

	protected String getTopicName(String businessDataitemId) {
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getTopicName();
	}

	protected int getShardCount(String businessDataitemId){
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getShardCount();
	}

	protected int getLifeCycle(String businessDataitemId){
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getLifeCycle();
	}

	protected String getFields(String businessDataitemId){
		SqlMapping mapping = (SqlMapping) afnFnSQLMap.get(businessDataitemId);
		return mapping.getFields();
	}

	protected void fillMap() {
		afnFnSQLMap = new HashMap<String, Object>();
		addSQLToMap(listSql);
	}

	protected void addSQLToMap(List<SqlMapping> listSql) {
		if (listSql == null || listSql.isEmpty()) {
			return;
		}
		afnFnSQLMap = new HashMap<String, Object>();
		for (int i = 0; i < listSql.size(); i++) {
			SqlMapping mapping = (SqlMapping) listSql.get(i);
			for(String _businessDataitemId:mapping.getBusinessDataitemIdArray()){
				afnFnSQLMap.put(_businessDataitemId.trim(), mapping);
			}
		}
	}

}
