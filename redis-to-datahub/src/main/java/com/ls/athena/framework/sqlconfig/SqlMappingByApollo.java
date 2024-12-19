package com.ls.athena.framework.sqlconfig;

import com.ctrip.framework.apollo.openapi.client.ApolloOpenApiClient;
import com.ctrip.framework.apollo.openapi.dto.OpenItemDTO;
import com.ctrip.framework.apollo.openapi.dto.OpenNamespaceDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlMappingByApollo {
	private static SqlMappingConfig sqlMappingConfig;

	private static class SingletonHolder {
		private static final SqlMappingByApollo INSTANCE = new SqlMappingByApollo();
	}

	private SqlMappingByApollo() {
	}

	public static final SqlMappingByApollo getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public static void init(String token,String portaUrl,String appid,String namespaceName) {
//		String token="e052478a3f04ef54739035bc4c63657843608912";
//		String portaUrl="http://25.32.166.26:18085";

		ApolloOpenApiClient client=ApolloOpenApiClient.newBuilder().withPortalUrl(portaUrl).withToken(token).build();
		OpenNamespaceDTO onDto=client.getNamespace(appid,"DEV","default",namespaceName);
		List<OpenItemDTO> allItems=onDto.getItems();
		for (OpenItemDTO odto:allItems) {
			try {
				JSONObject jsonObject=XML.toJSONObject(odto.getValue());


				JSONObject listSql= (JSONObject) jsonObject.get("SqlMappingConfig");

				JSONObject sqlMessage= (JSONObject) listSql.get("listSql");

				JSONArray objectList= (JSONArray) sqlMessage.get("SqlMapping");
				List<SqlMapping> sqlmList=new ArrayList<>();
				for(int m=0;m<objectList.length();m++){
					JSONObject objMap = objectList.getJSONObject(m);
					SqlMapping sqlMapp=new SqlMapping();
					if(objMap.get("businessDataitemIds")==null){
						continue;
					}
					sqlMapp.setBusinessDataitemIds(objMap.get("businessDataitemIds").toString());
					sqlMapp.setFields(objMap.get("fields").toString());
					sqlMapp.setLifeCycle(Integer.valueOf(objMap.get("lifeCycle").toString()));
					sqlMapp.setPnType(objMap.get("pnType").toString());
					sqlMapp.setProjectName(objMap.get("projectName").toString());
					sqlMapp.setShardCount(Integer.valueOf(objMap.get("shardCount").toString()));
					sqlMapp.setTopicName(objMap.get("topicName").toString());
					sqlmList.add(sqlMapp);
				}

//				String jsonData=jsonObject.getString("SqlMappingConfig");
//				JSONObject listSql= (JSONObject) JSON.parse(jsonData);

//				String listSqlMessage=listSql.getString("listSql");
//				JSONObject sqlMessage= (JSONObject) JSON.parse(listSqlMessage);

//				List<Object> objectList= (List<Object>) sqlMessage.get("SqlMapping");
//				List<SqlMapping> sqlmList=new ArrayList<>();
//				for (Object obj:objectList) {
//					Map<String,Object> objMap= (Map<String, Object>) obj;
//					SqlMapping sqlMapp=new SqlMapping();
//					if(objMap.get("businessDataitemIds")==null){
//						continue;
//					}
//					sqlMapp.setBusinessDataitemIds(objMap.get("businessDataitemIds").toString());
//					sqlMapp.setFields(objMap.get("fields").toString());
//					sqlMapp.setLifeCycle(Integer.valueOf(objMap.get("lifeCycle").toString()));
//					sqlMapp.setPnType(objMap.get("pnType").toString());
//					sqlMapp.setProjectName(objMap.get("projectName").toString());
//					sqlMapp.setShardCount(Integer.valueOf(objMap.get("shardCount").toString()));
//					sqlMapp.setTopicName(objMap.get("topicName").toString());
//					sqlmList.add(sqlMapp);
//				}
				sqlMappingConfig =new SqlMappingConfig();
				sqlMappingConfig.addSQLToMap(sqlmList);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		System.out.println("------------------------------------------------------------------------sql配置文件加载完毕。。。。。");
	}

//	public static void refreshConfig() {
//		init();
//	}

	public static void clear() {
		sqlMappingConfig = null;
	}

	public String getPnType(String businessDataitemId) {
		return sqlMappingConfig.getPnType(businessDataitemId);
	}

	public String getProjectName(String businessDataitemId) {
		return sqlMappingConfig.getProjectName(businessDataitemId);
	}

	public String getTopicName(String businessDataitemId) {
		return sqlMappingConfig.getTopicName(businessDataitemId);
	}

	public int getShardCount(String businessDataitemId){
		return sqlMappingConfig.getShardCount(businessDataitemId);
	}

	public int getLifeCycle(String businessDataitemId){
		return sqlMappingConfig.getLifeCycle(businessDataitemId);
	}

	public String getFields(String businessDataitemId){
		return sqlMappingConfig.getFields(businessDataitemId);
	}

}
