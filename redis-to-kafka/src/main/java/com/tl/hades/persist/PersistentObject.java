package com.tl.hades.persist;

import java.util.List;

public class PersistentObject {

	private String alias;
	private List<DataObject> objs;
	private String project;
	private Object[] refreshKey;// 刷新缓存的key

	public PersistentObject(String alias, List<DataObject> objs) throws Exception {//("athena" , list<测量点>)
		this.alias = alias;//别名
		this.objs = objs;
	}

	public String getAlias() {
		return alias;
	}

	public List<DataObject> getObjs() {
		return objs;
	}

	public void setObjs(List<DataObject> objs) {
		this.objs = objs;
	}

	public Object[] getRefreshKey() {
		return refreshKey;
	}

	public void setRefreshKey(Object[] refreshKey) {
		this.refreshKey = refreshKey;
	}
	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

}
