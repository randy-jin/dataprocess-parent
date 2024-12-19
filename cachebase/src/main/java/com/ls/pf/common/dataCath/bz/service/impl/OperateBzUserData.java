package com.ls.pf.common.dataCath.bz.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ls.pf.base.api.cache.ICache;
import com.ls.pf.common.dataCatch.bz.bo.ObjectBzUserType;

/**
 *  终端测量点的用户类型
 * @author jinzhiqiang
 *
 */
public class OperateBzUserData  {
	private final String KEY_HEADER="U$";
	private final String POINT_HEADER="P";
	private final String SEPARATOR = "#";

	ICache cacheService;

	public ICache getCacheService() {
		return cacheService;
	}

	public void setCacheService(ICache cacheService) {
		this.cacheService = cacheService;
	}

	/**
	 * 保存终端测量点的用户类型信息
	 * @param value 终端测量点的用户类型对象， 为ObjectBzUserType 类型
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean saveUserInfoObj(Object value) {
		boolean opres = false;
		ObjectBzUserType bzobj = (ObjectBzUserType) value;
		String key = KEY_HEADER+bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
		cacheService.remove(key);//先删除再放
		Map pointmap=wrapMap(bzobj.getPointNum(),POINT_HEADER);
		try {
			cacheService.hmput(key, pointmap);
			opres = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return opres;
	}




	/**
	 * 批量保存测量点用户类型信息
	 * @param value
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean saveUserInfoObj(List<Object> value) {
		boolean opres = false;
		removeUserObjInfo(value);//批量删除原来所有的再放，保证统一 
		Map key_map =new HashMap();
		try {
			for (Object obj : value) {
				ObjectBzUserType bzobj = (ObjectBzUserType) obj;
				String key =KEY_HEADER+ bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
				Map pointmap=wrapMap(bzobj.getPointNum(),POINT_HEADER);
				key_map.put(key, pointmap);
			}
			cacheService.batchHPut(key_map);
			opres = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return opres;
	}

	/**
	 * 删除测量点用户类型信息
	 * @param value
	 * @return
	 */
	public boolean removeUserObjInfo(Object value) {
		boolean delres = false;
		ObjectBzUserType bzobj = (ObjectBzUserType) value;
		String key = KEY_HEADER+bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
		try {
			cacheService.remove(key);
			delres = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return delres;
	}

	/**
	 * 批量删除测量点用户类型信息
	 * @param value
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean removeUserObjInfo(List<Object> value) {
		boolean listdelres = false;
		List<String> lkey=new ArrayList(); 
		for(int i=0;i<value.size();i++){
			ObjectBzUserType bzobj = (ObjectBzUserType) value.get(i);
			String key = KEY_HEADER+bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
			lkey.add(key);
		}
		try {
			cacheService.batchDel(lkey);
			listdelres = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listdelres;
	}


	/**
	 * 根据key 获取所有测量点用户类型信息
	 * @param areaCode
	 * @param termAdd
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Map getUserObjInfo(String areaCode, String termAdd) {
		String key=KEY_HEADER+areaCode+SEPARATOR+termAdd;
		Map map=new HashMap();
		try{
			map=cacheService.hgetAll(key);
		}catch(Exception e){
			e.printStackTrace();
		}
		return map;
	}


	/**
	 * 根据key 和域获取该域的用户类型值
	 * @param areaCode
	 * @param termAdd
	 * @param field
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Object getFieldPointNum(String areaCode, String termAdd,String field) {
		Object obj=null;
		String key=KEY_HEADER+areaCode+SEPARATOR+termAdd;
		try{
			obj=cacheService.hget(key,field);

		}catch(Exception e){
			e.printStackTrace();
		}
		return obj;
	}


	/**
	 * 删除key对应集合的某个域
	 * @param areaCode
	 * @param termAdd
	 * @param field
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long removeFieldPointNum(String areaCode, String termAdd,String field) {
		long obj=0;
		String key=KEY_HEADER+areaCode+SEPARATOR+termAdd;
		try{
			obj=cacheService.hdel(key,field);
		}catch(Exception e){
			e.printStackTrace();
		}
		return obj;
	}


	@SuppressWarnings("unchecked")
	public Map wrapMap(Map mp,String header){
		Map tempmp=new HashMap();
		Iterator keyterator = mp.entrySet().iterator();
		while (keyterator.hasNext()) {
			Map.Entry entry = (Map.Entry) keyterator.next();
			String key=entry.getKey().toString();
			String endkey=header+key;
			tempmp.put(endkey,mp.get(key));
		}
		return tempmp;
	}

}
