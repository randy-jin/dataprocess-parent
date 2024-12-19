package com.ls.pf.common.dataCath.bz.service;

import java.util.List;
import java.util.Map;

public interface IOperateBzData {

	public boolean loadDataItem(List value);//初始化业务数据项档案信息

	public boolean loadPCode(List value);//初始化低电压p_standard_code

	public boolean saveObj(Object value);//保存单个对象

	public boolean saveObj(List value);//操作批量保存单个对象

	public boolean saveObjSQR(List value);//操作批量保存单个对象

	public boolean saveDataCheck(List value);//数据核查档案加载

	public Map getObjectByPn(String areaCode, String termAdd);//根据返回档案信息

	public Map getObjectByPnSQR(String areaCode, String termAdd);//根据返回档案信息

	public Map getAllPointNum(String areaCode, String termAdd);//提供获取某一终端所有的测量点方法；

	public Map getAllPointNumSQR(String areaCode, String termAdd);//提供获取某一终端所有的测量点方法；

	public Map getAllPointUserType(String areaCode, String termAdd);//提供获取某一终端所有的测量点对应的用户类型；

	public Map getAlltotalNumber(String areaCode, String termAdd);//提供获取某一终端所有的总加组方法；

	public Map getAlltotalNumberSQR(String areaCode, String termAdd);//提供获取某一终端所有的总加组方法；

	public Map getAllportNumSQR(String areaCode, String termAdd);//提供获取某一终端所有的端口号方法；

	public Map getAllportNum(String areaCode, String termAdd);//提供获取某一终端所有的端口号方法；

	public Map getTB(String terminalId);//获取TB$终端缓存信息;

	public boolean removeObj(Object value);//删除对象

	public boolean removeObj(List value);//批量删除对象

	public List<Map> batchGetPointNum(List objlist);

	public List<Map> batchGetPointNumSQR(List objlist);

	public Map getAllPMped(String areaCode, String termAdd, String mpedIndex);//根据行政区划码+终端地址+测量点序号，查找对应的测量点属性

	public Map getFZX(String areaCode, String termAdd);//根据行政区划码+终端地址，查找对应的分支箱属性

	public Map getAllPMpedFromMP(String mpedId);//根据测量点业务标识(mped_id)，查找对应的测量点属性

	public String getTmnl(String tmnlId,String filed);//查询终端的相关信息 TB缓存

	public void saveHash(Map<String,Map> mapObj);//批量写入hash格式缓存
	/*
	public boolean removePn(Object value);//删除map 里的某个值

	public boolean removePn(List value);//批量删除删除map 里的某个值

	 */
	public List<Object> hmget(String key, String[] paramArrayOfString);
}
