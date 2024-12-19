package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 面向对象终端事件
 * @author easb
 */
public class OopTmnlEventDataProcessor extends UnsettledMessageProcessor {

	public static final Map<String, String> codeMap = new HashMap<String,String>();
	private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();

	static
	{//设置businessDataitemId
		//全事件30110200 31000200
		codeMap.put("31000200","erc1");//终端初始化事件
		codeMap.put("31000200","erc1");//版本变更
		codeMap.put("31060200","erc14");//终端停上电
		codeMap.put("31140200","erc41");//终端停上电

	}
	private final static Logger logger = LoggerFactory.getLogger(OopTmnlEventDataProcessor.class);

	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}
	@SuppressWarnings({ "rawtypes", "unchecked"})
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===GET OOP TmnlEvent  DATA===");
		Object obj = context.getContent();
		if (obj instanceof TerminalData) {
			TerminalData ternData=(TerminalData) obj;
			String areaCode = ternData.getAreaCode();
			String termAddr=ternData.getTerminalAddr();
			String businessDataitemId=null;
			List oad=ternData.getDataList();
			if(oad.size()!=0){
				businessDataitemId=(String)oad.get(0);
			}
			if("".equals(businessDataitemId)||businessDataitemId==null){
				businessDataitemId=ternData.getOadSign();
			}
			List<com.tl.hades.persist.PersistentObject> list = new ArrayList<com.tl.hades.persist.PersistentObject>();//持久类
			List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
			List<MeterData> meterList=	ternData.getMeterDataList();
			for (MeterData meterData :meterList) {
				Map<Object,Object> map=new HashMap<Object,Object>();

				List<com.tl.hades.objpro.api.beans.DataObject> doList=meterData.getMeterData();
				if (doList!=null&&doList.size()>0) {
					if(isAllNull(doList)){
						continue;
					}
					for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
						Object objectData=dataObject.getData();
						Object objectDataItem=dataObject.getDataItem();
						map.put(objectDataItem, objectData);
						if(objectData==null||"null".equals(objectData)){
							continue;
						}
					}
					if (businessDataitemId==null) {
						continue;
					}
					//erc14事件过滤掉
//					if("31060200".equals(businessDataitemId)){
//						continue;
//					}

					//获取档案信息
					TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode,termAddr,null);
					String tmnlId=terminalArchivesObject.getTerminalId();

					if (null == tmnlId || "".equals(tmnlId)) {
						logger.error("无法从缓存获取正确的档案信息real:"+areaCode+"_"+termAddr);
						continue;
					}
					List<Object> dataListFinal=new ArrayList<Object>();
					dataListFinal=getEventDateList(businessDataitemId,map,terminalArchivesObject);
					if(dataListFinal==null){continue;}
					CommonUtils.putToDataHub(businessDataitemId,tmnlId,dataListFinal,null,listDataObj);

					if(ParamConstants.startWith.equals("41")) {
						String codeVal1 = codeMap.get(businessDataitemId);
						if ("erc14".equals(codeVal1)) {
							//拼接终端地址用于后续查找档案
							String full = areaCode + termAddr;
							String realAc1 = full.substring(3, 7);
							String realTd1 = full.substring(7, 12);
							String realAc = String.valueOf(Integer.parseInt(realAc1));
							String realTd = String.valueOf(Integer.parseInt(realTd1));

							List eventErcChecked = new ArrayList();
							String eventId14 = dataListFinal.get(0).toString();
							eventErcChecked.add(eventId14);
							eventErcChecked.add(dataListFinal.get(1));
							eventErcChecked.add(realAc);
							eventErcChecked.add(realTd);
							List endList = dataListFinal.subList(2, dataListFinal.size());
							eventErcChecked.addAll(endList);
							CommonUtils.putToDataHub("erc14_checked_source", eventId14, eventErcChecked, null, listDataObj);
						}
					}
				}
				com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
				persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
				list.add(persistentObject);//这个完整报文添加到了持久化list了
				context.setContent(list);
				return false;
			}
		}
		return true;
	}
	/**
	 * 全事件
	 *
	 * @param dataitemID
	 * @param map
	 * @param terminalArchivesObject
	 * @return
	 */
	private static List<Object> getEventDateList(String dataitemID,Map<Object,Object> map,TerminalArchivesObject terminalArchivesObject){

		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String codeVal = codeMap.get(dataitemID);
		if(codeVal==null)return null;
		List<Object> dataList=new ArrayList<Object>();
		dataList.add(idGenerator.next());
		dataList.add(terminalArchivesObject.getTerminalId());
		Date starTime=null;
		Date endTime=null;
		try{
			if(isNull(map.get("201E0200"))){
				starTime=sdf.parse(map.get("201E0200").toString());
			}
			if(isNull(map.get("20200200"))){
				endTime=sdf.parse(map.get("20200200").toString());
			}
		}catch(Exception e){
			logger.error("无法转换成时间格式");
		}

		if ("erc1".equals(codeVal)) {//终端初始化事件
			dataList.add(starTime);
			if(dataitemID.equals("31000200")){
				dataList.add("01");
				dataList.add(null);
				dataList.add(null);
			}else{
				dataList.add("10");

			}

		}else if("erc14".equals(codeVal)){
			dataList.add(starTime);
			dataList.add(endTime);
			List bt= (List) map.get("33090206");
			if (bt == null) {
				dataList.add(null);
				dataList.add(null);
			} else {
				String zt=bt.get(0).toString();
				dataList.add(zt.substring(0, 1));//事件正常标志
				dataList.add(zt.substring(1, 2));//事件有效标志
			}
			dataList.add("2");
			dataList.add("9");
			dataList.add(bt.get(0));
		}else if("erc41".equals(codeVal)){
			dataList.add(starTime);
			dataList.add(null);
			Date before=null;
			Date after=null;
			if(isNull(map.get("201E0200"))){
				try {
					before=sdf.parse(map.get("201E0200").toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			if(isNull(map.get("20200200"))){
				try {
					after=sdf.parse(map.get("20200200").toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			dataList.add(before);
			dataList.add(after);
		}
		dataList.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));
		if(!"erc1".equals(codeVal)){
			dataList.add(new Date());
		}
		dataList.add(DateUtil.format(starTime, DateUtil.defaultDatePattern_YMD));
		return dataList;
	}
	/**
	 * 判断对象不为全空
	 * @param dataObject
	 * @return
	 */
	private static boolean isAllNull(List<com.tl.hades.objpro.api.beans.DataObject> dataObject){
		for(com.tl.hades.objpro.api.beans.DataObject data:dataObject){
			if(data.getData()!=null&&!"null".equals(data.getData())){
				return false;
			}
		}
		return true;
	}
	/**
	 * 判断对象不为空
	 * @param obj
	 * @return
	 */
	private static boolean isNull(Object obj){
		if(obj==null||"null".equals(obj)){
			return false;
		}
		return true;
	}

}