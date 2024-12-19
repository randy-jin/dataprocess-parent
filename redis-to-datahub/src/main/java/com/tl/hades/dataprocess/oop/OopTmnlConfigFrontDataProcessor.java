package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.OopDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * 前台终端参数召测
 * @author easb
 * oop-tmnlconfig-front-dataprocessor.xml
 */
public class OopTmnlConfigFrontDataProcessor extends UnsettledMessageProcessor {

	private final static Logger logger = LoggerFactory.getLogger(OopTmnlConfigFrontDataProcessor.class);
	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}
	@SuppressWarnings({ "rawtypes", "unchecked"})
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===GET OOP tmnl locke DATA===");
		Object obj = context.getContent();
		if (obj instanceof TerminalData) {
			TerminalData ternData=(TerminalData) obj;
			String areaCode = ternData.getAreaCode();
			String termAddr=ternData.getTerminalAddr();
			int protocolId = ternData.getCommandType();
			if(protocolId!=8){
				protocolId=9;
			}
			List<com.tl.hades.persist.PersistentObject> list = new ArrayList<com.tl.hades.persist.PersistentObject>();//持久类
			List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
			List<MeterData> meterList=	ternData.getMeterDataList();
			for (MeterData meterData :meterList) {
				List dataList =null;
				String meterAddr=null;
				String businessDataitemId=null;
				Date collDate=null;
				List<com.tl.hades.objpro.api.beans.DataObject> doList=meterData.getMeterData();
				if (doList!=null&&doList.size()>0) {
					for (com.tl.hades.objpro.api.beans.DataObject dataObject : doList) {
						if (dataObject.getDataItem().equals("60420200")) {//抄表时间
							Date d =(Date) dataObject.getData();
							collDate=d; 
							//抄表时间
						}else if (dataObject.getDataItem().equals("202A0200")) {
							//表地址
							meterAddr=String.valueOf(dataObject.getData());
						}else if(dataObject.getDataItem().equals("60410200")){
							continue;
						}else{
							businessDataitemId=dataObject.getDataItem();//数据项
							if (dataObject.getData()!=null) {
								if (dataObject.getData() instanceof List) {
									dataList=(List) dataObject.getData();
								}else {
									dataList=new ArrayList();
									dataList.add(String.valueOf(dataObject.getData()));
								}
							}
						}
					}
				}
				//40000200
				if (businessDataitemId==null||dataList==null) {
					continue;
				}
				//获取档案信息
				TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode,termAddr,null);
				String terminalId = terminalArchivesObject.getTerminalId();
				if (null == terminalId || "".equals(terminalId)) {
					logger.error("无法从缓存获取正确的档案信息real:"+areaCode+"_"+termAddr+"_COMMADDR");
					continue;
				}
				businessDataitemId="1601001";
				List<Object> dataListFinal=new ArrayList<Object>();
				boolean allNull = OopDataUtils.allNull(dataList);
				if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
					continue;
				}
				Map<Integer,Integer> protocolmap=new HashMap<Integer,Integer>();
				protocolmap.put(1,2);
				protocolmap.put(2,3);
				protocolmap.put(3,5);
				protocolmap.put(4,6);
				dataListFinal.add(BigDecimal.valueOf(Long.valueOf(terminalId)));
				dataListFinal.add(trimes(dataList.get(0)).substring(1));//电能表序号
				dataListFinal.add(trimes(dataList.get(0)).substring(1));//测量点序号
				dataListFinal.add(trimes(dataList.get(2)));//通讯速率
				dataListFinal.add(trimes(dataList.get(4)));//端口号dataList.get(4)
				dataListFinal.add(protocolmap.get(Integer.parseInt(trimes(dataList.get(3)))));//电表通讯规约
				dataListFinal.add(trimes(dataList.get(1)));//电表通讯地址
				dataListFinal.add(trimes(dataList.get(5)));//通讯密码
				dataListFinal.add(trimes(dataList.get(6)));//费率个数
				dataListFinal.add(null);//示值整数位
				dataListFinal.add(null);//示值小数位
				dataListFinal.add((trimes((String)dataList.get(11))).substring(1));//采集器通信地址
				dataListFinal.add(null);//大类号
				dataListFinal.add(null);//用户小类号
				dataListFinal.add(new Date());//插入时间
				dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));


				CommonUtils.putToDataHub(businessDataitemId,terminalId,dataListFinal,null,listDataObj);

			}
			com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
			persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
			list.add(persistentObject);//这个完整报文添加到了持久化list了
			context.setContent(list);
			return false;
		}
		return true;
	}

	public String trimes(Object obj){
		String str=(String)obj;
		return str.trim();
	}
}