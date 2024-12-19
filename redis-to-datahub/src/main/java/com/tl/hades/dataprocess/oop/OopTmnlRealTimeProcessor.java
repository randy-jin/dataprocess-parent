package com.tl.hades.dataprocess.oop;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.tl.dataprocess.param.ParamConstants;
import com.tl.hades.persist.CommonUtils;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.MeterData;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.OopDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

/**
 * 终端时钟批量
 * @author easb
 *oop-dataprocess-current-spring.xml
 */
public class OopTmnlRealTimeProcessor extends UnsettledMessageProcessor {

	private final static Logger logger = LoggerFactory.getLogger(OopTmnlRealTimeProcessor.class);

	private final static String redisName="Q_BASIC_DATA_40000200_R";

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
				String oopDataItemId=null;
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
							oopDataItemId=dataObject.getDataItem();//数据项
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
				if (oopDataItemId==null||dataList==null) {
					continue;
				}
				if(collDate==null){collDate=new Date();}

				//获取档案信息
				TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode,termAddr,null);
				
				String terminalId = terminalArchivesObject.getTerminalId();
				if (null == terminalId || "".equals(terminalId)) {
					logger.error("无法从缓存获取正确的档案信息real:"+areaCode+"_"+termAddr+"_COMMADDR");
					continue;
				}
				Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
				businessDataitemId= TerminalArchives.getInstance().getCmdForRedisOopPublic(protocol3761ArchivesObject,redisName, oopDataItemId);
				if(businessDataitemId==null){
					logger.error("无法从缓存获取正确的业务数据项ID:"+redisName+"_"+protocolId+"_"+oopDataItemId);
					continue;
				}
				Object[] refreshKey = new Object[5];
				refreshKey[0] = terminalId;
				refreshKey[1] = terminalId;
				refreshKey[2] = "00000000000000"; // 数据召测时间
				refreshKey[3] = businessDataitemId;
				refreshKey[4] = protocolId;


				List<Object> dataListFinal=new ArrayList<Object>();
				boolean allNull = OopDataUtils.allNull(dataList);
				if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
					continue;
				}


				//拼接终端地址用于后续查找档案
				String realAc=areaCode;
				String realTd=termAddr;
				if(ParamConstants.startWith.equals("41")){
					String full = areaCode + termAddr;
					String realAc1 = full.substring(3, 7);
					String realTd1 = full.substring(7, 12);
					realAc = String.valueOf(Integer.parseInt(realAc1));
					realTd = String.valueOf(Integer.parseInt(realTd1));
				}

				Date dateClock=(Date) dataList.get(0);
				Date dateCall=(Date) dataList.get(1);
				dataListFinal.add(BigDecimal.valueOf(Long.valueOf(terminalId)));
				dataListFinal.add(realAc);
				dataListFinal.add(realTd);
				dataListFinal.add(dateClock);
				dataListFinal.add(dateCall);//时间（主站）
				Long cha= Math.abs(dateClock.getTime() - dateCall.getTime());
				dataListFinal.add(new java.text.DecimalFormat("0.00").format(Double.parseDouble(cha.toString())/(1000 * 60)));//时钟超差值
				dataListFinal.add(0);//连续对时次数
				dataListFinal.add(new Date());
				dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));

				CommonUtils.putToDataHub(businessDataitemId,terminalId,dataListFinal,refreshKey,listDataObj);

			}
			com.tl.hades.persist.PersistentObject persistentObject = new com.tl.hades.persist.PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
			persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
			list.add(persistentObject);//这个完整报文添加到了持久化list了
			context.setContent(list);
			return false;
		}
		return true;
	}

}