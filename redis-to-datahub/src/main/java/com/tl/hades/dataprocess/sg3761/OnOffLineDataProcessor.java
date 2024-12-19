package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.TerminalActiveStatusObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OnOffLineDataProcessor extends UnsettledMessageProcessor {

	private final static Logger logger = LoggerFactory.getLogger(OnOffLineDataProcessor.class);

	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===GET ONOFFLINE DATA===");
		Object obj = context.getContent();
		if (obj instanceof TerminalActiveStatusObject) {
			TerminalActiveStatusObject terminalActiveStatusObject = (TerminalActiveStatusObject) obj;
			int status = terminalActiveStatusObject.getStatus();
			Date date = terminalActiveStatusObject.getStatusTime();
			int gateCode = terminalActiveStatusObject.getGateCode();
			String areaCode = terminalActiveStatusObject.getAreaCode();
			int terminalAddr = terminalActiveStatusObject.getTerminalAddr();
			logger.info("areaCode==" + areaCode + "terminalAddr==" + terminalAddr + "status==" + status);
			List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
			List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
			
			TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode,String.valueOf(terminalAddr));
			if(terminalArchivesObject==null){
				logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_ORG");
				return false;
			}
			String orgNo = terminalArchivesObject.getPowerUnitNumber();
			if (null == orgNo || "".equals(orgNo)) {
				logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_ORG");
				return false;
			}
			List online = new ArrayList();
			List offline = new ArrayList();
			String businessDataitemId = null;
			DataHubTopic onlineHubTopic = null;
			DataHubTopic offlineHubTopic = null;
			DataObject onlineObj =  null;
			DataObject offlineObj = null;
			String[] ipPort = null;
			switch (status) {
			case 1:// 终端上线，首先需要更新到online表（online表有记录就更新，没有就插入），第二步，往offline写一条上线记录（offline无主键）
//				String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
				businessDataitemId = "1000000";
				ipPort = terminalActiveStatusObject.getTerminalCommAddr().split(":");
				online.add(areaCode);
				online.add(terminalAddr);
				online.add(status);
				online.add(null);//refresh_time
				online.add(date);//login_time
				online.add(null);//heartbeat_count
				online.add(ipPort[0]);
				online.add(Integer.parseInt(ipPort[1]));
				online.add(null);//bytes_send
				online.add(null);//bytes_received
				online.add(gateCode);//computer_id
				online.add(orgNo.substring(0, 5));//shard_no
				online.add(new Date());

				CommonUtils.putToDataHub(businessDataitemId,areaCode,online, null,listDataObj);


				businessDataitemId = "1000001";
				offline.add(areaCode);
				offline.add(terminalAddr);
				offline.add(date);
				offline.add(status);
				offline.add(orgNo.substring(0, 5));//shard_no
				offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
				offline.add(new Date());

				CommonUtils.putToDataHub(businessDataitemId,areaCode,offline, null,listDataObj);

				
				break;
			case 0:// 终端离线，首先在online表里面在线状态修改为离线，第二步，往offline表里面写一条离线记录
				businessDataitemId = "1000000";
				ipPort = terminalActiveStatusObject.getTerminalCommAddr().split(":");
				online.add(areaCode);
				online.add(terminalAddr);
				online.add(status);
				online.add(date);//refresh_time
				online.add(null);//login_time
				online.add(null);//heartbeat_count
				online.add(ipPort[0]);
				online.add(Integer.parseInt(ipPort[1]));
				online.add(null);//bytes_send
				online.add(null);//bytes_received
				online.add(gateCode);//computer_id
				online.add(orgNo.substring(0, 5));//shard_no
				online.add(new Date());
//				online.add(0);
//				online.add(areaCode);
//				online.add(terminalAddr);
				CommonUtils.putToDataHub(businessDataitemId,areaCode,online, null,listDataObj);
				
				businessDataitemId = "1000001";
				offline.add(areaCode);
				offline.add(terminalAddr);
				offline.add(date);
				offline.add(status);
				offline.add(orgNo.substring(0, 5));//shard_no
				offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
				offline.add(new Date());
				CommonUtils.putToDataHub(businessDataitemId,areaCode,offline, null,listDataObj);
				
				break;
			case 2:// 重新上线，属于异常，没有正常检测到离线的情况，只需要插入一个离线记录
				
				
				businessDataitemId = "1000000";
				ipPort = terminalActiveStatusObject.getTerminalCommAddr().split(":");
				online.add(areaCode);
				online.add(terminalAddr);
				online.add(1);
				online.add(null);//refresh_time
				online.add(date);//login_time
				online.add(null);//heartbeat_count
				online.add(ipPort[0]);
				online.add(Integer.parseInt(ipPort[1]));
				online.add(null);//bytes_send
				online.add(null);//bytes_received
				online.add(gateCode);//computer_id
				online.add(orgNo.substring(0, 5));//shard_no
				online.add(new Date());
				CommonUtils.putToDataHub(businessDataitemId,areaCode,online, null,listDataObj);
				
				
				
				businessDataitemId = "1000001";
				offline.add(areaCode);
				offline.add(terminalAddr);
				offline.add(date);
				offline.add(1);
				offline.add(orgNo.substring(0, 5));//shard_no
				offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
				offline.add(new Date());
				CommonUtils.putToDataHub(businessDataitemId,areaCode,offline, null,listDataObj);
				break;
			default:
				throw new Exception("Terminal Status ERROR-areaCode=" + areaCode + "terminalAddr=" + terminalAddr + "status=" + status);
			}
			PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
			persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
			list.add(persistentObject);//这个完整报文添加到了持久化list了
			context.setContent(list);
			return false;
		}
		return true;
	}
	
}
