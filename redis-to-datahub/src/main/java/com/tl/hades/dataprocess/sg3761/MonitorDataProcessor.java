package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MonitorDataProcessor extends UnsettledMessageProcessor {

	private final static Logger logger = LoggerFactory.getLogger(MonitorDataProcessor.class);

	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis报文监控数据进行处理
		logger.info("===GET MONITOR DATA===");
		Object obj = context.getContent();
		if (obj instanceof String) {
			String str=obj.toString();
			
			String[] datas=str.split("_");
			
			List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
			List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
			int terminalAddr=0;
			String areaCode="";
			try {
				 terminalAddr=Integer.parseInt(datas[1]);
				 areaCode=datas[0];
			} catch (Exception e) {
			}
			TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "ORG");
			String orgNo = terminalArchivesObject.getPowerUnitNumber();
			if (null == orgNo || "".equals(orgNo)) {
				logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_ORG");
				return false;
			}
			
			List dataListFinal = new ArrayList();
			dataListFinal.add(datas[1]);
			dataListFinal.add(datas[0]);
			dataListFinal.add(datas[2]);
			dataListFinal.add(new Date());
			dataListFinal.add(datas[3]);
			dataListFinal.add(orgNo.substring(0, 5));
			dataListFinal.add(new Date());
			
			String businessDataitemId = "monitor";
			DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId);
			int index = (int) (Long.valueOf(datas[1]) % dataHubTopic.getShardCount());
			String shardId = dataHubTopic.getActiveShardList().get(index);
			DataObject dataObj = new DataObject(dataListFinal, null, dataHubTopic.topic(), shardId.toString());//给这个数据类赋值:list key classname 
			listDataObj.add(dataObj);//将组好的数据类添加到数据列表
			
			PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
			persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
			list.add(persistentObject);//这个完整报文添加到了持久化list了
			context.setContent(list);
			return false;
		}
		return true;
	}
	
}