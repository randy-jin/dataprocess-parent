package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 终端参数下发状态入库
 * @author easb
 *
 */
public class EventSetDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(EventSetDataProcessor.class);

	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===EventSetDataProcessor===");
		Object obj = context.getContent();
		if (obj instanceof TmnlMessageResult) {//双轨临时注掉
			TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
			int protocolId = tmnlMessageResult.getProtocolId();
			Class<?> clazz = ParamConstants.classMap.get(protocolId);
			if(null == clazz){
				throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
			}
			if(clazz.isInstance(tmnlMessageResult)){
				TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
				int afn = terminalDataObject.getAFN();
				String areaCode = terminalDataObject.getAreaCode();
				int terminalAddr = terminalDataObject.getTerminalAddr();
				List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
				List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
				//多测量点pnfn 
				for (DataItemObject data : terminalDataObject.getList()) {
					List dataList = data.getList();//本测量点的数据项xxx.xx...

					int pn = data.getPn();
					int fn = data.getFn();
					logger.info("===MAKE "+afn+"_"+fn+" DATA===");

					Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
					protocol3761ArchivesObject.setAfn(afn);
					protocol3761ArchivesObject.setFn(fn);

					String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();

					TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
					if(terminalArchivesObject == null) {
						logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
						continue;
					}
					String terminalId = terminalArchivesObject.getTerminalId();

					String indexKey=terminalId;
					List dataListFinal = new ArrayList<>();
					if (afn == 10 && fn == 105) {//电能表数据分级归类参数
						terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
						String mpedIdStr = terminalArchivesObject.getID();
						BigDecimal mpedId = new BigDecimal(mpedIdStr);
						if (null == mpedIdStr || "".equals(mpedIdStr)) {
							logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_TMNLID");
							continue;
						}

						dataListFinal.add(mpedId);
						dataListFinal.add(terminalId);
						dataListFinal.add(null);
						dataListFinal.add(dataList.get(0));
						dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
						dataListFinal.add(new Date());
						indexKey=mpedIdStr;
					} else if (afn == 10 && fn == 106) {//电能表数据分级参数

						List<List> datadoList=(List)(dataList.get(0));
						if(datadoList.isEmpty()||datadoList==null){
							continue;
						}
						dataListFinal.add(terminalId);
						dataListFinal.add(pn);
						StringBuffer sb=new StringBuffer();
						for(List li:datadoList){
							sb.append(li.get(0)+":"+li.get(1)+",");//分级参数明细
						}
						sb.deleteCharAt(sb.length()-1);
						dataListFinal.add(sb.toString());
						dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
						dataListFinal.add(new Date());

					}else if(afn == 10 && fn == 107){//电能表数据分级周期参数
						dataListFinal.add(terminalId);
						dataListFinal.add(pn);
						dataListFinal.add(dataList.get(0));
						dataListFinal.add(dataList.get(1));
						dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
						dataListFinal.add(new Date());
					}
					if(dataListFinal==null){continue;}
					CommonUtils.putToDataHub(businessDataitemId,indexKey,dataListFinal,null,listDataObj);
				}
				PersistentObject persistentObject = new PersistentObject("athena", listDataObj);//加载好了每个数据对应的字段和类型
				persistentObject.setProject(DataHubProps.project); //set the project name. static field access..
				list.add(persistentObject);//这个完整报文添加到了持久化list了
				context.setContent(list);
				return false;
			}
		}
		return true;
	}
}