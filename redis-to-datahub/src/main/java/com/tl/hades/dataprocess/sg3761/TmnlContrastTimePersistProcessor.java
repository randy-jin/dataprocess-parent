package com.tl.hades.dataprocess.sg3761;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.tl.hades.persist.CommonUtils;
import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TmnlContrastTimePersistProcessor extends UnsettledMessageProcessor {

	private final static Logger logger = LoggerFactory.getLogger(TmnlContrastTimePersistProcessor.class);
	private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
	@Override
	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===GET TERMINAL Contrast DATA===");
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
				String areaCode = terminalDataObject.getAreaCode();
				int terminalAddr = terminalDataObject.getTerminalAddr();
				List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
				List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类、
				logger.info("size..."+terminalDataObject.getList().size());
				for (DataItemObject data : terminalDataObject.getList()) {
					logger.info("===GET "+terminalDataObject.getAFN()+"_"+data.getFn()+" DATA===");
					if (data.getFn() == 2)continue;
					Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
					protocol3761ArchivesObject.setAfn(5);
					protocol3761ArchivesObject.setFn(31);
					String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
					TerminalArchivesObject terminalArchivesObject =   TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
					if(terminalArchivesObject == null) {
						logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
						continue;
					}

					String tmnlIdStr = terminalArchivesObject.getTerminalId();
					Object[] refreshKey = new Object[5];
					refreshKey[0] = tmnlIdStr;
					refreshKey[1] = tmnlIdStr;
					refreshKey[2] = "00000000000000"; // 数据召测时间
					refreshKey[3] = businessDataitemId;
					refreshKey[4] = protocolId;

					String id = idGenerator.next();
					List dataListFinal = new ArrayList<>();
					dataListFinal.add(id);
					dataListFinal.add(areaCode);
					dataListFinal.add(terminalAddr);
					Date acDate = adjustAquireTime(new Date());
					Date termDate = adjustTermTime(new Date());
					dataListFinal.add(termDate);
					dataListFinal.add(acDate);
					dataListFinal.add("01");//state
					dataListFinal.add("02");//对时
					dataListFinal.add("01");//自动对时
					dataListFinal.add("");//操作人员
					dataListFinal.add(new Date());
					dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));


					CommonUtils.putToDataHub(businessDataitemId,tmnlIdStr,dataListFinal,refreshKey,listDataObj);

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
	private Date adjustAquireTime(Date aquireTime)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(aquireTime);
		Random r = new Random();
		cal.add(13, 0 - r.nextInt(5) - 1);
		return cal.getTime();
	}

	private Date adjustTermTime(Date aquireTime) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(aquireTime);
		Random r = new Random();
		cal.add(13, 0 - r.nextInt(2) - 1);
		return cal.getTime();
	}

	private static Double getTimeGap(Date aquireTime, Date termTime)
	{
		Double doubleNum = Double.valueOf(Double.valueOf(termTime.getTime() - 
				aquireTime.getTime()).doubleValue() / 
				1000.0D / 
				60.0D);
		BigDecimal bd = new BigDecimal(doubleNum.doubleValue());
		return Double.valueOf(bd.setScale(2, 4).abs().doubleValue());
	}

}