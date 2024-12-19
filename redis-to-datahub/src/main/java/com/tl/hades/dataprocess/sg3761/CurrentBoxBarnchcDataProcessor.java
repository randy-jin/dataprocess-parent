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
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 预抄实时
 * @author easb
 *
 */

public class CurrentBoxBarnchcDataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(CurrentBoxBarnchcDataProcessor.class);

	protected boolean doCanProcess(MessageContext arg0) throws Exception {
		return true;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean doCanFinish(MessageContext context) throws Exception {
		//对获取的redis冻结类数据进行处理
		logger.info("===GET CurrentBOX DATA===");
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

				TerminalArchivesObject terminalArchivesObject=null;
				String terminalId=null;
				//多测量点pnfn 
				for (DataItemObject data : terminalDataObject.getList()) {
					List dataList = data.getList();//本测量点的数据项xxx.xx...
					List datatoredis = new ArrayList<Object>();
					int pn = data.getPn();
					int fn = data.getFn();
					logger.info("===MAKE "+afn+"_"+fn+" DATA===");

					Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
					protocol3761ArchivesObject.setAfn(afn);
					protocol3761ArchivesObject.setFn(fn);
					String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
					String mpedIdStr=null;
					String tmnlId=null;
					
					//表箱及电表总数
					if (afn == 12 && fn == 59) {
						List dataListFinal = new ArrayList<Object>();
						terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
						if(terminalArchivesObject == null) {
							logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
							continue;
						}
						tmnlId = terminalArchivesObject.getTerminalId();
						if (null == tmnlId || "".equals(tmnlId)) {
							logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_TMNLID");
							continue;
						}
						dataListFinal.add(tmnlId);
						dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
						for(Object ob:dataList){
							dataListFinal.add(ob);
						}
						datatoredis.addAll(dataListFinal);
						dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
						//区分直抄8/预抄9
						dataListFinal.add("10");//预抄
						dataListFinal.add(new Date());//入库时间3531778244
						dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]
						
						
						Object[] refreshKey = new Object[5];
						refreshKey[0] = tmnlId;
						refreshKey[1] = tmnlId;
						refreshKey[2] = "00000000000000"; // 数据召测时间
						refreshKey[3] = businessDataitemId;
						refreshKey[4] = protocolId;

						CommonUtils.putToDataHub(businessDataitemId,tmnlId,dataListFinal,refreshKey,listDataObj);

						//表象当前数据
					}else if (afn == 12 && fn == 60) {
						terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "P" + pn);
						if(terminalArchivesObject == null) {
							logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
							continue;
						}
						tmnlId = terminalArchivesObject.getTerminalId();
						if (null == tmnlId || "".equals(tmnlId)) {
							logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_TMNLID");
							continue;
						}
						List datal=(List)dataList.get(0);
						for(int j=0;j<datal.size();j++){
							List dataListFinal = new ArrayList<Object>();
							List datas=(List)datal.get(j);
							dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
							for (int q = 0; q <datas.size(); q++) {
								if(q==2){
									dataListFinal.add(terminalAddr);
								}
								if(q==3){
									dataListFinal.add(areaCode);
								}
								if(q!=2&&q!=3&&q!=6&&q!=7&&q!=21&&q!=22){
									dataListFinal.add(datas.get(q));
								}
							}
							dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
							//区分直抄8/预抄9
							dataListFinal.add("10");//预抄
							dataListFinal.add(new Date());//入库时间3531778244
							dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]
							
							Object[] refreshKey = new Object[5];
							refreshKey[0] = tmnlId;
							refreshKey[1] = tmnlId;
							refreshKey[2] = "00000000000000"; // 数据召测时间
							refreshKey[3] = businessDataitemId;
							refreshKey[4] = protocolId;


							CommonUtils.putToDataHub(businessDataitemId,tmnlId,dataListFinal,refreshKey,listDataObj);
						}
						//表箱内电表信息
					}else if (afn == 12 && fn == 61) {
						List datal=(List)dataList.get(0);
						for(int j=0;j<datal.size();j++){
							List dataListFinal = new ArrayList<Object>();
							List datas=(List)datal.get(j);
							String maddr=(String)datas.get(1);
							try{
								terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr);
							}catch(Exception e){
								continue;
							}
							if(terminalArchivesObject == null) {
								logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
								continue;
							}
							tmnlId=terminalArchivesObject.getTerminalId();
							mpedIdStr = terminalArchivesObject.getID();
							if (null == mpedIdStr || "".equals(mpedIdStr)) {
								logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
								continue;
							}
							dataListFinal.add(mpedIdStr);
							dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
							for(int i=0;i<4;i++){
								dataListFinal.add(datas.get(i));
							}
							dataListFinal.add(terminalAddr);
							dataListFinal.add(areaCode);
							dataListFinal.add(datas.get(6));
							for(int i=9;i<datas.size()-2;i++){
								dataListFinal.add(datas.get(i));
							}
							dataListFinal.add(null);
							dataListFinal.add(null);
							dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
							//区分直抄8/预抄9
							dataListFinal.add("10");//预抄
							dataListFinal.add(new Date());//入库时间3531778244
							dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]
							
							Object[] refreshKey = new Object[5];
							refreshKey[0] = tmnlId;
							refreshKey[1] = mpedIdStr;
							refreshKey[2] = "00000000000000"; // 数据召测时间
							refreshKey[3] = businessDataitemId;
							refreshKey[4] = protocolId;

							CommonUtils.putToDataHub(businessDataitemId,tmnlId,dataListFinal,refreshKey,listDataObj);
						}
					} 


				
					//分支箱写缓存
					if(!datatoredis.isEmpty()){
						TerminalArchives.getInstance().putfzx(areaCode, terminalAddr,datatoredis);
					}
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
