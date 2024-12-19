package com.tl.hades.dataprocess.jjt;

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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JjtFeectrlDataProcessor extends UnsettledMessageProcessor {

    Logger logger = LoggerFactory.getLogger(JjtEventDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET JJT MAXNO DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class<?> clazz = ParamConstants.classMap.get(protocolId);
            if (null == clazz) {
                throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
            }
            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
                int afn = terminalDataObject.getAFN();
                String areaCode = terminalDataObject.getAreaCode();
                int terminalAddr = terminalDataObject.getTerminalAddr();
                List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
                List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
                for (DataItemObject data : terminalDataObject.getList()) {
                    List dataList = data.getList();//本测量点的数据项xxx.xx...
                    int pn = data.getPn();
                    int fn = data.getFn();
                    logger.info("===MAKE FREEZE " + afn + "_" + fn + " DATA===");

                    Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
                    protocol3761ArchivesObject.setAfn(afn);
                    protocol3761ArchivesObject.setFn(fn);
                    //**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
                    TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(areaCode, String.valueOf(terminalAddr));
                    String terminalId = terminalArchivesObject.getTerminalId();
                    String orgNo = terminalArchivesObject.getPowerUnitNumber();
                    if (null == terminalId || "".equals(terminalId)) {
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                        continue;
                    }
                    if (orgNo == null || "".equals(orgNo)) {//8-23新增
                        logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_P" + pn);
                        continue;
                    }

                    String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
                    if (afn == 12 && fn == 402) {//210003
                    	 List dataListFinal = new ArrayList();
                        dataListFinal.add(terminalId);//8-23表变更新增字段
                        dataListFinal.add(areaCode);
                        dataListFinal.add(terminalAddr);
                        dataListFinal.add(dataList.get(0));
                        dataListFinal.add(dataList.get(1));
                        dataListFinal.add(new Date());//8-23新增字段
                        dataListFinal.add(orgNo.substring(0, 5));//8-23新增字段

                        CommonUtils.putToDataHub(businessDataitemId,terminalId,dataListFinal, null,listDataObj );

                    }else if(afn == 10 && fn == 227) {
                    	 if(dataList.size()==0) {
                    		 logger.error("无数据返回");
                             continue;
                    	 }
                    	 List endDataList=(List) dataList.get(0);
                    	 for(int d=0;d<endDataList.size();d++) {
                    		 List dList=(List) endDataList.get(d);
                    		 List dataListFinal = new ArrayList();
                    		 dataListFinal.add(terminalId);
                    		 dataListFinal.add(orgNo);
                    		 dataListFinal.add(dList.get(0));
                    		 dataListFinal.add(dList.get(1));
                    		 dataListFinal.add(dList.get(2));
                    		 dataListFinal.add(dList.get(3));
                    		 dataListFinal.add(orgNo.substring(0, 5));
                    		 dataListFinal.add(new Date());

                             CommonUtils.putToDataHub(businessDataitemId,terminalId,dataListFinal, null,listDataObj );
                    	 }
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