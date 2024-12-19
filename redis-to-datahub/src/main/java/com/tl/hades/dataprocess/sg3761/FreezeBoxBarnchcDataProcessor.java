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
import com.tl.hades.persist.FzxUtils;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 日冻结
 * @author easb
 *
 */
public class FreezeBoxBarnchcDataProcessor extends UnsettledMessageProcessor {
    private final static Logger logger = LoggerFactory.getLogger(FreezeBoxBarnchcDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET FreezeBOX DATA===");
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
                List dataListFinal = new ArrayList<Object>();
                TerminalArchivesObject terminalArchivesObject=null;
                String mpedIdStr=null;
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
                    if (afn == 13 && fn == 139) {
                        List dataLists=(List)dataList.get(2);
                        for(int i=0;i<dataLists.size();i++){
                            List meterList=(List)dataLists.get(i);
                            String maddr=(String)meterList.get(1);
                            terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr);
                            if(terminalArchivesObject == null) {
                                logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
                                continue;
                            }
                            mpedIdStr = terminalArchivesObject.getID();
                            if (null == mpedIdStr || "".equals(mpedIdStr)) {
                                logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
                                continue;
                            }
                            dataListFinal = new ArrayList<Object>();
                            dataListFinal.add(mpedIdStr);
                            dataListFinal.add(dataList.get(1));
                            dataListFinal.add(DateUtil.format((Date)dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                            dataListFinal.add(meterList.get(1));
                            dataListFinal.add(meterList.get(0));
                            dataListFinal.add(meterList.get(2));
                            dataListFinal.add(meterList.get(3));

                            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                            dataListFinal.add("00");
                            //区分直抄8/预抄9
                            dataListFinal.add("0");//预抄
                            dataListFinal.add(new Date());//入库时间3531778244
                            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]

                        }
                    }else if (afn == 13 && fn == 140) {
                        List meterList=(List)((List)dataList.get(4)).get(0);
                        String maddr=(String)meterList.get(1);
                        terminalArchivesObject = TerminalArchives.getInstance().getTmnlAndMpedIdArchives(areaCode, String.valueOf(terminalAddr), "COMMADDR" + maddr);
                        if(terminalArchivesObject == null) {
                            logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
                            continue;
                        }
                        mpedIdStr = terminalArchivesObject.getID();
                        if (null == mpedIdStr || "".equals(mpedIdStr)) {
                            logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr);
                            continue;
                        }
                        dataListFinal.add(mpedIdStr);
                        dataListFinal.add(dataList.get(3));//数据类型
                        dataListFinal.add(FzxUtils.dataTime(String.valueOf(dataList.get(1)), (Date)dataList.get(0)));//数据时间类型
                        dataListFinal.add(DateUtil.format((Date)dataList.get(0), DateUtil.defaultDatePattern_YMD));//数据时间
                        dataListFinal.add(dataList.get(1));//数据点标志
                        dataListFinal.add(meterList.get(1));
                        dataListFinal.add(meterList.get(0));
                        dataListFinal.add(meterList.get(2));
                        dataListFinal.add(meterList.get(3));

                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
                        dataListFinal.add("00");
                        //区分直抄8/预抄9
                        dataListFinal.add("0");//预抄
                        dataListFinal.add(new Date());//入库时间3531778244
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]

                    }
                    if(dataListFinal==null){continue;}
                    CommonUtils.putToDataHub(businessDataitemId,mpedIdStr,dataListFinal,null,listDataObj);

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
