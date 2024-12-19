package com.tl.hades.dataprocess.sg3761;


import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.dcxt.protocol.inter.beans.TmnlMessageResult;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import com.tl.hades.persist.TransfarConcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author chendongwei
 * Date 2020/4/17 15:47
 * @Descrintion 透传
 */
public class TransferDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(TransferDataProcessor.class);


    @Override
    protected boolean doCanProcess(MessageContext messageContext) throws Exception {
        return true;
    }

    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET TRANSFER DATA===");
        Object obj = context.getContent();
        if (obj instanceof TmnlMessageResult) {//双轨临时注掉
            TmnlMessageResult tmnlMessageResult = (TmnlMessageResult) obj;
            int protocolId = tmnlMessageResult.getProtocolId();
            Class<?> clazz = ParamConstants.classMap.get(protocolId);
            if (null == clazz) {
                throw new RuntimeException("根据规约ID[" + protocolId + "]无法获取对应的Class Name");
            }
            if (clazz.isInstance(tmnlMessageResult)) {
                TerminalDataObject terminalDataObject = (TerminalDataObject) tmnlMessageResult;
                List<PersistentObject> list = new ArrayList<>();//持久类
                List<DataObject> listDataObj = new ArrayList<>();//数据类
                //多测量点pnfn
                for (DataItemObject data : terminalDataObject.getList()) {
                    try {
                        TransfarConcat.getDataList(terminalDataObject, protocolId, listDataObj, data, "auto");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (listDataObj.isEmpty()) {
                    return true;
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