package com.tl.hades.dataprocess.sg3761;

import com.ls.athena.dataprocess.sg3761.beans.TerminalActiveStatusObject;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubControl;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.datahub.DataHubTopic;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import com.zzuli.taskcdw.AutoInsert;
import com.zzuli.taskcdw.linked.ClinkedList;
import com.zzuli.taskcdw.thread.RunnableTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OnOffLineDataProcessor extends RunnableTask {

    private final static Logger logger = Logger.getLogger(OnOffLineDataProcessor.class);

    @AutoInsert
    private DataHubControl dataHubControl;


    @Override
    protected String doCanFinish(Object o) throws Exception {
        TerminalActiveStatusObject terminalActiveStatusObject = ClinkedList.getObject(o, TerminalActiveStatusObject.class);
        int status = terminalActiveStatusObject.getStatus();
        Date date = terminalActiveStatusObject.getStatusTime();
        int gateCode = terminalActiveStatusObject.getGateCode();
        String areaCode = terminalActiveStatusObject.getAreaCode();
        int terminalAddr = terminalActiveStatusObject.getTerminalAddr();
        logger.info("areaCode==" + areaCode + "terminalAddr==" + terminalAddr + "status==" + status);
        List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
        List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类

//			Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
//			protocol3761ArchivesObject.setAfn(afn);
//			protocol3761ArchivesObject.setFn(fn);
        logger.info("===MAKE ONOFFLINE DATA===");

        TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "ORG");
        String orgNo = terminalArchivesObject.getPowerUnitNumber();
        if (null == orgNo || "".equals(orgNo)) {
            logger.error("无法从缓存获取正确的档案信息:" + areaCode + "_" + terminalAddr + "_ORG");
            return null;
        }

        List online = new ArrayList();
        List offline = new ArrayList();
        String businessDataitemId = null;
        DataHubTopic onlineHubTopic = null;
        DataHubTopic offlineHubTopic = null;
        DataObject onlineObj = null;
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
                onlineHubTopic = new DataHubTopic(businessDataitemId);
                int index = Math.abs(areaCode.hashCode()) % onlineHubTopic.getShardCount();
                String onshardId = onlineHubTopic.getActiveShardList().get(index);
                onlineObj = new DataObject(online, null, onlineHubTopic.topic(), onshardId);
                listDataObj.add(onlineObj);

                businessDataitemId = "1000001";
                offline.add(areaCode);
                offline.add(terminalAddr);
                offline.add(date);
                offline.add(status);
                offline.add(orgNo.substring(0, 5));//shard_no
                offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                offline.add(new Date());
                offlineHubTopic = new DataHubTopic(businessDataitemId);
                int offindex = Math.abs(areaCode.hashCode()) % offlineHubTopic.getShardCount();
                String offshardId = offlineHubTopic.getActiveShardList().get(offindex);
                offlineObj = new DataObject(offline, null, offlineHubTopic.topic(), offshardId);
                listDataObj.add(offlineObj);

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
                onlineHubTopic = new DataHubTopic(businessDataitemId);
                int onindex0 = Math.abs(areaCode.hashCode()) % onlineHubTopic.getShardCount();
                String onshardId0 = onlineHubTopic.getActiveShardList().get(onindex0);
                onlineObj = new DataObject(online, null, onlineHubTopic.topic(), onshardId0);
                listDataObj.add(onlineObj);

                businessDataitemId = "1000001";
                offline.add(areaCode);
                offline.add(terminalAddr);
                offline.add(date);
                offline.add(status);
                offline.add(orgNo.substring(0, 5));//shard_no
                offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                offline.add(new Date());
                offlineHubTopic = new DataHubTopic(businessDataitemId);
                int offindex0 = Math.abs(areaCode.hashCode()) % offlineHubTopic.getShardCount();
                String offshardId0 = offlineHubTopic.getActiveShardList().get(offindex0);
                offlineObj = new DataObject(offline, null, offlineHubTopic.topic(), offshardId0);
                listDataObj.add(offlineObj);

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
                onlineHubTopic = new DataHubTopic(businessDataitemId);
                int index2 = Math.abs(areaCode.hashCode()) % onlineHubTopic.getShardCount();
                String onshardId2 = onlineHubTopic.getActiveShardList().get(index2);
                onlineObj = new DataObject(online, null, onlineHubTopic.topic(), onshardId2);
                listDataObj.add(onlineObj);


                businessDataitemId = "1000001";
                offline.add(areaCode);
                offline.add(terminalAddr);
                offline.add(date);
                offline.add(1);
                offline.add(orgNo.substring(0, 5));//shard_no
                offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                offline.add(new Date());
                offlineHubTopic = new DataHubTopic(businessDataitemId);
                int offindex2 = Math.abs(areaCode.hashCode()) % offlineHubTopic.getShardCount();
                String offshardId2 = offlineHubTopic.getActiveShardList().get(offindex2);
                offlineObj = new DataObject(offline, null, offlineHubTopic.topic(), offshardId2);
                listDataObj.add(offlineObj);
                break;
            default:
                throw new Exception("Terminal Status ERROR-areaCode=" + areaCode + "terminalAddr=" + terminalAddr + "status=" + status);
        }
        dataHubControl.insertion(DataHubProps.project, listDataObj, null);
        return null;
    }

    @Override
    protected void cacheException(Object o, Exception e) {

    }
}
