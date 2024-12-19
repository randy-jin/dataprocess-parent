package com.tl.hades.dataprocess.oop;

import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.datahub.DataHubProps;
import com.tl.hades.objpro.api.beans.TerminalData;
import com.tl.hades.persist.CommonUtils;
import com.tl.hades.persist.DataObject;
import com.tl.hades.persist.PersistentObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OopOnOffLineDataProcessor extends UnsettledMessageProcessor {

    private final static Logger logger = LoggerFactory.getLogger(OopOnOffLineDataProcessor.class);

    @Override
    protected boolean doCanProcess(MessageContext arg0) throws Exception {
        return true;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected boolean doCanFinish(MessageContext context) throws Exception {
        //对获取的redis冻结类数据进行处理
        logger.info("===GET ONOFFLINE DATA===");
        Object obj = context.getContent();
        if (obj instanceof TerminalData) {
            TerminalData ternData = (TerminalData) obj;
            String areaCode = ternData.getAreaCode();
            String termAddr = ternData.getTerminalAddr();
            //拼接终端地址用于后续查找档案
            String realAc = areaCode;
            String realTd = termAddr;
            if (ParamConstants.startWith.equals("41")) {
                String full = areaCode + termAddr;
                String realAc1 = full.substring(3, 7);
                String realTd1 = full.substring(7, 12);
                realAc = String.valueOf(Integer.parseInt(realAc1));
                realTd = String.valueOf(Integer.parseInt(realTd1));
            }
            List<PersistentObject> list = new ArrayList<PersistentObject>();//持久类
            List<DataObject> listDataObj = new ArrayList<DataObject>();//数据类
            TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTmnlArchives(realAc, realTd);
            if (terminalArchivesObject == null) {
                return false;
            }
            String orgNo = terminalArchivesObject.getPowerUnitNumber();
            String tmnlid = terminalArchivesObject.getTerminalId();
            if (null == orgNo || "".equals(orgNo)) {
                logger.error("无法从缓存获取正确的档案信息:" + realAc + "_" + realTd + "_ORG");
                return false;
            }

            List online = new ArrayList();
            List offline = new ArrayList();
            List<Object> objList = ternData.getDataList();
            if (objList.size() > 0 && objList != null) {
                String status = String.valueOf(objList.get(0));
                if (status != null) {
                    switch (Integer.parseInt(status)) {
                        case 1:// 终端上线，首先需要更新到online表（online表有记录就更新，没有就插入），第二步，往offline写一条上线记录（offline无主键）
                            online.add(realAc);
                            online.add(realTd);
                            online.add(status);
                            online.add(new Date());//refresh_time
                            online.add(objList.get(1));//login_time
                            online.add(0);//heartbeat_count
                            online.add(objList.get(2));
                            online.add(objList.get(3));
                            online.add(0);//bytes_send
                            online.add(0);//bytes_received
                            online.add(objList.get(4));//computer_id
                            online.add(orgNo.substring(0, 5));//shard_no
                            online.add(new Date());

                            CommonUtils.putToDataHub("1000000", tmnlid, online, null, listDataObj);


                            offline.add(realAc);
                            offline.add(realTd);
                            offline.add(objList.get(5));
                            offline.add(status);
                            offline.add(orgNo.substring(0, 5));//shard_no
                            offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                            offline.add(new Date());
                            CommonUtils.putToDataHub("1000001", tmnlid, offline, null, listDataObj);

                            break;
                        case 0:// 终端离线，首先在online表里面在线状态修改为离线，第二步，往offline表里面写一条离线记录
                            online.add(realAc);
                            online.add(realTd);
                            online.add(status);
                            online.add(objList.get(1));//refresh_time
                            online.add(null);//login_time
                            online.add(0);//heartbeat_count
                            online.add(objList.get(2));
                            online.add(objList.get(3));
                            online.add(0);//bytes_send
                            online.add(0);//bytes_received
                            online.add(objList.get(4));//computer_id
                            online.add(orgNo.substring(0, 5));//shard_no
                            online.add(new Date());
                            CommonUtils.putToDataHub("1000000", tmnlid, online, null, listDataObj);

                            offline.add(realAc);
                            offline.add(realTd);
                            offline.add(objList.get(5));
                            offline.add(status);
                            offline.add(orgNo.substring(0, 5));//shard_no
                            offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                            offline.add(new Date());
                            CommonUtils.putToDataHub("1000001", tmnlid, offline, null, listDataObj);

                            break;
                        case 2:// 重新上线，属于异常，没有正常检测到离线的情况，只需要插入一个离线记录

                            online.add(realAc);
                            online.add(realTd);
                            online.add(1);
                            online.add(null);//refresh_time
                            online.add(objList.get(1));//login_time
                            online.add(0);//heartbeat_count
                            online.add(objList.get(2));
                            online.add(objList.get(3));
                            online.add(0);//bytes_send
                            online.add(0);//bytes_received
                            online.add(objList.get(4));//computer_id
                            online.add(orgNo.substring(0, 5));//shard_no
                            online.add(new Date());
                            CommonUtils.putToDataHub("1000000", tmnlid, online, null, listDataObj);


                            offline.add(realAc);
                            offline.add(realTd);
                            offline.add(objList.get(5));
                            offline.add(1);
                            offline.add(orgNo.substring(0, 5));//shard_no
                            offline.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                            offline.add(new Date());
                            CommonUtils.putToDataHub("1000001", tmnlid, offline, null, listDataObj);
                            break;
                        default:
                            throw new Exception("Terminal Status ERROR-areaCode=" + realAc + "terminalAddr=" + realTd + "status=" + status);
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
