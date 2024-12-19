package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FrontDataUtils {
	private final static Logger logger = Logger.getLogger(FrontDataUtils.class);
	public static List freeFront(DataItemObject data, int afn, int protocolId, int terminalAddr, String areaCode){
		if (data.getFn()!=43) {
			boolean allNull = FreezeDataUtils.allNull(data.getList());
			if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
				return null;
			}
		}
		int pn = data.getPn();
		int fn = data.getFn();

		Protocol3761ArchivesObject protocol3761ArchivesObject = new Protocol3761ArchivesObject(protocolId);
		protocol3761ArchivesObject.setAfn(afn);
		protocol3761ArchivesObject.setFn(fn);
		//**getInstance是构建对象**只是从缓存读取终端档案信息，填充此处的几个字段*/
		TerminalArchivesObject terminalArchivesObject = TerminalArchives.getInstance().getTerminalArchivesObjectLocal(areaCode, terminalAddr, "P" + pn);
		String mpedIdStr = terminalArchivesObject.getID();
		if (null == mpedIdStr || "".equals(mpedIdStr)) {
			logger.error("无法从缓存获取正确的档案信息:"+areaCode+"_"+terminalAddr+"_P"+pn);
			return null;
		}

		String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObject(protocol3761ArchivesObject).getBusiDataItemId();
		List dataListFinal =new ArrayList();
		try {
			dataListFinal = FreezeDataUtils.getDataList(data.getList(), afn, fn, mpedIdStr);//标识-----14个正向有功
		} catch (Exception e) {
			return null;
		}
		dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//供电单位编号
		dataListFinal.add("00");//未分析
		dataListFinal.add("0");//自动抄表
		dataListFinal.add(new Date());//入库时间
		dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0,5));//前闭后开  前5位 不包括[5]

		dataListFinal.add(dataListFinal.size(), dataListFinal.get(dataListFinal.size()-6));
		dataListFinal.remove(dataListFinal.size()-7);
		return dataListFinal;
	}
}
