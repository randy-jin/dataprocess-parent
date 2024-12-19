package com.tl.hades.persist;

import com.aliyun.datahub.model.PutRecordsResult;
import com.ls.athena.core.MessageContext;
import com.ls.athena.core.basic.UnsettledMessageProcessor;
import com.tl.dataprocess.refreshkey.RefreshDataCache;
import com.tl.hades.datahub.DataHubControl;

import java.util.List;

//入库
public class DataPersistProcessor extends UnsettledMessageProcessor {

	private RefreshDataCache refreshprocessor;
	private DataHubControl dataHubControl;

	public DataHubControl getDataHubControl() {
		return dataHubControl;
	}

	public void setDataHubControl(DataHubControl dataHubControl) {
		this.dataHubControl = dataHubControl;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean doCanProcess(MessageContext context) throws Exception {
		List<PersistentObject> poList = (List<PersistentObject>) context.getContent();//那边组完了，这边开始了
		if(poList != null && poList.size() > 0) {
			for(PersistentObject po : poList) {//PersistentObject对象里面 有别名和定义好的DataObject对象
				List<DataObject> doList = po.getObjs();//objs是一个数组，其实只存了一个DataObject对象【0】

				if(doList != null && doList.size() > 0) {
					//					for(DataObject doo : doList) {//取出了DataObject对象：反射类名，refreshKey6个值，21个值，topic表名 
					//						if(doo != null) {
					PutRecordsResult putRecordsResult = dataHubControl.insertion(po.getProject(), doList, refreshprocessor);//(数据库名，DataObject对象):入库
					context.setContent(putRecordsResult);//入库完了后，重新设置content的值
					//终端地址、区域码、pn、上面对象get(0)即召测时间、afn16进制、fn
					//						}
					//					}
				}
			}
		}
		return true;
	}

	public RefreshDataCache getRefreshprocessor() {
		return refreshprocessor;
	}

	public void setRefreshprocessor(RefreshDataCache refreshprocessor) {
		this.refreshprocessor = refreshprocessor;
	}

	public static final String INSERT_STATUS = "00";
}
