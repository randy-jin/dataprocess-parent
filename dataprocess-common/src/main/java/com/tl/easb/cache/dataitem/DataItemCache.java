package com.tl.easb.cache.dataitem;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import com.tl.util.StringUtil;

import com.ls.pf.base.common.persistence.utils.DBUtils;
import com.tl.easb.system.listener.AbstractSystemListener;
import com.tl.easb.utils.SpringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisSentinelPool;

/**
 * 数据项缓存操作类
 *
 * @author JinZhiQiang
 * @date 2014年3月11日
 */
public class DataItemCache extends AbstractSystemListener {
    static Logger log = LoggerFactory.getLogger(DataItemCache.class);

    private static Map<String, DataItemView> dataItemsCache = null;
    private static Map<String, List<String>> dataItemsCacheByAutotask = null;

    @Override
    public void onInit(ServletContextEvent event) {
        initDataItems();
        //启动jedis连接池监控线程
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                monitorRedisSentinelPool();
//            }
//        }).start();
//        log.info("启动jedis连接池监控线程成功！");
    }

    @Override
    public void onDestory(ServletContextEvent event) {

    }

    private void monitorRedisSentinelPool() {
        int sleepTime = 10000;
        while (true) {
            JedisSentinelPool jedisSentinelPool = (JedisSentinelPool) SpringUtils.getBean("jedisSentinelPool");
            log.info("JedisPool activeNum:" + jedisSentinelPool.getNumActive() + ",idleNum:"
                    + jedisSentinelPool.getNumIdle() + ",waiterNum:" + jedisSentinelPool.getNumWaiters()
                    + ",MeanBorrowWaitTime:" + jedisSentinelPool.getMeanBorrowWaitTimeMillis()
                    + ",MaxBorrowWaitTime:" + jedisSentinelPool.getMaxBorrowWaitTimeMillis() + ",MaxBorrowWaitTime:"
                    + jedisSentinelPool.getMaxBorrowWaitTimeMillis());
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                log.error("",e);
            }
        }
    }

    /**
     * 用自动任务编号从表【R_AUTOTASK_ITEMS】中获取数据项
     *
     * @param autotaskId
     * @return
     * @throws Exception
     */
    public synchronized static Map<String, List<String>> getDataitemIdByAutotaskId(String autotaskId) throws Exception {
        if (null == dataItemsCacheByAutotask || dataItemsCacheByAutotask.size() == 0 || null == dataItemsCacheByAutotask.get(autotaskId)) {
            //通过任务id查询R_AUTOTASK_ITEMS表来获取此任务所需要的数据项标识
            ResultSet rs = DBUtils.executeQuery(GET_DATAITEMID_BY_TASKID, new String[]{autotaskId});
            dataItemsCacheByAutotask = new HashMap<String, List<String>>();
            List<String> list = new ArrayList<String>();
            try {
                while (rs.next()) {
                    list.add(rs.getString("BUSINESS_DATAITEM_ID"));
                }
                dataItemsCacheByAutotask.put(autotaskId, list);
            } catch (SQLException e) {
                log.error("",e);
            }
            if (dataItemsCacheByAutotask.size() == 0) {  //如果没有数据项标识，则返回空
                throw new Exception("任务【" + autotaskId + "】从r_autotask_items表中获取对应数据项为空");
            }
        }
        return dataItemsCacheByAutotask;
    }

    /**
     * 从内存中清除该任务对应的数据项列表
     *
     * @param autotaskId
     */
    public synchronized static void removeDataitemIdbyAutotaskId(String autotaskId) {
        if (null != dataItemsCacheByAutotask) {
            dataItemsCacheByAutotask.remove(autotaskId);
        }
    }

    /**
     * 初始化
     *
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private synchronized static void initDataItems() {
        try {
            ResultSet rs = DBUtils.executeQuery(FIND_ITEM_SQL, null);
            List<Map> list = DBUtils.resultSetToListMap(rs, false);
            dataItemsCache = translate(list);
        } catch (Exception e) {
            log.error("初始化数据项缓存失败，@DataItemCache.initDataItems", e);
        }
    }

    /**
     * 转换
     *
     * @param qryRlt
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static Map<String, DataItemView> translate(List<Map> qryRlt) {
        if (qryRlt == null || qryRlt.size() == 0)
            return null;
        Map<String, DataItemView> rlt = new HashMap<String, DataItemView>();
        for (Map map : qryRlt) {
        	if("".equals(map.get("CMD"))) {continue;}
 
            String cmdArr[] = StringUtil.parseString(map.get("CMD")).split("\\|");//最新设计的cmd格式为3-11-0001FF00|1-10-1-11-0001FF00|9-10200-10200/XXXX
           
            for (String cmd : cmdArr) {
                DataItemView v = new DataItemView();
                v.setBusinessDataitemId(StringUtil.parseString(map.get("ID")));
                v.setDataItemId(StringUtil.parseString(map.get("DATAITEM_ID")));
                v.setPid(StringUtil.parseString(map.get("P_ID")));
                v.setName(StringUtil.parseString(map.get("NAME")));
                v.setObjFlag(StringUtil.parseString(map.get("OBJ_FLAG")));
                v.setDataFlag(StringUtil.parseString(map.get("DATA_FLAG")));
                String AFN_FN = parseAFN_FN(cmd);
                v.setAFN_FN(AFN_FN);
                String protocolId = cmd.split("-")[0];//按照最新设计原表中的protocol_id已经无法满足要求，需要从单位cmd中截取第一位
                v.setCmd(cmd);
                if(AFN_FN == null) {
                	v.setProtocolId("9");
                } else {
                	v.setProtocolId(protocolId);
                	rlt.put(AFN_FN, v);
                }
                rlt.put(StringUtil.parseString(map.get("ID")), v);//通过BUSINESS_DATAITEM_ID获取数据项对象
                String []c=cmd.split("-");
                if(c.length>3&&c[3].equals("5")){
                    rlt.put(StringUtil.parseString(map.get("ID") + "_698"+"_" + protocolId), v);//通过BUSINESS_DATAITEM_ID获取数据项对象
                }else{
                    rlt.put(StringUtil.parseString(map.get("ID") + "_" + protocolId), v);//通过BUSINESS_DATAITEM_ID获取数据项对象
                }
                rlt.put(StringUtil.parseString(map.get("DATAITEM_ID") + "_" + protocolId), v);
            }
        }
        return rlt;
    }

    /**
     * 输入CMD,格式如:1-0D-38
     * 输出AFN_FN,格式如：0D_38
     *
     * @param cmd
     * @return
     */
	private static String parseAFN_FN(String cmd) {
		if (null == cmd) {
			return null;
		}
		String AFN_FN = null;
		try {
			String[] lastTo = cmd.split("-");
			if (lastTo.length < 2) {
				return null;
			}
            if(lastTo.length==2){
                AFN_FN=lastTo[0]+"_"+lastTo[1];
            }else{
                AFN_FN = lastTo[lastTo.length - (lastTo.length-1)] + "_" + lastTo[lastTo.length - (lastTo.length-2)];
            }


//			if (lastTo.length == 5) {// 透传cmd格式如下：3-10-1-11-1B03FF01
//				AFN_FN = lastTo[lastTo.length - 4] + "_" + lastTo[lastTo.length - 3];
//			} else {
//				AFN_FN = lastTo[lastTo.length - 2] + "_" + lastTo[lastTo.length - 1];
//			}
		} catch (Exception e) {
			log.error("cmd解析异常，当前cmd为[" + cmd + "]", e);
		}

		return AFN_FN;
	}

    /**
     * 根据数据项ID返回DataItemView对象
     * 输入：150058_1
     * 返回：DataItemView
     *
     * @param dataItemId
     * @return
     */
    public synchronized static DataItemView getDataItemViewById(String dataItemId) {
        if (null == dataItemsCache) {
            initDataItems();
        }
        DataItemView div=dataItemsCache.get(dataItemId);
        //如果本地缓存没有获取到就根据数据项id查询出数据项信息增加到本地缓存中。2020-03-30 hch add
        if(div==null) {
            initDataItems();
            div=dataItemsCache.get(dataItemId);
        }
        return div;
    }

    /**
     * 根据AFN和FN返回DataItemView对象
     * 输入：0D,76
     * 返回：DataItemView
     *
     * @author 靳治强
     * @time 2013-8-10 上午11:47:37
     */
    public synchronized static DataItemView getDataItemView(String AFN, String FN) {
        String key = AFN + "_" + FN;
        if (null == dataItemsCache) {
            initDataItems();
        }
        return getDataItemView(key);
    }

    /**
     * 根据AFN和FN的组合返回DataItemView对象
     * 输入：0D_76
     * 输出：DataItemView
     *
     * @param AFN_FN
     * @return
     */
    public synchronized static DataItemView getDataItemView(String AFN_FN) {
    	dataItemsCache=null;
        if (null == dataItemsCache) {
            initDataItems();
        }
        return dataItemsCache.get(AFN_FN);
    }


    /**
     * 根据_AFN_FN组合返回DataItemView对象
     * 输入：_0D_F81
     * 输出：DataItemView
     *
     * @author 靳治强
     * @time 2013-8-13 上午09:49:49
     */
    public synchronized static DataItemView getDataItemV(String _AFN_FN) {
        String[] fns = _AFN_FN.split("_");
        String key = fns[1] + "_" + fns[2];
        if (null == dataItemsCache) {
            initDataItems();
        }
        return dataItemsCache.get(key);
    }

    // 根据自动任务编号从表【R_AUTOTASK_ITEMS】中获取数据项
    public static String GET_DATAITEMID_BY_TASKID = " SELECT BUSINESS_DATAITEM_ID FROM R_AUTOTASK_ITEMS WHERE AUTOTASK_ID = ? ";
    // 查询所有数据项
    private final static String FIND_ITEM_SQL = " SELECT B.ID, B.DATAITEM_ID,D.CMD,B.PROTOCOL_ID,D.P_ID,D. NAME,D.OBJ_FLAG,D.DATA_FLAG FROM R_PROTOCOL P, R_BUSINESS_DATAITEM B, R_DATAITEM D WHERE P.PROTOCOL_ID = B.PROTOCOL_ID AND B.DATAITEM_ID = D.DATAITEM_ID  ";

}
