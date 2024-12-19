package com.ls.pf.common.dataCath.bz.service.impl;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.util.AntPathMatcher;

import com.ls.pf.base.api.cache.ICache;
import com.ls.pf.common.dataCatch.bz.bo.ObjectBz;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;

/**
 * 缓存档案操作类
 *
 * @author jinzhiqiang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OperateBzData implements IOperateBzData {
    private final String KEY_HEADER = "M$";
    private final String KEY_HEADERSQR = "THM$";
    private final String USER_TYPE_HEADER = "U$";
    private final String P_MPED_HEADER = "P$";
    private final String SEPARATOR = "#";
    private final String POINT_HEADER = "P";
    private final String COMM_HEADER = "COMMADDR";
    private final String TOTAL_HEADER = "T";
    private final String PORT_HEADER = "D";
    private final String METERID_HEADER = "MI";
    // 2016-7-6杭州 多规约
    private final String MP_TMNL_HEADER = "TB$";
    private final String MP_R_MPED_HEADER = "MP$";

    private final String DATAITEM_HEADER = "DT$";
    // 根据AFN|FN反推数据项标识
    private final String DATAITEM = "PT$";
    //	private final String DATAITEM_3761 = "DT3761";
//	private final String DATAITEM_645 = "DT645";
    //分支箱
    private final String FZX_HEADER = "FZX$";
    //数据核查档案
    private final String DATACHECK_HEADER = "$R_";

    ICache cacheService;

    public ICache getCacheService() {
        return cacheService;
    }

    public void setCacheService(ICache cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * 初始化业务数据项档案信息
     *
     * @param value
     * @return
     */
    public boolean loadDataItem(List value) {
        boolean opres = false;
        Map key_map = new HashMap();
        Map dtMap = new HashMap();
        Map dt288Map = new HashMap();
        try {
            for (Object obj : value) {
                ObjectBz bzobj = (ObjectBz) obj;
                String dKey = DATAITEM_HEADER + bzobj.getDataItemId() + SEPARATOR + bzobj.getProtocolId();
                List<Map<String, String>> dataItem = bzobj.getDataItemList();
                Map dMap = new HashMap();
                for (Map<String, String> _map : dataItem) {
                    if (_map.containsKey("SPLITCMD") && _map.containsKey("NAME") && _map.get("NAME").contains("_交采")) {
                        dt288Map.put(_map.get("SPLITCMD"), bzobj.getDataItemId());
                    } else if (_map.containsKey("SPLITCMD") && _map.containsKey("NAME")
                            && !_map.get("NAME").contains("_交采")) {
                        dtMap.put(_map.get("SPLITCMD"), bzobj.getDataItemId());
                    }
                    Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        String _key = entry.getKey();
                        String _value = entry.getValue();
//						if (_key.equals("DUNIT")) {
//							dtMap.put(_value, bzobj.getDataItemId());
//						}
                        if (!_key.equals("SPLITCMD")) {
                            dMap.put(_key, _value);
                        }
                    }
                }
                key_map.put(dKey, dMap);
                /*
                 * if (bzobj.getProtocolId().equals("1")) {
                 * key_map.put(DATAITEM_3761, dtMap); }else if
                 * (bzobj.getProtocolId().equals("2")) {
                 * key_map.put(DATAITEM_645, dtMap); }
                 */
                key_map.put(DATAITEM + bzobj.getProtocolId(), dtMap);
                key_map.put(DATAITEM + bzobj.getProtocolId() + "288", dt288Map);
            }
            cacheService.batchHPut(key_map);
            opres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return opres;
    }

    /**
     * 初始化低电压p_standard_code
     *
     * @param value
     * @return
     */
    public boolean loadPCode(List value) {
        boolean opres = false;
        Map key_map = new HashMap();
        try {
            for (Object obj : value) {
                ObjectBz bzobj = (ObjectBz) obj;
                String dKey = "PC$" + bzobj.getDataItemId();
                List<Map<String, String>> dataItem = bzobj.getDataItemList();
                Map dMap = new HashMap();
                for (Map<String, String> _map : dataItem) {
                    Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        String _key = entry.getKey();
                        String _value = entry.getValue();
                        dMap.put(_key, _value);
                    }
                }
                key_map.put(dKey, dMap);
            }
            cacheService.batchHPut(key_map);
            opres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return opres;
    }

    /**
     * 保存单个测量点对象
     *
     * @param value
     * @return
     */
    public boolean saveObj(Object value) {
        boolean opres = false;
        ObjectBz bzobj = (ObjectBz) value;
        String mKey = KEY_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
        String uKey = USER_TYPE_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
        cacheService.remove(mKey);// 先删除再放
        cacheService.remove(uKey);
        Map pointmap = wrapMap(bzobj.getPointNum(), POINT_HEADER);
        Map commaddrmap = wrapMap(bzobj.getCommAddr(), COMM_HEADER);
        Map meterIdmap = wrapMap(bzobj.getMeterIdMap(), METERID_HEADER);
        Map totalmap = wrapMap(bzobj.getTotalNum(), TOTAL_HEADER);
        Map portmap = wrapMap(bzobj.getPortNum(), PORT_HEADER);
        Map userFlagMap = wrapMap(bzobj.getUserFlag(), POINT_HEADER);
        try {
            if (userFlagMap != null) {
                cacheService.hput(uKey, "TPID", bzobj.getProtocolId());
                cacheService.hmput(uKey, userFlagMap);
            }
            cacheService.hput(mKey, "ORG", bzobj.getDepartCode());
            cacheService.hput(mKey, "TMNLID", bzobj.getTmnlId());
            cacheService.hmput(mKey, pointmap);
            cacheService.hmput(mKey, commaddrmap);
            cacheService.hmput(mKey, meterIdmap);

            if (totalmap != null)
                cacheService.hmput(mKey, totalmap);
            if (portmap != null)
                cacheService.hmput(mKey, portmap);
            opres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return opres;
    }

    public static void main(String[] args) {
        String key = "ADDR#1254";
        int _idx = key.indexOf("#");
        String _key = key.substring(0, _idx);
        String pn = key.substring(_idx + 1, key.length());
        System.out.println(_key);
        System.out.println(pn);
    }

    /**
     * 批量保存测量点对象
     *
     * @param value
     * @return
     */
    public boolean saveObj(List value) {
        boolean opres = false;
        removeObj(value);// 批量删除原来所有的再放，保证统一
        Map key_map = new HashMap();
        for (Object obj : value) {
            ObjectBz bzobj = (ObjectBz) obj;
            String mKey = KEY_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            String uKey = USER_TYPE_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            String mpKey = MP_TMNL_HEADER + bzobj.getTmnlId();

            List<Map<String, String>> meterAttribute = bzobj.getMeterAttribute();
            for (Map<String, String> _map : meterAttribute) {
                Map pMap = new HashMap();
                Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                String pKey = null;
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String _key = entry.getKey();
                    String _value = entry.getValue();
                    int _idx = _key.indexOf(SEPARATOR);
                    String _pkey = _key.substring(0, _idx);
                    String _pn = _key.substring(_idx + 1, _key.length());
                    pKey = P_MPED_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd() + SEPARATOR + _pn;
                    pMap.put(_pkey, _value);
                }
                key_map.put(pKey, pMap);
            }
            // 2016-7-6杭州 多规约
            List<Map<String, String>> meterMP = bzobj.getMeterMP();
            for (Map<String, String> _map : meterMP) {
                Map pMap = new HashMap();
                Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                String pKey = null;
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String _key = entry.getKey();
                    String _value = entry.getValue();
                    int _idx = _key.indexOf(SEPARATOR);
                    String _pkey = _key.substring(0, _idx);
                    String _mpedId = _key.substring(_idx + 1, _key.length());
                    pKey = MP_R_MPED_HEADER + _mpedId;
                    pMap.put(_pkey, _value);
                }
                key_map.put(pKey, pMap);
            }

            Map pointmap = wrapMap(bzobj.getPointNum(), POINT_HEADER);
            Map commaddrmap = wrapMap(bzobj.getCommAddr(), COMM_HEADER);
            Map meterIdmap = wrapMap(bzobj.getMeterIdMap(), METERID_HEADER);
            Map totalmap = wrapMap(bzobj.getTotalNum(), TOTAL_HEADER);
            Map portmap = wrapMap(bzobj.getPortNum(), PORT_HEADER);
            Map userFlagMap = wrapMap(bzobj.getUserFlag(), POINT_HEADER);
            Map uMap = new HashMap();
            if (userFlagMap != null) {
                uMap.putAll(userFlagMap);
                uMap.put("TPID", bzobj.getProtocolId());
            }
            Map mMap = new HashMap();
            mMap.put("ORG", bzobj.getDepartCode());
            mMap.put("TMNLID", bzobj.getTmnlId());
            mMap.put("TPID", bzobj.getProtocolId());
            mMap.putAll(pointmap);
            mMap.putAll(commaddrmap);
            if (meterIdmap != null) {
                mMap.putAll(meterIdmap);
            }
            if (totalmap != null) {
                mMap.putAll(totalmap);
            }
            if (portmap != null) {
                mMap.putAll(portmap);
            }
            Map mpMap = new HashMap();
            mpMap.put("ORG", bzobj.getDepartCode());
            mpMap.put("ADDR", bzobj.getAreaCode() + "|" + bzobj.getTermAdd());
            mpMap.put("PTL", bzobj.getProtocolId());
            mpMap.put("SCMNO", bzobj.getSchemeNo());
            key_map.put(mKey, mMap);
            key_map.put(uKey, uMap);
            key_map.put(mpKey, mpMap);// TM$+终端业务标识
        }
        cacheService.batchHPut(key_map);
        opres = true;
        return opres;
    }

    /**
     * 水汽热批量保存测量点对象
     *
     * @param value
     * @return
     */
    public boolean saveObjSQR(List value) {
        boolean opres = false;
//		removeObj(value);// 批量删除原来所有的再放，保证统一
        Map key_map = new HashMap();
        for (Object obj : value) {
            ObjectBz bzobj = (ObjectBz) obj;
            String mKey = KEY_HEADERSQR + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            //				String uKey = USER_TYPE_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            //				String mpKey = MP_TMNL_HEADER + bzobj.getTmnlId();

            List<Map<String, String>> meterAttribute = bzobj.getMeterAttribute();
            for (Map<String, String> _map : meterAttribute) {
                Map pMap = new HashMap();
                Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                String pKey = null;
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String _key = entry.getKey();
                    String _value = entry.getValue();
                    int _idx = _key.indexOf(SEPARATOR);
                    String _pkey = _key.substring(0, _idx);
                    String _pn = _key.substring(_idx + 1, _key.length());
                    pKey = P_MPED_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd() + SEPARATOR + _pn;
                    pMap.put(_pkey, _value);
                }
                //					key_map.put(pKey, pMap);
            }
            // 2016-7-6杭州 多规约
            List<Map<String, String>> meterMP = bzobj.getMeterMP();
            for (Map<String, String> _map : meterMP) {
                Map pMap = new HashMap();
                Iterator<Map.Entry<String, String>> iterator = _map.entrySet().iterator();
                String pKey = null;
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String _key = entry.getKey();
                    String _value = entry.getValue();
                    int _idx = _key.indexOf(SEPARATOR);
                    String _pkey = _key.substring(0, _idx);
                    String _mpedId = _key.substring(_idx + 1, _key.length());
                    pKey = MP_R_MPED_HEADER + _mpedId;
                    pMap.put(_pkey, _value);
                }
                //					key_map.put(pKey, pMap);
            }

            Map pointmap = wrapMap(bzobj.getPointNum(), POINT_HEADER);
            Map commaddrmap = wrapMap(bzobj.getCommAddr(), COMM_HEADER);
            Map meterIdmap = wrapMap(bzobj.getMeterIdMap(), METERID_HEADER);
            Map totalmap = wrapMap(bzobj.getTotalNum(), TOTAL_HEADER);
            Map portmap = wrapMap(bzobj.getPortNum(), PORT_HEADER);
            Map userFlagMap = wrapMap(bzobj.getUserFlag(), POINT_HEADER);
            Map uMap = new HashMap();
            if (userFlagMap != null) {
                uMap.putAll(userFlagMap);
                uMap.put("TPID", bzobj.getProtocolId());
            }
            Map mMap = new HashMap();
            mMap.put("ORG", bzobj.getDepartCode());
            mMap.put("TMNLID", bzobj.getTmnlId());
            mMap.put("TPID", bzobj.getProtocolId());
            mMap.putAll(pointmap);
            mMap.putAll(commaddrmap);
            if (meterIdmap != null) {
                mMap.putAll(meterIdmap);
            }
            if (totalmap != null) {
                mMap.putAll(totalmap);
            }
            if (portmap != null) {
                mMap.putAll(portmap);
            }
            Map mpMap = new HashMap();
            mpMap.put("ORG", bzobj.getDepartCode());
            mpMap.put("ADDR", bzobj.getAreaCode() + "|" + bzobj.getTermAdd());
            mpMap.put("PTL", bzobj.getProtocolId());
            key_map.put(mKey, mMap);
            //				key_map.put(uKey, uMap);
            //				key_map.put(mpKey, mpMap);// TM$+终端业务标识
        }
        cacheService.batchHPut(key_map);
        opres = true;
        return opres;
    }

    /**
     * 加载数据核查档案
     *
     * @param value
     * @return
     */
    public boolean saveDataCheck(List value) {
        boolean opres = false;
        Map key_map = new HashMap();
        try {
            for (Object obj : value) {
                ObjectBz bzobj = (ObjectBz) obj;
                String dKey = DATACHECK_HEADER + bzobj.getMpedId();
                String val = bzobj.getVal();
                key_map.put(dKey, val);
            }
            cacheService.batchHPut(key_map);
            opres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return opres;
    }

    /**
     * 删除单个测量点数据
     *
     * @param value
     * @return
     */
    public boolean removeObj(Object value) {
        boolean delres = false;
        ObjectBz bzobj = (ObjectBz) value;
        String mKey = KEY_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
        String uKey = USER_TYPE_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
        String mpKey = MP_TMNL_HEADER + bzobj.getTmnlId();
        try {
            List<String> paramList = new ArrayList<>();
            paramList.add(mKey);
            paramList.add(uKey);
            paramList.add(mpKey);
            // cacheService.hdelall(key);
            long l = cacheService.batchDel(paramList);
            System.out.println(l);
            delres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return delres;
    }

    /**
     * 批量删除测量点数据
     *
     * @param value
     * @return
     */
    public boolean removeObj(List value) {
        boolean listdelres = false;
        List<String> lkey = new ArrayList<String>();
        for (int i = 0; i < value.size(); i++) {
            ObjectBz bzobj = (ObjectBz) value.get(i);
            String mKey = KEY_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            lkey.add(mKey);
            String uKey = USER_TYPE_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
            lkey.add(uKey);
        }
        try {
            cacheService.batchDel(lkey);
            listdelres = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return listdelres;
    }

    /**
     * 获取测量点所有信息
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getObjectByPn(String areaCode, String termAdd) {
        String key = KEY_HEADER + areaCode + SEPARATOR + termAdd;
        Map map = new HashMap();
        try {
            map = cacheService.hgetAll(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 水汽热获取测量点所有信息
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getObjectByPnSQR(String areaCode, String termAdd) {
        String key = KEY_HEADERSQR + areaCode + SEPARATOR + termAdd;
        Map map = new HashMap();
        try {
            map = cacheService.hgetAll(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public List<Map> batchGetPointNum(List objlist) {
        List keylist = new ArrayList();
        List<Map> mp = new ArrayList();
        try {
            for (Object obj : objlist) {
                ObjectBz bzobj = (ObjectBz) obj;
                String key = KEY_HEADER + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
                keylist.add(key);
            }
            mp = cacheService.batchGet(keylist);
            return mp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Map> batchGetPointNumSQR(List objlist) {
        List keylist = new ArrayList();
        List<Map> mp = new ArrayList();
        try {
            for (Object obj : objlist) {
                ObjectBz bzobj = (ObjectBz) obj;
                String key = KEY_HEADERSQR + bzobj.getAreaCode() + SEPARATOR + bzobj.getTermAdd();
                keylist.add(key);
            }
            mp = cacheService.batchGet(keylist);
            return mp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据行政区划码+终端地址+测量点序号，查找对应的测量点属性
     *
     * @param areaCode
     * @param termAdd
     * @param mpedIndex
     * @return
     */
    public Map getAllPMped(String areaCode, String termAdd, String mpedIndex) {
        String key = P_MPED_HEADER + areaCode + SEPARATOR + termAdd + SEPARATOR + mpedIndex;
        return cacheService.hgetAll(key);
    }

    /**
     * 根据行政区划码+终端地址，查找对应的分支箱属性
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getFZX(String areaCode, String termAdd) {
        String key = FZX_HEADER + areaCode + SEPARATOR + termAdd;
        return cacheService.hgetAll(key);
    }

    /**
     * 根据测量点业务标识(mped_id)，查找对应的测量点属性
     *
     * @param
     * @return
     */
    public Map getAllPMpedFromMP(String mpedId) {
        String key = MP_R_MPED_HEADER + mpedId;
        return cacheService.hgetAll(key);
    }

    /**
     * 获取某一终端所有的测量点对应的用户类型
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAllPointUserType(String areaCode, String termAdd) {
        String key = USER_TYPE_HEADER + areaCode + SEPARATOR + termAdd;
        return cacheService.hgetAll(key);
    }

    /**
     * 获取TB$终端缓存信息
     *
     * @param terminalId
     * @return
     */
    public Map getTB(String terminalId) {
        String key = MP_TMNL_HEADER + terminalId;
        return cacheService.hgetAll(key);
    }

    public String get(String key) {
        return (String) cacheService.get(key);
    }

    /**
     * 获取某一终端所有的测量点号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAllPointNum(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADER + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = "*" + POINT_HEADER + "*";
            String selectaddr = COMM_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                } else if (matcher.match(selectaddr, mapkey)) {
                    map.put(entry.getValue(), mapkey.substring(8, mapkey.length()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 水汽热获取某一终端所有的测量点号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAllPointNumSQR(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADERSQR + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = "*" + POINT_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取某一终端所有的端口号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAllportNum(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADER + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = PORT_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 水汽热获取某一终端所有的端口号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAllportNumSQR(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADERSQR + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = PORT_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取某一终端所有的总加组号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAlltotalNumber(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADER + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = TOTAL_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        map.remove("TMNLID");// filter TMNLID field
        return map;
    }

    /**
     * 水汽热获取某一终端所有的总加组号
     *
     * @param areaCode
     * @param termAdd
     * @return
     */
    public Map getAlltotalNumberSQR(String areaCode, String termAdd) {
        Map map = new HashMap();
        String key = KEY_HEADERSQR + areaCode + SEPARATOR + termAdd;
        try {
            AntPathMatcher matcher = new AntPathMatcher();
            Map tmpmap = cacheService.hgetAll(key);
            String selectkey = TOTAL_HEADER + "*";
            Iterator keyterator = tmpmap.entrySet().iterator();
            while (keyterator.hasNext()) {
                Map.Entry entry = (Map.Entry) keyterator.next();
                String mapkey = entry.getKey().toString();
                if (matcher.match(selectkey, mapkey)) {
                    map.put(mapkey, entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        map.remove("TMNLID");// filter TMNLID field
        return map;
    }

    public Map wrapMap(Map mp, String header) {
        if (mp == null) {
            return mp;
        }
        Map tempmp = new HashMap();
        Iterator keyterator = mp.entrySet().iterator();
        while (keyterator.hasNext()) {
            Map.Entry entry = (Map.Entry) keyterator.next();
            String key = entry.getKey().toString();
            String endkey = header + key;
            tempmp.put(endkey, mp.get(key));
        }
        return tempmp;
    }


    public String getTmnl(String tmnlId, String filed) {
        String str = (String) cacheService.hget(MP_TMNL_HEADER + tmnlId, filed);
        return str;
    }

    /**
     * 加载单个hash结构缓存
     */
    public void saveHash(Map<String, Map> mapObj) {
        cacheService.batchHPut(mapObj);
    }

    /**
     * hmget
     */
    public List<Object> hmget(String key, String[] paramArrayOfString) {
        return cacheService.hmget(key, paramArrayOfString);
    }
}
