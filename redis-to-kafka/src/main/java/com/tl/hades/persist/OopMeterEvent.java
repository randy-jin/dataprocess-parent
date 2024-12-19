package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OopMeterEvent {

    /**
     * 直抄/预抄
     */
    public final static Map<String, String> itemMap = new HashMap<>();

    static {
        itemMap.put("03110001", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110002", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110003", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110004", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110005", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110006", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110007", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110008", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("03110009", "E_METER_EVENT_NO_POWER_SOURCE");
        itemMap.put("0311000A", "E_METER_EVENT_NO_POWER_SOURCE");

        itemMap.put("03300D01", "E_METER_EVENT_OPEN_LID");//开表盖事件
        itemMap.put("03300D02", "E_METER_EVENT_OPEN_LID");//开表盖事件2
        itemMap.put("03300D03", "E_METER_EVENT_OPEN_LID");//开表盖事件3
        itemMap.put("03300D04", "E_METER_EVENT_OPEN_LID");//开表盖事件4
        itemMap.put("03300D05", "E_METER_EVENT_OPEN_LID");//开表盖事件5
        itemMap.put("03300D06", "E_METER_EVENT_OPEN_LID");//开表盖事件6
        itemMap.put("03300D07", "E_METER_EVENT_OPEN_LID");//开表盖事件7
        itemMap.put("03300D08", "E_METER_EVENT_OPEN_LID");//开表盖事件8
        itemMap.put("03300D09", "E_METER_EVENT_OPEN_LID");//开表盖事件9
        itemMap.put("03300D0A", "E_METER_EVENT_OPEN_LID");//开表盖事件10
    }

    public static List<Object> putToDataHub(String oopDataItemId, List dataList, String... str) throws ParseException {
        List<Object> dataListFinal = new ArrayList<>();
        switch (itemMap.get(oopDataItemId)) {
            case "E_METER_EVENT_NO_POWER_SOURCE":
                String stTime = (String) dataList.get(0);
                String edTime = (String) dataList.get(1);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                dataListFinal.add(new BigDecimal(str[0]));
                dataListFinal.add(new Date());
                dataListFinal.add(str[1]);
                dataListFinal.add("0");
                dataListFinal.add(sdf.parse(stTime));
                dataListFinal.add(sdf.parse(edTime));
                dataListFinal.add(str[1].substring(0, 5));
                dataListFinal.add(DateUtil.format(sdf.parse(edTime), DateUtil.defaultDatePattern_YMD));
                break;
            case "E_METER_EVENT_OPEN_LID":
                break;
            default:
                return null;
        }
        return dataListFinal;
    }
}
