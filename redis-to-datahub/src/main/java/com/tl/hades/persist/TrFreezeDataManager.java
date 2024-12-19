package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.tl.hades.persist.CommonUtils.getArchives;

/**
 * @author Dongwei-Chen
 * @Date 2021/3/9 16:13
 * @Description 直抄日冻结
 */
public class TrFreezeDataManager {

    static Logger logger = LoggerFactory.getLogger(TrFreezeDataManager.class);

    private static List<String> dataItems = Arrays.asList(new String[]{
            "05060101", "05060102", "05060103", "05060104", "05060105", "05060106", "05060107",
            "05060108", "05060109", "05060110", "05060111", "05060112", "05060113", "05060114",
            "05060115", "05060116", "05060117", "05060118", "05060119", "05060120", "05060121",
            "05060122", "05060123", "05060124", "05060125", "05060126", "05060127", "05060128",
            "05060129", "05060130", "05060131", "05060132", "05060133", "05060134", "05060135", "05060136"
            , "05060137", "05060138", "05060139", "05060140", "05060141", "05060142", "05060143", "05060144"
            , "05060145", "05060146", "05060147", "05060148", "05060149", "05060150", "05060151", "05060152"
            , "05060153", "05060154", "05060155", "05060156", "05060157", "05060158", "05060159", "05060160"
            , "05060161", "05060162"
    });

    /**
     * @param terminalDataObject
     * @param protocolId
     * @param listDataObj
     * @param data
     * @param front
     */
    public static void getDataList(TerminalDataObject terminalDataObject, int protocolId, List<DataObject> listDataObj, DataItemObject data, String front) {
        List dataList = data.getList();
        if (dataList.size() == 0) {
            return;
        }
        int afn = terminalDataObject.getAFN();
        int fn = data.getFn();
        String areaCode = terminalDataObject.getAreaCode();
        int terminalAddr = terminalDataObject.getTerminalAddr();
        logger.info("===MAKE " + afn + "_" + fn + " DATA===");

        Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, afn, fn);
        String items = dataList.get(7).toString();
        String substring = items.substring(items.length() - 2);
        int i1 = Integer.parseInt(substring, 16);
        String item;
        if (i1 < 10) {
            item = "0" + i1;
        } else {
            item = String.valueOf(i1);
        }
        String dataItem = items.replace(substring, item);
        String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, dataItem).getBusiDataItemId();

        TerminalArchivesObject tao = getArchives(areaCode, terminalAddr, dataList.get(6).toString());
        Object[] refreshKey = null;
        String terminalId = tao.getTerminalId();
        String mpedIdStr = tao.getID();
        List finalDataList = new ArrayList();
        if (mpedIdStr == null || "null".equals(mpedIdStr)) {
            return;
        }
        //直抄日冻结正向有功
        if (dataItems.contains(dataItem)) {
            String dateStr = String.valueOf(dataList.get(5));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Date date = null;
            try {
                date = sdf.parse(dateStr);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            int index = dataItems.indexOf(dataItem);
            calendar.add(Calendar.DATE, -index);
            Date time = calendar.getTime();
            SimpleDateFormat ymd = new SimpleDateFormat("yyyyMMdd");
            String formatOne = ymd.format(date);
            String formatTwo = ymd.format(time);
            if (!formatOne.equals(formatTwo)) {
                return;
            }
            calendar.add(Calendar.DATE, -1);
            date = calendar.getTime();
            finalDataList.add(mpedIdStr + "_" + DateUtil.format(date, DateUtil.defaultDatePattern_YMD));
            finalDataList.add(mpedIdStr);
            finalDataList.add("1");
            finalDataList.add(new Date());
            for (int i = 0; i < 5; i++) {
                finalDataList.add(dataList.get(i));
            }
            for (int i = 0; i < 10; i++) {
                finalDataList.add(null);
            }
            String orgNo = tao.getPowerUnitNumber();
            finalDataList.add(orgNo);
            finalDataList.add("00");
            if ("front".equals(front)) {
                finalDataList.add("24");
            } else {
                finalDataList.add("20");
            }

            finalDataList.add(new Date());
            finalDataList.add(orgNo.substring(0, 5));
            finalDataList.add(DateUtil.format(date, DateUtil.defaultDatePattern_YMD));
            refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
            businessDataitemId = "tr_freeze";

        }

        try {
            CommonUtils.putToDataHub(businessDataitemId, terminalId, finalDataList, refreshKey, listDataObj);
        } catch (
                Exception e) {
            e.printStackTrace();
        }


    }
}
