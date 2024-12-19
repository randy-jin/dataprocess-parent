package com.tl.hades.persist;

import com.google.common.collect.ImmutableSet;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.utils.idgenerate.DefaultIdGenerator;
import com.tl.dataprocess.utils.idgenerate.IdGenerator;
import com.tl.easb.utils.DateUtil;
import com.tl.hades.objpro.api.beans.DataObject;
import com.tl.hades.objpro.api.beans.MeterData;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author wangjunjie
 * @date 2022/8/15 17:41
 */
public class SmartIotEventUtils {
    private final static Logger logger = LoggerFactory.getLogger(SmartIotEventUtils.class);
    private final static IdGenerator idGenerator = DefaultIdGenerator.newInstance();
    private static Map<String, String> codeMap = new HashMap<>();

    static {
        codeMap.put("303A0800", "e_meter_event_current_distort_source");//电流谐波总畸变率超限事件
        codeMap.put("30390800", "e_meter_event_voltage_distort_source");//电压谐波总畸变率超限事件
        codeMap.put("303B0700", "e_meter_event_power_factor_lower_source");//电能表功率因数超下限事件
        codeMap.put("30400200", "e_meter_event_zero_current_abnormal_source");//电能表零线电流异常事件
        codeMap.put("30310200", "e_meter_event_module_plug_source");//管理模组插拔事件记录

        codeMap.put("30360200", "e_meter_event_upgrade_source");//升级事件
        codeMap.put("303C0200", "e_meter_event_broadcast_timing_source");//电能表广播校时事件
        codeMap.put("30500200", "e_meter_event_module_powerloss_source");//计量模组掉电事件
        codeMap.put("36020200", "e_meter_event_app_monitor_source");//应用监测事件
        codeMap.put("30320200", "e_meter_event_block_overheat_source");//端子座过热报警事件记录
        codeMap.put("30330200", "e_meter_event_block_temp_change_source");//端子座温度剧变事件
        codeMap.put("30340200", "e_meter_event_block_temp_unbalance_source");//端子座温度不平衡事件

        codeMap.put("F20B0400", "r_meter_bluetooth_devicelist_source");//蓝牙从设备列表
        codeMap.put("F20B0700", "r_meter_bluetooth_locklist_source");//蓝牙锁具列表

        codeMap.put("20100400", "r_meter_block_temp_source");//表内温度（端子温度组）
        codeMap.put("20430200", "r_meter_block_temp_min_source");//端子座温度分钟变化量
        codeMap.put("37010200", "e_meter_event_lock_source");//端子座温度分钟变化量

        codeMap.put("20000200", "e_mp_phase_voltage_source");//电压（分相数值数组）
        codeMap.put("20000700", "e_mp_phase_deviation_voltage_source");//电压（分相偏差数值数组）
        codeMap.put("20000600", "e_mp_zero_plus_minus_voltage_source");//电压（零正负序）-零序
        codeMap.put("20000900", "e_mp_zero_plus_minus_voltage_source");//电压（零正负序）-正序
        codeMap.put("20000A00", "e_mp_zero_plus_minus_voltage_source");//电压（零正负序）-负序
        codeMap.put("20010500", "e_mp_high_precision_current_source");// 高精度电流
        codeMap.put("20010600", "e_mp_zero_plus_minus_current_source");//电流（零正负序）-零序
        codeMap.put("20010900", "e_mp_zero_plus_minus_current_source");//电流（零正负序）-正序
        codeMap.put("20010A00", "e_mp_zero_plus_minus_current_source");//电流（零正负序）-负序
        codeMap.put("200B0200", "e_mp_voltage_waveform_distortion_source");//电压波形失真度
        codeMap.put("200C0200", "e_mp_current_waveform_distortion_source");//电流波形失真度
        codeMap.put("20350200", "e_mp_voltage_fluctuation_source");//电压波动量
        codeMap.put("20360200", "e_mp_voltage_fluctuation_frequency_source");//电压波动频次
        codeMap.put("20260400", "e_mp_voltage_unbalance_source");//电压不平衡-负序
        codeMap.put("20260500", "e_mp_voltage_unbalance_source");//电压不平衡-零序
        codeMap.put("20270400", "e_mp_current_unbalance_source");//电流不平衡-负序
        codeMap.put("20270500", "e_mp_current_unbalance_source");//电流不平衡-零序
        codeMap.put("20490200", "e_mp_inter_voltage_content_source");//间谐波电压含有率
        codeMap.put("204A0200", "e_mp_inter_current_content_source");//间谐波电流含有率
        codeMap.put("20010700", "e_mp_high_precision_current_unit_source");//高精度电流换算及单位
        codeMap.put("200F0200", "r_meter_grid_frequency_source");//电网频率
        codeMap.put("20370200", "e_mp_voltage_short_flicker_source");//电压短时闪变
        codeMap.put("20380200", "e_mp_voltage_long_flicker_source");//电压长时闪变
        codeMap.put("48000500", "r_meter_software_sign_source");//软件标识
        codeMap.put("200D0200", "e_mp_harmonic_voltage_content_source");//谐波电压含有率-A相
        codeMap.put("200D0300", "e_mp_harmonic_voltage_content_source");//谐波电压含有率-B相
        codeMap.put("200D0400", "e_mp_harmonic_voltage_content_source");//谐波电压含有率-C相
        codeMap.put("200E0200", "e_mp_harmonic_current_content_source");//谐波电流含有率-A相
        codeMap.put("200E0300", "e_mp_harmonic_current_content_source");//谐波电流含有率-B相
        codeMap.put("200E0400", "e_mp_harmonic_current_content_source");//谐波电流含有率-C相
        codeMap.put("20480200", "e_mp_harmonic_active_power_source");//谐波有功功率-A相
        codeMap.put("20480300", "e_mp_harmonic_active_power_source");//谐波有功功率-B相
        codeMap.put("20480400", "e_mp_harmonic_active_power_source");//谐波有功功率-C相
        codeMap.put("20600200", "e_mp_inter_voltage_source");//间谐波电压-A相
        codeMap.put("20600300", "e_mp_inter_voltage_source");//间谐波电压-B相
        codeMap.put("20600400", "e_mp_inter_voltage_source");//间谐波电压-C相
        codeMap.put("20610200", "e_mp_inter_current_source");//间谐波电流-A相
        codeMap.put("20610300", "e_mp_inter_current_source");//间谐波电流-B相
        codeMap.put("20610400", "e_mp_inter_current_source");//间谐波电流-C相
        codeMap.put("20620200", "e_mp_inter_power_source");//间谐波功率-A相
        codeMap.put("20620300", "e_mp_inter_power_source");//间谐波功率-B相
        codeMap.put("20620400", "e_mp_inter_power_source");//间谐波功率-C相
//        codeMap.put("20620400", "e_meter_event_voltage_increase_source");//电压暂升事件
        codeMap.put("304B0200", "e_meter_event_voltage_interrupt_source");//电压中断事件
        codeMap.put("30490200", "e_meter_event_voltage_reduce_source");//电压暂降事件
        codeMap.put("20420200", "r_meter_bluetooth_switch_status_source");//外置蓝牙负荷开关状态字

    }
    private static final ImmutableSet<String> volAndCurSet = ImmutableSet.of("e_mp_inter_voltage_content_source",
                    "e_mp_inter_current_content_source", "e_mp_harmonic_voltage_content_source",
                    "e_mp_harmonic_current_content_source", "e_mp_harmonic_active_power_source",
                    "e_mp_inter_voltage_source", "e_mp_inter_current_source",
                    "e_mp_inter_power_source");

    public static void smartIotDataHandler(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        String topicName = codeMap.get(businessDataitemId);
        if (StringUtils.isBlank(topicName)) {
            logger.error("businessDataitemId:" + businessDataitemId + " 对应的 topicName 不存在");
            return;
        }
        switch (topicName) {
            case "e_meter_event_current_distort_source":
            case "e_meter_event_voltage_distort_source":
            case "e_meter_event_power_factor_lower_source":
            case "e_meter_event_zero_current_abnormal_source":
            case "e_meter_event_module_plug_source":
            case "e_meter_event_upgrade_source":
            case "e_meter_event_broadcast_timing_source":
            case "e_meter_event_module_powerloss_source":
            case "e_meter_event_app_monitor_source":
            case "e_meter_event_block_overheat_source":
            case "e_meter_event_block_temp_change_source":
            case "e_meter_event_block_temp_unbalance_source":
            case "e_meter_event_lock_source":
                getEventData(dataList, areaCode, termAddr, businessDataitemId, listDataObj);
                break;
            case "r_meter_bluetooth_devicelist_source":
            case "r_meter_bluetooth_locklist_source":
                getBluetoothDeviceData(dataList, areaCode, termAddr, businessDataitemId, listDataObj);
                break;
            case "r_meter_block_temp_source":
            case "r_meter_block_temp_min_source":
                getBlockTempData(dataList, areaCode, termAddr, businessDataitemId, listDataObj);
                break;
            case "e_mp_phase_voltage_source":
            case "e_mp_phase_deviation_voltage_source":
            case "e_mp_zero_plus_minus_voltage_source":
            case "e_mp_high_precision_current_source":
            case "e_mp_zero_plus_minus_current_source":
            case "e_mp_voltage_waveform_distortion_source":
            case "e_mp_current_waveform_distortion_source":
            case "e_mp_voltage_fluctuation_source":
            case "e_mp_voltage_fluctuation_frequency_source":
            case "e_mp_voltage_unbalance_source":
            case "e_mp_current_unbalance_source":
            case "e_mp_inter_voltage_content_source":
            case "e_mp_inter_current_content_source":
            case "e_mp_high_precision_current_unit_source":
            case "e_mp_voltage_short_flicker_source":
            case "e_mp_voltage_long_flicker_source":
            case "e_mp_harmonic_voltage_content_source":
            case "e_mp_harmonic_current_content_source":
            case "e_mp_harmonic_active_power_source":
            case "e_mp_inter_voltage_source":
            case "e_mp_inter_current_source":
            case "e_mp_inter_power_source":
                getVoltageOrCurrentData(dataList, areaCode, termAddr, businessDataitemId, listDataObj);
                break;
            case "r_meter_grid_frequency_source":
            case "r_meter_software_sign_source":
                getMeterData(dataList, areaCode, termAddr, businessDataitemId, listDataObj);
                break;
            default:break;
        }
    }

    private static void getEventData(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        for (MeterData meterData : dataList) {
            Map<Object, Object> dataMap = new HashMap<>();
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meterData.getMeterData();
            if (doList != null && doList.size() > 0) {
                if (OopTmnlEventConcat.isAllNull(doList)) {
                    continue;
                }
                String meterId = meterData.getMeterAddr();
                StringBuilder extenInfo = new StringBuilder();
                StringBuilder reportStatus = new StringBuilder("[");
                boolean startAppend = false;
                for (DataObject dataObj : doList) {
                    //dataItem 201E0200 data就是发生时间
                    //dataItem 20200200 data就是结束时间
                    String dataItem = dataObj.getDataItem();
                    Object data = dataObj.getData();
                    if (data == null || "null".equals(data)) {
                        continue;
                    }
                    dataMap.put(dataItem, data);
                    if (startAppend) {
                        extenInfo.append(dataItem).append("#").append(data).append(";");
                    }
                    if (dataObj.getDataItem().equals("33000200")) {
                        startAppend = true;
                        List<String> tempList = (List<String>)data;
                        if (tempList.size() > 0) {
                            for (String aTempList : tempList) {
                                if ("null".equals(aTempList)) continue;
                                int index = aTempList.indexOf(",");
                                reportStatus.append("{OAD:").append(aTempList, 1, index).append(",上报状态:").append(aTempList, index + 1, aTempList.length() - 1).append("};");
                            }
                            if (reportStatus.lastIndexOf(";")>0) {
                                reportStatus.deleteCharAt(reportStatus.lastIndexOf(";"));
                            }
                            reportStatus.append("]");
                        }
                    }
                }
                //获取档案信息
                TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meterData.getMeterAddr());
                if (null == terminalArchivesObject) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                    continue;
                    //TODO 测试后删除
//                    terminalArchivesObject = new TerminalArchivesObject();
//                    terminalArchivesObject.setTerminalId("123123345");
//                    terminalArchivesObject.setPowerUnitNumber("114018778907");
                }
                String tmnlId = terminalArchivesObject.getTerminalId();
                if (null == tmnlId || "".equals(tmnlId)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                    continue;
                }

                List<Object> dataListFinal = new ArrayList<Object>();
                String eventId = idGenerator.next();
                dataListFinal.add(eventId);
                dataListFinal.add(new BigDecimal(meterId));
                Date startDate = DateUtil.parse(dataMap.get("201E0200").toString(), DateUtil.defaultDatePattern_Y_M_D_HMS);
                dataListFinal.add(DateUtil.formatDate(startDate, DateUtil.FMT_DATE_YYYY_MM_DD));
                dataListFinal.add(new Date());
                dataListFinal.add(extenInfo.toString());// 事件扩展信息
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());
                dataListFinal.add(startDate);
                String endDateStr = dataMap.get("20200200").toString();
                if (StringUtils.isBlank(endDateStr) || endDateStr.contains("null")) {
                    endDateStr = null;
                }
                Date endDate = DateUtil.parse(endDateStr, DateUtil.defaultDatePattern_Y_M_D_HMS);
                dataListFinal.add(endDate);
                dataListFinal.add(meterId);// 事件发生源,即电表通信地址
                dataListFinal.add(reportStatus.toString());// 事件上报状态,格式为[{OAD:xxxxxxxx,上报状态:xxxxxxxx},
                switch (codeMap.get(businessDataitemId)) {
                    case "e_meter_event_upgrade_source":
                        dataListFinal.add(startDate);//升级发生时间
                        dataListFinal.add(endDate);//升级结束时间
                        dataListFinal.add(dataMap.get("40270300"));//升级操作类型
                        if (null != dataMap.get("00102200")) {
                            dataListFinal.add(dataMap.get("00102200").toString());//升级前的正向有功电能
                        } else {
                            dataListFinal.add("");
                        }
                        if (null != dataMap.get("00202200")) {
                            dataListFinal.add(dataMap.get("00202200").toString());//升级前的反向有功电能
                        } else {
                            dataListFinal.add("");
                        }
                        dataListFinal.add(dataMap.get("F0012500"));//下载方的标识
                        dataListFinal.add(dataMap.get("F4022204"));//升级前软件版本号
                        dataListFinal.add(dataMap.get("F4028204"));//升级后软件版本号
                        dataListFinal.add(dataMap.get("F4028202"));//升级应用名称
                        dataListFinal.add(dataMap.get("40278200"));//升级结果
                        break;
                    case "e_meter_event_app_monitor_source":
                        dataListFinal.add(dataMap.get("33270206"));//cpu （0），内存 （1），存储 （2），主板温度 （3）
                        dataListFinal.add(dataMap.get("33270207"));//实际值
                        dataListFinal.add(dataMap.get("33270208"));//设置阈值
                        dataListFinal.add(dataMap.get("33270209"));//容器名称
                        dataListFinal.add(dataMap.get(""));//应用名称
                        dataListFinal.add(dataMap.get(""));//服务名称
                        break;
                    default:break;
                }
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));

                CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
            }
        }
    }

    private static void getBluetoothDeviceData(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        for (MeterData meter : dataList) {
            List<Object> bluetoothDataList = new ArrayList<>();
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meter.getMeterData();
            if (doList != null && doList.size() > 0) {
                if (OopTmnlEventConcat.isAllNull(doList)) {
                    continue;
                }
                for (DataObject dataObj : doList) {
                    String dataItem = dataObj.getDataItem();
                    Object data = dataObj.getData();
                    if (data == null || "null".equals(data)) {
                        continue;
                    }
                    if (data instanceof List) {
                        bluetoothDataList = (List<Object>) data;
                    }
                }
            }

            TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meter.getMeterAddr());
            if (null == terminalArchivesObject) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                continue;
                //TODO 测试后删除
//                terminalArchivesObject = new TerminalArchivesObject();
//                terminalArchivesObject.setTerminalId("123123345");
//                terminalArchivesObject.setPowerUnitNumber("114018778907");
            }
            String tmnlId = terminalArchivesObject.getTerminalId();
            if (null == tmnlId || "".equals(tmnlId)) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                continue;
            }

            for (Object data : bluetoothDataList) {
                if (data instanceof List) {
                    List<String> bluetoothData = (List<String>) data;
                    List<Object> dataListFinal = new ArrayList<Object>();
                    dataListFinal.add(terminalArchivesObject.getID()); //MPED_ID
                    dataListFinal.add(terminalArchivesObject.getTerminalId()); //TERMINAL_ID
                    dataListFinal.addAll(bluetoothData);//MAC_ADDR 和 ASSET_NO
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataListFinal.add(new Date());

                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
                }
            }
        }
    }

    private static void getBlockTempData(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        for (MeterData meter : dataList) {
            List<Long> tempGroupList = new ArrayList<>();
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meter.getMeterData();
            if (doList != null && doList.size() > 0) {
                if (OopTmnlEventConcat.isAllNull(doList)) {
                    continue;
                }
                for (DataObject dataObj : doList) {
                    String dataItem = dataObj.getDataItem();
                    Object data = dataObj.getData();
                    if (data == null || "null".equals(data)) {
                        continue;
                    }
                    if (data instanceof List) {
                        List dList = (List)data;
                        for (Object d : dList) {
                            tempGroupList.add((Long) d);
                        }
                    }
                }
            }

            TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meter.getMeterAddr());
            if (null == terminalArchivesObject) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                continue;
                //TODO 测试后删除
//                terminalArchivesObject = new TerminalArchivesObject();
//                terminalArchivesObject.setTerminalId("123123345");
//                terminalArchivesObject.setPowerUnitNumber("114018778907");
            }
            String tmnlId = terminalArchivesObject.getTerminalId();
            if (null == tmnlId || "".equals(tmnlId)) {
                logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                continue;
            }

            List<Object> dataListFinal = new ArrayList<Object>();
            dataListFinal.add(terminalArchivesObject.getID()); //MPED_ID
            dataListFinal.add(terminalArchivesObject.getTerminalId()); //TERMINAL_ID
            dataListFinal.add(Arrays.toString(tempGroupList.toArray())); //TEMP_GROUP
            dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
            dataListFinal.add(new Date());

            CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
        }
    }

    /**
     * 电压电流数据
     * @author wjj
     * @date 2022/8/19 17:35
     * @return
     */
    private static void getVoltageOrCurrentData(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        for (MeterData meter : dataList) {
            List<Object> mpDataList = new ArrayList<>();
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meter.getMeterData();
            if (doList != null && doList.size() > 0) {
                if (OopTmnlEventConcat.isAllNull(doList)) {
                    continue;
                }
                for (DataObject dataObj : doList) {
                    String dataItem = dataObj.getDataItem();
                    Object data = dataObj.getData();
                    if (data == null || "null".equals(data)) {
                        continue;
                    }
                    if (data instanceof List) {
                        mpDataList = (List<Object>) data;
                    }
                }

                TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meter.getMeterAddr());
                if (null == terminalArchivesObject) {
//                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
//                    continue;
                    //TODO 测试后删除
                    terminalArchivesObject = new TerminalArchivesObject();
                    terminalArchivesObject.setTerminalId("123123345");
                    terminalArchivesObject.setPowerUnitNumber("114018778907");
                    terminalArchivesObject.setID("111222333");
                }
                String tmnlId = terminalArchivesObject.getTerminalId();
                if (null == tmnlId || "".equals(tmnlId)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                    continue;
                }

                if(volAndCurSet.contains(codeMap.get(businessDataitemId))) {
                    List<Object> dataListFinal = new ArrayList<Object>();
                    dataListFinal.add(terminalArchivesObject.getID()); //ID
                    dataListFinal.add(DateUtil.formatDate(new Date(), 1)); //data_date

//                    if ("e_mp_inter_voltage_content_source".equals(codeMap.get(businessDataitemId))) {
//                        if ("20490200".equals(businessDataitemId)) dataListFinal.add(1); //电压A相
//                        else if("20490300".equals(businessDataitemId)) dataListFinal.add(2);//电压B相
//                        else if("20490400".equals(businessDataitemId)) dataListFinal.add(3);//电压C相
//                    } else if ("e_mp_inter_current_content_source".equals(codeMap.get(businessDataitemId))) {
//                        if ("204A0200".equals(businessDataitemId)) dataListFinal.add(1);//电流A相
//                        else if("204A0300".equals(businessDataitemId)) dataListFinal.add(2);//电流B相
//                        else if("204A0400".equals(businessDataitemId)) dataListFinal.add(3);//电流C相
//                    } else if ("e_mp_harmonic_voltage_content_source".equals(codeMap.get(businessDataitemId))) {
//                        if ("200D0200".equals(businessDataitemId)) dataListFinal.add(1); //电压A相
//                        else if("200D0300".equals(businessDataitemId)) dataListFinal.add(2);//电压B相
//                        else if("200D0400".equals(businessDataitemId)) dataListFinal.add(3);//电压C相
//                    } else if ("e_mp_harmonic_current_content_source".equals(codeMap.get(businessDataitemId))) {
//                        if("200E0200".equals(businessDataitemId)) dataListFinal.add(1);//电流A相
//                        else if("200E0300".equals(businessDataitemId)) dataListFinal.add(2);//电流B相
//                        else if("200E0400".equals(businessDataitemId)) dataListFinal.add(3);//电流C相
//                    }
                    String abcStr = businessDataitemId.substring(businessDataitemId.length()-3);
                    if ("200".equals(abcStr)) {
                        dataListFinal.add(1);//A相
                    } else if ("300".equals(abcStr)) {
                        dataListFinal.add(2);//B相
                    } else if ("400".equals(abcStr)) {
                        dataListFinal.add(3);//C相
                    } else {
                        dataListFinal.add("");//占位
                    }

                    dataListFinal.add(Arrays.toString(mpDataList.toArray()));//
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//org_no
                    dataListFinal.add("00");//status
                    dataListFinal.add("0");//data_src
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataListFinal.add(new Date());

                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
                } else if("e_mp_high_precision_current_unit_source".equals(codeMap.get(businessDataitemId))){
                    List<Object> dataListFinal = new ArrayList<>();
                    dataListFinal.add(terminalArchivesObject.getID()); //ID
                    dataListFinal.add(DateUtil.formatDate(new Date(), 1)); //data_date
                    dataListFinal.add(doList.get(1).getData());//CURRENT_UNIT
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//org_no
                    dataListFinal.add("00");//status
                    dataListFinal.add("0");//data_src
                    dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                    dataListFinal.add(new Date());

                    CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
                } else {
                    for (int i = 0; i < mpDataList.size(); i++) {
                        List<Object> dataListFinal = new ArrayList<Object>();
                        dataListFinal.add(terminalArchivesObject.getID()); //ID
                        dataListFinal.add(DateUtil.formatDate(new Date(), 1)); //data_date
                        dataListFinal.add(i+1);//phase_flag
                        if ("e_mp_voltage_fluctuation_frequency_source".equals(codeMap.get(businessDataitemId)) || "e_mp_voltage_fluctuation_source".equals(codeMap.get(businessDataitemId))) {
                            if (null != mpDataList.get(i)) {
                                dataListFinal.add(Arrays.toString(((List) mpDataList.get(i)).toArray()));//
                            } else {
                                dataListFinal.add("null");
                            }
                        } else {
                            dataListFinal.add(mpDataList.get(i));//phase_voltage
                        }
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber());//org_no
                        dataListFinal.add("00");//status
                        dataListFinal.add("0");//data_src
                        dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                        dataListFinal.add(new Date());

                        CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);
                    }
                }
            }
        }
    }

    private static void getMeterData(List<MeterData> dataList, String areaCode, String termAddr, String businessDataitemId,  List<com.tl.hades.persist.DataObject> listDataObj) throws Exception {
        for (MeterData meter : dataList) {
            List<com.tl.hades.objpro.api.beans.DataObject> doList = meter.getMeterData();
            if (doList != null && doList.size() > 0) {
                if (OopTmnlEventConcat.isAllNull(doList)) {
                    continue;
                }
                TerminalArchivesObject terminalArchivesObject = CommonUtils.getTerminalArchives(areaCode, termAddr, meter.getMeterAddr());
                if (null == terminalArchivesObject) {
//                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
//                    continue;
                    //TODO 测试后删除
                    terminalArchivesObject = new TerminalArchivesObject();
                    terminalArchivesObject.setID("111222");
                    terminalArchivesObject.setTerminalId("123123345");
                    terminalArchivesObject.setPowerUnitNumber("114018778907");
                }
                String tmnlId = terminalArchivesObject.getTerminalId();
                if (null == tmnlId || "".equals(tmnlId)) {
                    logger.error("无法从缓存获取正确的档案信息real:" + areaCode + "_" + termAddr);
                    continue;
                }

                List<Object> dataListFinal = new ArrayList<>();
                dataListFinal.add(terminalArchivesObject.getID()); //MPED_ID
                dataListFinal.add(terminalArchivesObject.getTerminalId()); //TERMINAL_ID
                for (int i = 1; i < doList.size(); i++) {
                    dataListFinal.add(doList.get(i).getData()); //FREQUENCY
                }
                dataListFinal.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
                dataListFinal.add(new Date());

                CommonUtils.putToDataHub(businessDataitemId, tmnlId, dataListFinal, null, listDataObj);

            }
        }
    }
}
