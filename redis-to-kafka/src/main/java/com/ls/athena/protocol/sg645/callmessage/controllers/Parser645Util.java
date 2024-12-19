package com.ls.athena.protocol.sg645.callmessage.controllers;

import com.ls.athena.callmessage.multi.util.StringCollectUtil;
import com.ls.athena.protocol.sg645.callmessage.controllers.comm.CommUtils;
import com.ls.athena.protocol.sg645.callmessage.controllers.comm.Data0001FF01;
import com.ls.pf.base.utils.tools.StringUtils;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class Parser645Util {
	public Parser645Util() {
		
	}
	
	/**
	 * <p>获取[当前正向有功电能示值]</p>
	 * @param index
	 * @param value	正向有功费率index电能示值
	 * @return
	 * @author 曾凡
	 * @time 2013-9-5 上午11:18:44
	 */
	private static void setDate200100(Data0001FF01 data200100, int index, BigDecimal value) {
		try {
			Method m = data200100.getClass().getMethod("setPapR" + index, BigDecimal.class);
			m.invoke(data200100, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void setDate200100(Map<Object, Object> dataMap, BigDecimal... values) {
		Data0001FF01 data200100 = new Data0001FF01();
		for(int i = 0; i < values.length; i++) {
			if(i == 0) {
				data200100.setPapR(values[i]);
			}else{
				setDate200100(data200100, i, values[i]);
			}
		}
		dataMap.put("DATA200100", data200100);
	}
	
	public static void process645Data(int ctrlWord, String command,	byte[] dataBytes, Map<Object, Object> dataMap) {
		if (ctrlWord == 0x81) { //97规约
			if (command.equals("9010")) { //97规约   正反向实时数据
				String data = CommUtils.byteToHexStringLH(dataBytes); 
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue + "");
			}
			if (command.equals("901F") || command.equals("911F") || command.equals("902F") || command.equals("912F") ||
				command.equals("941F") || command.equals("951F") || command.equals("942F") || command.equals("952F") ||
				command.equals("913F") || command.equals("915F") || command.equals("916F") || command.equals("914F")) {
				Data0001FF01 data200100 = new Data0001FF01();
				int rapNum = dataBytes.length / 4;
				for (int k = 0; k < rapNum; k++) {
					String data = CommUtils.byteToHexStringLH(dataBytes, k * 4,	4); 
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					if(k == 0){
						data200100.setPapR(new BigDecimal(data));
					}else{
						BigDecimal dataValue = new BigDecimal(data);
						setDate200100(data200100,k,dataValue);
					}
				}
				dataMap.put("DATA200100", data200100);
			} else if (command.equals("A010") || command.equals("A020") || command.equals("A110") || command.equals("A120")
					|| command.equals("B630") || command.equals("B631") || command.equals("B632") || command.equals("B633")) { 
				int pos = 0;
				String dataTotal = CommUtils.byteToHexStringLH(dataBytes, pos, 3);
				dataTotal = dataTotal.substring(0, 2) + "." + dataTotal.substring(2, dataTotal.length());
				pos += 3;
				BigDecimal T = new BigDecimal(dataTotal);
				dataMap.put("A0B63DataTotal", T);
			} else if (command.equals("B640") || command.equals("B641") || command.equals("B642") || command.equals("B643")) {
				int pos = 0;
				String dataTotal = CommUtils.byteToHexStringLH(dataBytes, pos, 2);
				dataTotal = dataTotal.substring(0, 2) + "." + dataTotal.substring(2, dataTotal.length());
				pos += 2;
				BigDecimal T = new BigDecimal(dataTotal);
				dataMap.put("B64DataTotal", T);
			} else if (command.equals("B611") || command.equals("B612") || command.equals("B613")) {
				int pos = 0;
				String dataTotal = CommUtils.byteToHexStringLH(dataBytes, pos, 2);
				pos += 2;
				BigDecimal T = new BigDecimal(dataTotal);
				dataMap.put("VoltDataTotal", T);
				/***********************************/
				//新增ABC项电流解析
			}else if(command.equals("B621") || command.equals("B622") || command.equals("B623")) {
				if(dataBytes.length < 4) {
					return;
				}
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2, true);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				dataMap.put("D1", data);
			}else if(command.equals("B650") || command.equals("B651") || command.equals("B652") || command.equals("B653")){
				if(dataBytes.length < 4) {
					return;
				}
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2, true);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				dataMap.put("D1", data);
			} else if (command.equals("A01F") || command.equals("A02F") || command.equals("A11F") || command.equals("A12F")
				||command.equals("A41F") || command.equals("A42F")||command.equals("A410") || command.equals("A420")){ //97规约正反向最大需量数值
				int pos = 0;
				String dataToto = CommUtils.byteToHexString(dataBytes, pos, 3);
				dataToto = dataToto.substring(0, dataToto.length() - 4) + "." + dataToto.substring(dataToto.length() - 4);
				pos += 3;
				
				String dataStr = "";
				for(int i = 1; i <= 4; i++) {
					if(dataBytes.length < i * 8) {
						return;
					}
					dataStr = CommUtils.byteToHexStringLH(dataBytes, pos, 3, true);
					dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
					BigDecimal T = new BigDecimal(dataStr);
					String tName = "T" + i;
					dataMap.put(tName, T);
					pos += 3;
				}
			} else if (command.equals("B01F") || command.equals("B02F") || command.equals("B11F") || command.equals("B12F")
					||command.equals("B41F") ||command.equals("B42F") ||command.equals("B410") ||command.equals("B420")) { //97规约正反向最大需量时间
				int pos = 0;
				String totoTime = CommUtils.byteToHexString(dataBytes, pos, 4);
				pos += 4;
				dataMap.put("TIME", totoTime);
				for(int i = 1; i <= 4; i++) {
					String time = CommUtils.byteToHexString(dataBytes, pos, 4);
					pos += 4;
					dataMap.put("TIME" + i, time);
				}
			} else if (command.equals("C020")) { //97规约电表运行状态字
				int state = (dataBytes[0] & 0xFF);
				int jdqzt = ((state >> 6) & 0x01);
				dataMap.put("JDQZT", jdqzt);
				if (jdqzt == 1)
					dataMap.put("JDQZT_TITLE", "parser.poweroff");
				else
					dataMap.put("JDQZT_TITLE", "parser.poweroff");
			} else if (command.equals("C010")) { //97规约电表日期(年月日)
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
				String date = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6);
				dataMap.put("D1", date);
			} else if (command.equals("C011")) { //97规约电表日期(时分秒) 
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				String time = data.substring(0, 2) + ":" + data.substring(2, 4)
						+ ":" + data.substring(4, 6);
				dataMap.put("", time);
			} else if (command.equals("C111")) { //97规约最大需量周期
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
				dataMap.put("item1", data);
			} 
		} else if (ctrlWord == 0x91) { //07规约
			if(command.equals("00000000") ) {
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
				data = data.substring(0, 6) + "." + data.substring(6);
				dataMap.put("D1", data);
			} 
			if(command.equals("03301201") ) {
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 4);
				byteCount += 4;
				dataMap.put("item2", data);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
				byteCount += 1;
				dataMap.put("item3", data);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 4);
				byteCount += 4;
				dataMap.put("item4", data);
			} 
			if(command.equals("03010000") || command.equals("03020000") || command.equals("03030000")
					|| command.equals("03040000") || command.equals("030B0000") || command.equals("030C0000")
					|| command.equals("030D0000") || command.equals("030E0000") || command.equals("030F0000")) {
				int pos = 0;
				for(int i = 0; i < 3; i++) {
					dataMap.put("item" + (i + 1), 
							Parser645UtilDataGrid.readSixComma(dataBytes, pos));
				}
			}else if ((command.equals("01010001")) || (command.equals("01020001")) || (command.equals("01030001")) || (command.equals("01040001"))){
		        String data = CommUtils.byteToHexStringLH(dataBytes);
		        
		        String vle = data.substring(0, 10);
		        String datt0 = data.substring(10, 12) + "." + data.substring(12);
		        dataMap.put("item1", datt0);
		        String datt1 = vle.substring(0, 2) + "-" + vle.substring(2, 4) + "-" + vle.substring(4, 6) + " " + vle.substring(6, 8) + ":" + vle.substring(8);
		        dataMap.put("item2", datt1);
		    }else if(command.equals("03050000") || command.equals("03060000")
					|| command.equals("03070000") || command.equals("03080000")
					|| command.equals("03090000") || command.equals("030A0000")) {
				int pos = 0;
				dataMap.put("D1", Parser645UtilDataGrid.readSixComma(dataBytes, pos));
			} else if(command.equals("03110000") || command.equals("03300000") || command.equals("03300100")
					|| command.equals("03300200") || command.equals("03300300") || command.equals("03300400")
					|| command.equals("03300500") || command.equals("03300600") || command.equals("03300700")
					|| command.equals("03300800") || command.equals("03300900") || command.equals("03300A00")
					|| command.equals("03300B00") || command.equals("03300C00") || command.equals("03300D00") || command.equals("03300E00")) {
				int pos = 0;
				dataMap.put("D1", Parser645UtilDataGrid.readThree(dataBytes, pos));
			} else if(command.equals("03120000")) {
				int pos = 0;
				for(int i = 0; i < 6; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (i + 1), 
							Parser645UtilDataGrid.readThree(dataBytes, pos));
				}
			} else if(OTREODOD.contains(command)) {
				int pos = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("item" + (i + 1), 
							Parser645UtilDataGrid.read6Date(dataBytes, pos));
				}
				for(int i = 0; i < 12; i++) {
					dataMap.put("item" + (i + 3), 
							Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
			} else if((command.startsWith("030101") || command.startsWith("030102")
					|| command.startsWith("030103") || command.startsWith("030201")
					|| command.startsWith("030202") || command.startsWith("030203")
					|| command.startsWith("030301") || command.startsWith("030302")
					|| command.startsWith("030303") || command.startsWith("030401")
					|| command.startsWith("030402") || command.startsWith("030403"))
					&& !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT"  + (counter + 1), 
							Parser645UtilDataGrid.read6Date(dataBytes, pos));
					counter++;
				}
				for(int i = 0; i < 8; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter + 1), 
							Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
					counter++;
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter + 1), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
					counter++;
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
				for(int i = 0; i < 4; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
				for(int i = 0; i < 4; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
				for(int i = 0; i < 4; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
			} else if(command.startsWith("030500") && !command.endsWith("00")) {
				int pos = 0;
				dataMap.put("GENERAL_METER_EVENT1", Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT2", Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT3", Parser645UtilDataGrid.read6Date(dataBytes, pos));
			}else if((command.startsWith("030700") || command.startsWith("030800")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				}
				for(int i = 0; i < 16; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
			} else if((command.startsWith("030900") || command.startsWith("030A00")
					|| command.startsWith("030E01") || command.startsWith("030E02")
					|| command.startsWith("030E03") || command.startsWith("030F01")
					|| command.startsWith("030F02") || command.startsWith("030F03")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTwoData(dataBytes, pos));
				for(int i = 0; i < 16; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
			} else if((command.startsWith("030B01") || command.startsWith("030B02")
					|| command.startsWith("030B03") || command.startsWith("030C01")
					|| command.startsWith("030C02") || command.startsWith("030C03")
					|| command.startsWith("030D01") || command.startsWith("030D02")
					|| command.startsWith("030D03")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				}
				for(int i = 0; i < 8; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes,pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes,pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
				for(int i = 0; i < 4; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
				for(int i = 0; i < 4; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1Data(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read3P1TwoData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readHalfTreData(dataBytes, pos));
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P3TwoData(dataBytes, pos));
			} else if((command.startsWith("031201") || command.startsWith("031202")
					|| command.startsWith("031203") || command.startsWith("031204")
					|| command.startsWith("031205") || command.startsWith("031206")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				for(int i = 0; i < 2; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				}
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read5Date(dataBytes, pos));
			} else if((command.startsWith("033001")) && !command.endsWith("00")) {
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
				byteCount += 4;
				dataMap.put("item2", data);
				
				for(int i = 0; i < 24; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(3+i), data);
				}
			} else if((command.startsWith("033002")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readFour(dataBytes, pos));
				for(int i = 0; i < 24; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read1P2TreData(dataBytes, pos));
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read5Date(dataBytes, pos));
				}
			} else if((command.startsWith("033003")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("item1", Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("item2", Parser645UtilDataGrid.readFour(dataBytes, pos));
				dataMap.put("item3", Parser645UtilDataGrid.readFour(dataBytes, pos));
			} else if((command.startsWith("033004")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("item1", Parser645UtilDataGrid.readFour(dataBytes, pos));
				dataMap.put("item2", Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("item3", Parser645UtilDataGrid.read6Date(dataBytes, pos));
			} else if((command.startsWith("033006") || command.startsWith("033005")) && !command.endsWith("00")) {
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 4);
				byteCount += 4;
				dataMap.put("item2", data);
				for(int i = 0; i < 28; i++) {
					String month = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
					String day= CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
					String num = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
					data=month+"月"+day+"日,"+"日时段表号"+num;
					byteCount += 3;
					dataMap.put("item"+(3+i), data);				
				}
			} else if((command.startsWith("033007") || command.startsWith("033009")
					|| command.startsWith("03300A") || command.startsWith("03300B")) && !command.endsWith("00")) {
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 4);
				byteCount += 4;
				dataMap.put("item2", data);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
				dataMap.put("item3", data);
			} else if((command.startsWith("033008")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readFour(dataBytes, pos));
				for(int i = 0; i < 254; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readFour(dataBytes, pos));
				}
			}else if((command.equals("03300C01"))) {
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 4);
				byteCount += 4;
				dataMap.put("item2", data);
			
				for (int i = 0; i < 3; i++) {
					String day = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
					String hour = CommUtils.byteToHexStringLH(dataBytes,byteCount, 1);
					data = day+"日"+hour+"时";
					byteCount += 2;
					dataMap.put("item"+(3+i), data);
					
				}
			}else if((command.startsWith("03300C")) && !command.endsWith("00")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readFour(dataBytes, pos));
				for(int i = 0; i < 3; i++) {
					dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readTwo(dataBytes, pos));
				}
			} else if(command.equals("04001104")) { //主动上报模式字
				String ori = StringUtils.encodeHex(dataBytes);
				try {
					int[] allData = Parser645UtilDataGrid.readAutoRepoNote(ori, 8);
					if(allData != null && allData.length > 0) {
						dataMap.put("AUTO_REPO", "8");
						for(int i = 0; i < allData.length; i++) {
							dataMap.put(Parser645UtilDataGrid.AUTO_REPO_MARKS.get(i), allData[i]);
						}
					}
				} catch (Exception e) {
					dataMap.put("ERROR", "error");
				} 
			} else if(command.equals("04001501")) {
				byte[] statNoteArr = new byte[12];
				byte[] numbersArr = new byte[dataBytes.length - 12];
				if(dataBytes.length >= 12) {
					System.arraycopy(dataBytes, 0, statNoteArr, 0, 12);
					System.arraycopy(dataBytes, 12, numbersArr, 0, dataBytes.length - 12);
					try {
						int[] allData = Parser645UtilDataGrid.readAutoRepoNote(StringUtils.encodeHex(statNoteArr), 12);
						if(allData != null && allData.length > 0) {
							dataMap.put("AUTO_REPO", "12");
							for(int i = 0; i < allData.length; i++) {
								dataMap.put(Parser645UtilDataGrid.AUTO_REPO_MARKS_OTHER.get(i), allData[i]);
							}
						}
						if(numbersArr != null && numbersArr.length > 0) {
							int ct = 1;
							for(byte b : numbersArr) {
								dataMap.put("AUTOREPO" + ct, StringUtils.encodeHex(new byte[]{b}));
								ct++;
							}
						}
					} catch (Exception e) {
						dataMap.put("ERROR", "error");
					}
				}
			} else if(command.equals("19010001") || command.equals("19020001")
					|| command.equals("19030001") || command.equals("10010001")
					|| command.equals("10020001") || command.equals("10030001") 
					|| command.equals("11010001") || command.equals("11020001")
					|| command.equals("11020002") || command.equals("11030002")
					|| command.equals("10000001") || command.equals("10000002")
					|| command.equals("13010001") || command.equals("13010002")
					|| command.equals("13020001") || command.equals("13020002")
					|| command.equals("13030001") || command.equals("13030002")
					|| command.equals("11010001") || command.equals("11010002")
					|| command.equals("11020001") || command.equals("11020002")
					|| command.equals("11030001") || command.equals("11030002")
					|| command.equals("10020002") || command.equals("21000000")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readThree(dataBytes, pos));
			} else if(command.equals("19010101") || command.equals("19012501")
					|| command.equals("19020101") || command.equals("19022501")
					|| command.equals("19030101") || command.equals("19022501")
					|| command.equals("10010101") || command.equals("10012501")
					|| command.equals("10020101") || command.equals("10022501")
					|| command.equals("10030101") || command.equals("10032501")
					|| command.equals("11010101") || command.equals("11012501")
					|| command.equals("11020101") || command.equals("11022501")
					|| command.equals("10000101") || command.equals("10000201")
					|| command.equals("11030101") || command.equals("11032501")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
			} else if(command.equals("12010001") || command.equals("12020001")
					|| command.equals("12030001") || command.equals("18010001")
					|| command.equals("18020001") || command.equals("18030001") 
					|| command.equals("1C010001") || command.equals("1C020001")
					|| command.equals("12020002") || command.equals("12030002")
					|| command.equals("03350000") || command.equals("03300100")
					|| command.equals("03370000") || command.equals("03300400")
					|| command.equals("03300400") || command.equals("03360000")
					|| command.equals("1C030001") || command.equals("12010002")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readThree(dataBytes, pos));
			} else if(command.equals("12010101") || command.equals("12012501")
					|| command.equals("12020101") || command.equals("12022501")
					|| command.equals("12030101") || command.equals("12022501")
					|| command.equals("18010101") || command.equals("18012501")
					|| command.equals("18020101") || command.equals("18022501")
					|| command.equals("18030101") || command.equals("18032501")
					|| command.equals("1C010101") || command.equals("1C012501")
					|| command.equals("1C020101") || command.equals("1C022501")
					|| command.equals("1C030101") || command.equals("1C032501")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
			} else if(command.equals("13010001") || command.equals("13020001")
					|| command.equals("13030001") || command.equals("1A010001")
					|| command.equals("1A020001") || command.equals("1A030001") 
					|| command.equals("1B010001") || command.equals("1B020001")
					|| command.equals("1B010002") || command.equals("1B020002")
					|| command.equals("1B030001") || command.equals("1B030002")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readThree(dataBytes, pos));
			} else if(command.equals("13010101") || command.equals("13012501")
					|| command.equals("13020101") || command.equals("13022501")
					|| command.equals("13030101") || command.equals("13022501")
					|| command.equals("1A010101") || command.equals("1A012501")
					|| command.equals("1A020101") || command.equals("1A022501")
					|| command.equals("1A030101") || command.equals("1A032501")
					|| command.equals("1B010101") || command.equals("1B012501")
					|| command.equals("1B020101") || command.equals("1B022501")
					|| command.equals("1B030101") || command.equals("1B032501")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
			} else if(command.equals("14000001") || command.equals("15000001")  || command.equals("21000001")
					|| command.equals("16000001") || command.equals("17000001") || command.equals("20000001")
					|| command.equals("16000002")
					|| command.equals("1F000001") || command.equals("1D000001") || command.equals("1E000001")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.readThree(dataBytes, pos));
			} else if(command.startsWith("210000") && !command.endsWith("00")){
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 1);
				byteCount += 1;
				dataMap.put("item2", data);
				
				for (int i = 1; i <17; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(2+i), data);
				}
			} else if(command.equals("14000101") || command.equals("14001201")
					|| command.equals("15000101") || command.equals("15001201")
					|| command.equals("16000101") || command.equals("16001301")
					|| command.equals("17000101") || command.equals("17001301")
					|| command.equals("1F000101") || command.equals("1F000601")
					|| command.equals("20000101") || command.equals("20001301")) {
				int pos = 0;
				int counter = 0;
				dataMap.put("GENERAL_METER_EVENT" + (counter++), Parser645UtilDataGrid.read6Date(dataBytes, pos));
			} else if (command.equals("1D000001") || command.equals("1E000001")
					|| command.equals("03020000") || command.equals("03030000")
					|| command.equals("030A0000") || command.equals("03300D00")
					|| command.equals("03300E00") || command.equals("1F000001")) { 
				//事件记录
				String dataValue = CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("EVENTDATAVALUE", Integer.toString(Integer.parseInt(dataValue)));
			} else if (command.equals("1F000002")) { 
				//功率因数超下限累计时间1F000002
				String dataValue = CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("EVENTDATATIME", Integer.toString(Integer.parseInt(dataValue)));
			} else if (command.equals("00010000") || command.equals("00150000")
					|| command.equals("00290000") || command.equals("003D0000")
					|| command.equals("00050000") || command.equals("00060000")
					|| command.equals("00070000") || command.equals("00080000")
					) { 
				//当前正向有功总电能00010000	当前A相正向有功电能00150000
				//当前B相正向有功电能00290000	当前C相正向有功电能003D0000
				String data = CommUtils.byteToHexStringLH(dataBytes); 
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue + "");
			} else if (command.equals("00160000") || command.equals("002A0000") || command.equals("003E0000")) {
				//当前A相反向有功电能00160000   当前B相反向有功电能002A0000   当前C相反向有功电能003E0000
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			} else if (command.equals("0205FF00")) {
				//瞬时视在功率数据块0205FF00
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataAll = "";
				String dataA = "";
				String dataB = "";
				String dataC = "";
				dataC = data.substring(0, 2) + "." + data.substring(2, 6);
				dataB = data.substring(6, 8) + "." + data.substring(8, 12);
				dataA = data.substring(12, 14) + "." + data.substring(14, 18);
				dataAll = data.substring(18, 20) + "." + data.substring(20, 24);
				BigDecimal dataValue_ALL = new BigDecimal(dataAll);
				BigDecimal dataValue_A = new BigDecimal(dataA);
				BigDecimal dataValue_B = new BigDecimal(dataB);
				BigDecimal dataValue_C = new BigDecimal(dataC);
				dataMap.put("D1", dataValue_ALL);
				dataMap.put("D2", dataValue_A);
				dataMap.put("D3", dataValue_B);
				dataMap.put("D4", dataValue_C);
			}else if (command.equals("0203FF00")) {
				//瞬时有功功率数据块0203FF00
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataAll = "";
				String dataA = "";
				String dataB = "";
				String dataC = "";
				dataC = data.substring(0, 2) + "." + data.substring(2, 6);
				dataB = data.substring(6, 8) + "." + data.substring(8, 12);
				dataA = data.substring(12, 14) + "." + data.substring(14, 18);
				dataAll = data.substring(18, 20) + "." + data.substring(20, 24);
				dataMap.put("D1", dataAll);
				dataMap.put("D2", dataA);
				dataMap.put("D3", dataB);
				dataMap.put("D4", dataC);
			}else if (command.startsWith("033600")) {
				//上1次负荷开关误动作记录内容
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataG = data.substring(0, 6) + "." + data.substring(6, 8);
				String dataF = data.substring(8, 14) + "." + data.substring(14, 16);
				String dataE = data.substring(16, 22) + "." + data.substring(22, 24);
				String dataD = data.substring(24, 30) + "." + data.substring(30, 32);
				String dataC = data.substring(32, 34);
				
				String date1 = "20" + data.substring(34, 36) + "-" + data.substring(36, 38) + "-" + data.substring(38, 40);
				String time1 = " "+data.substring(40, 42) + ":" + data.substring(42, 44) + ":" + data.substring(44, 46);
				String dataB = date1+time1;
				
				String date2 = "20" + data.substring(46, 48) + "-" + data.substring(48, 50) + "-" + data.substring(50, 52);
				String time2 = " "+data.substring(52, 54) + ":" + data.substring(54, 56) + ":" + data.substring(56, 58);
				String dataA = date2+time2;
				dataMap.put("item1", dataA);
				dataMap.put("item2", dataB);
				dataMap.put("item3", dataC);
				dataMap.put("item4", dataD);
				dataMap.put("item5", dataE);
				dataMap.put("item6", dataF);
				dataMap.put("item7", dataG);

			}else if (command.startsWith("033500") && !command.endsWith("00")) {
				//上1次恒定磁场干扰记录
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataF = data.substring(0, 6) + "." + data.substring(6, 8);
				String dataE = data.substring(8, 14) + "." + data.substring(14, 16);
				String dataD = data.substring(16, 22) + "." + data.substring(22, 24);
				String dataC = data.substring(24, 30) + "." + data.substring(30, 32);
				
				String date1 = "20" + data.substring(32, 34) + "-" + data.substring(34, 36) + "-" + data.substring(36, 38);
				String time1 = " "+data.substring(38, 40) + ":" + data.substring(40, 42) + ":" + data.substring(42, 44);
				String dataB = date1+time1;
				
				String date2 = "20" + data.substring(44, 46) + "-" + data.substring(46, 48) + "-" + data.substring(50, 52);
				String time2 = " "+data.substring(52, 54) + ":" + data.substring(54, 56) + ":" + data.substring(56, 58);
				String dataA = date2+time2;
				
				dataMap.put("item1", dataA);
				dataMap.put("item2", dataB);
				dataMap.put("item3", dataC);
				dataMap.put("item4", dataD);
				dataMap.put("item5", dataE);
				dataMap.put("item6", dataF);

			}else if (command.startsWith("033700")) {
				//上1次电源异常记录内容
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataD = data.substring(0, 6) + "." + data.substring(6, 8);
				String dataC = data.substring(8, 14) + "." + data.substring(14, 16);
				
				String date1 = "20" + data.substring(16, 18) + "-" + data.substring(18, 20) + "-" + data.substring(20, 22);
				String time1 = " "+data.substring(22, 24) + ":" + data.substring(24, 26) + ":" + data.substring(26, 28);
				String dataB = date1+time1;
				
				String date2 = "20" + data.substring(28, 30) + "-" + data.substring(30, 32) + "-" + data.substring(32, 34);
				String time2 = " "+data.substring(34, 36) + ":" + data.substring(36, 38) + ":" + data.substring(38, 40);
				String dataA = date2+time2;
				
				dataMap.put("item1", dataA);
				dataMap.put("item2", dataB);
				dataMap.put("item3", dataC);
				dataMap.put("item4", dataD);

			}else if (command.startsWith("1D00FF") || command.startsWith("1E00FF")) {
				//上n次跳闸 合闸
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String dataH =data.substring(0, 6) + "." + data.substring(6, 8);
				String dataG =data.substring(8, 14) + "." + data.substring(14, 16);
				String dataF =data.substring(16, 22) + "." + data.substring(22, 24);
				String dataE =data.substring(24, 30) + "." + data.substring(30, 32);
				String dataD =data.substring(32, 38) + "." + data.substring(38, 40);
				String dataC =data.substring(40, 46) + "." + data.substring(46, 48);
				String dataB = data.substring(48, 56) ;
				//String dataA = data.substring(56, 68) ;
				
				String date = "20" + data.substring(56, 58) + "-" + data.substring(58, 60) + "-" + data.substring(60, 62);
				String time = " "+data.substring(62, 64) + ":" + data.substring(64, 66) + ":" + data.substring(66, 68);
				String dataA = date+time;
				
				dataMap.put("item1", dataA);
				dataMap.put("item2", dataB);
				dataMap.put("item3", dataC);
				dataMap.put("item4", dataD);
				dataMap.put("item5", dataE);
				dataMap.put("item6", dataF);
				dataMap.put("item7", dataG);
				dataMap.put("item8", dataH);

			}else if (command.startsWith("1400FF") || command.startsWith("1B01FF") ||command.startsWith("1B02FF") ||command.startsWith("1B03FF")) {
				//电压逆相序记录
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				
				for (int i = 1; i <17; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(1+i), data);
				}
				
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time2 = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item18", time2);
				
				for (int i = 1; i <17; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(18+i), data);
				}
				
			}else if (command.startsWith("1901FF") || command.startsWith("1902FF") || command.startsWith("1903FF") ||
					  command.startsWith("1801FF") || command.startsWith("1802FF") || command.startsWith("1803FF")) {
				//失流记录   过流记录
				int byteCount = 0;
				String data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", time);
				//-------8  XXXXXX.XX------
				for (int i = 1; i < 9; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(i+1), data);
				}
				//-------1 XXX.X------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				byteCount += 2;
				dataMap.put("item10", data);
				//-------1个  XXX.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 3;
				dataMap.put("item11", data);
				
				//-------2个 XX.XXXX------
				for (int i = 1; i < 3; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
					data = data.substring(0, data.length() - 4) + "." + data.substring(data.length() - 4);
					byteCount += 3;
					dataMap.put("item"+(11+i), data);
				}
				//-------1个 X.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 2;
				dataMap.put("item14", data);
				//-------4个 XXXXXX.XX------
				for (int i = 1; i < 5; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(14+i), data);
				}
				//-------1个XXX.X------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				byteCount += 2;
				dataMap.put("item19", data);
				//-------1个XXX.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 3;
				dataMap.put("item20", data);
				//-------2个 XX.XXXX------
				for (int i = 1; i < 3; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
					data = data.substring(0, data.length() - 4) + "." + data.substring(data.length() - 4);
					byteCount += 3;
					dataMap.put("item"+(20+i), data);
				}
				//-------1个X.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 2;
				dataMap.put("item23", data);
				//-------4个XXXXXX.XX------
				for (int i = 1; i < 5; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(23+i), data);
				}
				
				//-------1个XXX.X------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				byteCount += 2;
				dataMap.put("item28", data);
				//-------1个XXX.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 3;
				dataMap.put("item29", data);
				//-------2个 XX.XXXX------
				for (int i = 1; i < 3; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 3);
					data = data.substring(0, data.length() - 4) + "." + data.substring(data.length() - 4);
					byteCount += 3;
					dataMap.put("item"+(29+i), data);
				}
				//-------1个X.XXX------
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 2);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				byteCount += 2;
				dataMap.put("item32", data);
				
				data = CommUtils.byteToHexStringLH(dataBytes,byteCount, 6);
				String time2 = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item33", time2);
				//----------16个XXXXXX.XX--------------
				for (int i = 1; i < 17; i++) {
					data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					byteCount += 4;
					dataMap.put("item"+(33+i), data);
				}
				
			}else if (command.equals("02800020") ||command.equals("02800021") ||command.equals("0280000B")) {
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0,4) + "." + data.substring(4,8);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			}else if(command.equals("02030000") || command.equals("02030100") || command.equals("02030200") || command.equals("02030300")
					|| command.equals("02040000") || command.equals("02040100") || command.equals("02040200") || command.equals("02040300")
					|| command.equals("02050000") || command.equals("02050100") || command.equals("02050200") || command.equals("02050300")) {
				//功率
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 4);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			} else if (command.equals("02800001")) {
				//零线电流
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			} else if(command.equals("02800002")) { 
				//电网频率
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			} else if(command.equals("02800003") || command.equals("02800004") || command.equals("02800005") || command.equals("02800006")) {
				//一分钟有功总平均功率 / 当前有功需量 /当前无功需量 /当前视在需量
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 4);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue);
			} else if (command.equals("01010000") || command.equals("01020000") || command.equals("01030000") ||command.equals("01040000")) {
				//当前正向有功总最大需量及发生时间01010000   当前反向有功总最大需量及发生时间01020000
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String data1 = data.substring(data.length() - 6,data.length() - 4) + "." + data.substring(data.length() - 4);
				try {
					dataMap.put("item1", new BigDecimal(data1) + "");
				} catch (Exception e) {
					dataMap.put("item1", data1);
				}
				String data2 = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" 
						+ data.substring(4, 6) + " " + data.substring(6, 8) + ":" + data.substring(8, 10);
				dataMap.put("item2", data2);
			} else if (command.equals("040005FF")) { 
				//电表运行状态字数据块   14个字节   2 * 7 = 2 + 2 + ... + 2
				int rs1 = dataBytes[0] & 0xFF; 
				String runstatus1 = Integer.toString(rs1);
				dataMap.put("RUNSTATUS1", runstatus1);
				for (int i = 1; i < 6; i++) {
					dataMap.put("RUNSTATUS1" + i, ((rs1 >> i) & 0x01) + "");
				}
				int rs2 = dataBytes[2] & 0xFF; 
				String runstatus2 = Integer.toString(rs2);
				dataMap.put("RUNSTATUS2", runstatus2);
				for (int i = 0; i < 7; i++) {
					if (i != 3) {
						dataMap.put("RUNSTATUS2" + i, ((rs2 >> i) & 0x01) + "");
					}
				}
				int rs3 = dataBytes[4] & 0xFF;
				String runstatus3 = Integer.toString(rs3);
				String ss = "";
				dataMap.put("RUNSTATUS3", runstatus3);
				for (int i = 0; i < 5; i++) {
					if (i == 1 || i == 2) {
						if (i == 1) {
							ss = ((rs3 >> i) & 0x01) + "";
						} else if (i == 2) {
							dataMap.put("RUNSTATUS3" + 2, ((rs3 >> i) & 0x01)+ ss);
						}
					} else {
						dataMap.put("RUNSTATUS3" + i, ((rs3 >> i) & 0x01) + "");
					}
				}
				int rs4 = dataBytes[6] & 0xFF;
				String runstatus4 = Integer.toString(rs4);
				dataMap.put("RUNSTATUS4", runstatus4);
				for (int i = 0; i < 8; i++) {
					dataMap.put("RUNSTATUS4" + i, ((rs4 >> i) & 0x01) + "");
				}
				dataMap.put("RUNSTATUS4" + 8, (((dataBytes[7] & 0xFF) >> 1) & 0x01) + "");
				int rs5 = dataBytes[8] & 0xFF;
				String runstatus5 = Integer.toString(rs5);
				dataMap.put("RUNSTATUS5", runstatus5);
				for (int i = 0; i < 8; i++) {
					dataMap.put("RUNSTATUS5" + i, ((rs5 >> i) & 0x01) + "");
				}
				dataMap.put("RUNSTATUS5" + 8, (((dataBytes[9] & 0xFF) >> 1) & 0x01) + "");
				int rs6 = dataBytes[10] & 0xFF;
				String runstatus6 = Integer.toString(rs6);
				dataMap.put("RUNSTATUS6", runstatus6);
				for (int i = 0; i < 8; i++) {
					dataMap.put("RUNSTATUS6" + i, ((rs6 >> i) & 0x01) + "");
				}
				dataMap.put("RUNSTATUS6" + 8,(((dataBytes[11] & 0xFF) >> 1) & 0x01) + "");
				int rs7 = dataBytes[12] & 0xFF; 
				String runstatus7 = Integer.toString(rs7);
				dataMap.put("RUNSTATUS7", runstatus7);
				for (int i = 0; i < 7; i++) {
					dataMap.put("RUNSTATUS7" + i, ((rs7 >> i) & 0x01) + "");
				}
			} else if (command.equals("04000601") || command.equals("04000602") || command.equals("04000603")) {
				//有功组合方式特征字04000601   无功组合方式1特征字04000602   无功组合方式2特征字04000603
				String data = CommUtils.byteToHexStringLH(dataBytes);
				int b = dataBytes[0] & 0xFF;
				if (command.equals("04000601")) {
					dataMap.put("COMBINATIONBYTE1", data);
					for (int i = 0; i < 4; i++) {
						dataMap.put("COMBINATIONBYTE1" + i, ((b >> i) & 0x01) + "");
					}
				} else if (command.equals("04000602")) {
					dataMap.put("COMBINATIONBYTE2", data);
					for (int i = 0; i < 8; i++) {
						dataMap.put("COMBINATIONBYTE2" + i, ((b >> i) & 0x01) + "");
					}
				} else if (command.equals("04000603")) {
					dataMap.put("COMBINATIONBYTE3", data);
					for (int i = 0; i < 8; i++) {
						dataMap.put("COMBINATIONBYTE3" + i, ((b >> i) & 0x01) + "");
					}
				}
			} else if (command.equals("04000901")) { 
				//负荷记录模式字
				int rs1 = dataBytes[0] & 0xFF;
				String loadProfileByte = Integer.toString(rs1);
				dataMap.put("LOADPROFILEBYTE", loadProfileByte);
				for (int i = 0; i < 8; i++) {
					dataMap.put("LOADPROFILEBYTE" + i, ((rs1 >> i) & 0x01) + "");
				}
			} else if (command.startsWith("0405")) {
				if(command.startsWith("040501FF") || command.startsWith("040502FF")) {
					String data = CommUtils.byteToHexStringLH(dataBytes);
					for (int i = 0; i < 4;i=i+1) {
						String a = data.substring(8*i,8+(8*i));
						a = a.substring(0, 4) + "." + a.substring(4, a.length());
						dataMap.put("D"+(i+1), a);
					}
				}else {
					String data = CommUtils.byteToHexStringLH(dataBytes);
					data = data.substring(0, 4) + "." + data.substring(4, data.length());
					dataMap.put("PAYRATEGRADE", data);
				}
				
			} else if (command.equals("02060000")||command.equals("02060100")||command.equals("02060200")||command.equals("02060300")) {
				if(dataBytes.length < 2) {
					return;
				} 
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = data.substring(0, data.length() - 3) + "." + data.substring(data.length()-3);
				try {
					dataMap.put("D1", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("D1", data + "");
				}
			}else if (command.startsWith("0406")) {
				String data = CommUtils.byteToHexStringLH(dataBytes);
				if(command.startsWith("040600") || command.startsWith("040602")) {
					data = data.substring(0, 6) + "." + data.substring(6, data.length());
					dataMap.put("LADDERVALUE", data);
				} else {
					data = data.substring(0, 4) + "." + data.substring(4, data.length());
					dataMap.put("LADDERPAYGRADE", data);
				}
			}else if(command.startsWith("0400040E")) {
				String data = CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("D1", data);
			} 
			else if(command.startsWith("040010")) {
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, 6) + "." + data.substring(6, data.length());
				dataMap.put("DEPOSITLIMITATION", data);
			} else if (command.equals("04090201") || command.equals("04090301")
					|| command.equals("04090303") || command.equals("04090601")
					|| command.equals("04090602") || command.equals("04090E01")
					|| command.equals("04090E02") || command.equals("04091001")
					|| command.equals("04091002") || command.equals("04091003")
					|| command.equals("04091101") || command.equals("04091102")
					|| command.equals("04091103") || command.equals("04091201")
					|| command.equals("04091202") || command.equals("04091203")
					|| command.equals("04091204") || command.equals("04092000")
					|| command.equals("04092001") || command.equals("04091104")
					|| command.equals("04091004")) {
				String data = CommUtils.byteToHexStringLH(dataBytes);
				if (command.equals("04090201")) { 
					//欠压事件电压触发上限
					data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
					try {
						dataMap.put("UNDERVTHH", new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("UNDERVTHH", data);
					}
				}
				if (command.equals("04090301")) { 
					//过压事件电压触发下限
					data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
					try {
						dataMap.put("OVERVTHH", new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("OVERVTHH", data);
					}
				}
				if (command.equals("04090601")) {
					//电流不平衡率限值
					data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
					dataMap.put("CURRENTUNBL", data);
				}
				if (command.equals("04090602")) { 
					//电流不平衡率判定延时时间
					try {
						dataMap.put("CURRENTUNBSP", new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("CURRENTUNBSP", data);
					}
				}
				if (command.equals("04090E01")) { 
					//总功率因数超下限阀值
					data = data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
					try {
						dataMap.put("PFLIMITTHH", new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("PFLIMITTHH", data);
					}
				}
				if (command.equals("04090E02")) { 
					//总功率因数超下限判定延时时间
					try {
						dataMap.put("PFLIMITSP", new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("PFLIMITSP", data);
					}
				}
			} else if (command.equals("00900200") || command.equals("00900201")) {
				//当前剩余电量00900200   当前透支电量00900201
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				try {
					dataMap.put("CREDITVALUE", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("CREDITVALUE", data);
				}
			}else if (command.indexOf("04000F") != -1){
		        String data = CommUtils.byteToHexStringLH(dataBytes);
		        data = data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		        try{
		          dataMap.put("LIMITVALUE", new BigDecimal(data));
		        }catch (Exception e){
		          dataMap.put("LIMITVALUE", data);
		        }
		    }else if (command.equals("04000103") || command.equals("04000104")) { 
				//最大需量周期04000103   滑差时间04000104
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String key = "";
				if (command.equals("04000103")) {
					key = "MDMPERIOD";
				} else if (command.equals("04000104")) {
					key = "MDMSLIDETIME";
				}
				try {
					dataMap.put(key, new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put(key, data);
				}
			} else if (command.startsWith("04000C")) {
				//0 - 9 级密码
				String data = CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("PASSWORD", data);
			} else if (command.equals("04001401")) {
				//跳闸延时时间(NNNN为跳闸前告警时间)(分钟)
				String data = CommUtils.byteToHexStringLH(dataBytes);
				try {
					dataMap.put("RELAYOFFDELAY", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("RELAYOFFDELAY", data);
				}
			} else if (command.equals("06000002") || command.endsWith("06000001")) {
				//负荷记录最近一个记录块06000002   给定时间记录块06000001
				int dataBytesLen = dataBytes.length;
				if (dataBytesLen < 2) {
					return;
				}
				int posIndex = 0;
				if (!(dataBytes[posIndex] == (byte) 0xA0 && dataBytes[posIndex + 1] == (byte) 0xA0)) {
					return; //第一 第二 个字节A0,E0表示数据不正确
				}
				posIndex += 2;
				int byteCount = dataBytes[2] & 0xFF; //负荷记录字节
				dataMap.put("LOADRECORDLENGTH", byteCount);
				posIndex += 1;
				// 5个字节的负荷记录存储时间
				String loadRecordTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // yyMMddHHmm
				loadRecordTime = "20" + loadRecordTime.substring(0, 2) + "-" + loadRecordTime.substring(2, 4) + "-"
						+ loadRecordTime.substring(4, 6) + " " + loadRecordTime.substring(6, 8) + ":" + loadRecordTime.substring(8);
				posIndex += 5;
				dataMap.put("LOADRECORDTIME", loadRecordTime);
				dataMap.put("V_A", "");
				dataMap.put("V_B", "");
				dataMap.put("V_C", "");
				dataMap.put("A_A", "");
				dataMap.put("A_B", "");
				dataMap.put("A_C", "");
				dataMap.put("FREQUENCY", "");
				if (posIndex + 17 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String V_a = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2); 
					V_a = V_a.substring(0, 3) + "." + V_a.substring(3);
					posIndex += 2;
					dataMap.put("V_A", V_a);
					String V_b = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);
					V_b = V_b.substring(0, 3) + "." + V_b.substring(3);
					posIndex += 2;
					dataMap.put("V_B", V_b);
					String V_c = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);
					V_c = V_c.substring(0, 3) + "." + V_c.substring(3);
					posIndex += 2;
					dataMap.put("V_C", V_c);
					String sign = CommUtils .getSignStr(dataBytes[posIndex + 3 - 1]);
					String A_a = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					A_a = sign + A_a.substring(0, 3) + "." + A_a.substring(3);
					posIndex += 3;
					dataMap.put("A_A", A_a);
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String A_b = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					A_b = sign + A_b.substring(0, 3) + "." + A_b.substring(3);
					posIndex += 3;
					dataMap.put("A_B", A_b);
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String A_c = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					A_c = sign + A_c.substring(0, 3) + "." + A_c.substring(3);
					posIndex += 3;
					dataMap.put("A_C", A_c);
					String frequency = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);
					frequency = frequency.substring(0, 2) + "." + frequency.substring(2);
					posIndex += 2;
					dataMap.put("FREQUENCY", frequency);
				}
				posIndex += 1; 
				if (posIndex + 24 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true); // NN.NNNN 有功总功
					power1 = sign + power1.substring(0, 2) + "." + power1.substring(2);
					posIndex += 3;
					BigDecimal dataValue = new BigDecimal(power1);
					dataMap.put("POWER1", dataValue + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power1_A = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power1_A = sign + power1_A.substring(0, 2) + "." + power1_A.substring(2);
					posIndex += 3;
					BigDecimal dataValue1 = new BigDecimal(power1_A);
					dataMap.put("POWER1_A", dataValue1 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power1_B = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power1_B = sign + power1_B.substring(0, 2) + "." + power1_B.substring(2);
					posIndex += 3;
					BigDecimal dataValue2 = new BigDecimal(power1_B);
					dataMap.put("POWER1_B", dataValue2 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power1_C = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power1_C = sign + power1_C.substring(0, 2) + "." + power1_C.substring(2);
					posIndex += 3;
					BigDecimal dataValue3 = new BigDecimal(power1_C);
					dataMap.put("POWER1_C", dataValue3 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power2 = sign + power2.substring(0, 2) + "." + power2.substring(2);
					posIndex += 3;
					BigDecimal dataValue4 = new BigDecimal(power2);
					dataMap.put("POWER2", dataValue4 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power2_A = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power2_A = sign + power2_A.substring(0, 2) + "." + power2_A.substring(2);
					posIndex += 3;
					BigDecimal dataValue5 = new BigDecimal(power2_A);
					dataMap.put("POWER2_A", dataValue5 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power2_B = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power2_B = sign + power2_B.substring(0, 2) + "." + power2_B.substring(2);
					posIndex += 3;
					BigDecimal dataValue6 = new BigDecimal(power2_B);
					dataMap.put("POWER2_B", dataValue6 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String power2_C = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true);
					power2_C = sign + power2_C.substring(0, 2) + "." + power2_C.substring(2);
					posIndex += 3;
					BigDecimal dataValue7 = new BigDecimal(power2_C);
					dataMap.put("POWER2_C", dataValue7 + "");
				}
				posIndex += 1; 
				if (posIndex + 8 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String sign = CommUtils.getSignStr(dataBytes[posIndex + 2 - 1]);
					String powerFactor = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2, true);
					powerFactor = sign + powerFactor.substring(0, 1) + "." + powerFactor.substring(1);
					posIndex += 2;
					dataMap.put("POWER_FACTOR", powerFactor);
					sign = CommUtils.getSignStr(dataBytes[posIndex + 2 - 1]);
					String powerFactorA = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2, true);
					powerFactorA = sign + powerFactorA.substring(0, 1) + "." + powerFactorA.substring(1);
					posIndex += 2;
					dataMap.put("POWER_FACTORA", powerFactorA);
					sign = CommUtils.getSignStr(dataBytes[posIndex + 2 - 1]);
					String powerFactorB = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2, true);
					powerFactorB = sign + powerFactorB.substring(0, 1) + "." + powerFactorB.substring(1);
					posIndex += 2;
					dataMap.put("POWER_FACTORB", powerFactorB);
					sign = CommUtils.getSignStr(dataBytes[posIndex + 2 - 1]);
					String powerFactorC = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2, true);
					powerFactorC = sign + powerFactorC.substring(0, 1) + "." + powerFactorC.substring(1);
					posIndex += 2;
					dataMap.put("POWER_FACTORC", powerFactorC);
				}

				posIndex += 1;
				if (posIndex + 16 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String energyActive1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4); // 正向有功
					energyActive1 = energyActive1.substring(0, 6) + "." + energyActive1.substring(6);
					posIndex += 4;
					BigDecimal dataValue = new BigDecimal(energyActive1);
					dataMap.put("ENERGY_ACTIVE1", dataValue + "");
					String energyActive2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4); // 反向向有
					energyActive2 = energyActive2.substring(0, 6) + "." + energyActive2.substring(6);
					posIndex += 4;
					BigDecimal dataValuea = new BigDecimal(energyActive2);
					dataMap.put("ENERGY_ACTIVE2", dataValuea + "");
					String sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActive1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 组合无功1
					energyInActive1 = sign + energyInActive1.substring(0, 6) + "." + energyInActive1.substring(6);
					posIndex += 4;
					BigDecimal dataValueb = new BigDecimal(energyInActive1);
					dataMap.put("ENERGY_INACTIVE1", dataValueb + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActive2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 组合无功2
					energyInActive2 = sign + energyInActive2.substring(0, 6) + "." + energyInActive2.substring(6);
					posIndex += 4;
					BigDecimal dataValuec = new BigDecimal(energyInActive2);
					dataMap.put("ENERGY_INACTIVE2", dataValuec + "");
				}
				posIndex += 1; 
				if (posIndex + 16 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActiveQ1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 象限1
					energyInActiveQ1 = sign + energyInActiveQ1.substring(0, 6) + "." + energyInActiveQ1.substring(6);
					posIndex += 4;
					BigDecimal dataValue = new BigDecimal(energyInActiveQ1);
					dataMap.put("ENERGY_INACTIVE_Q1", dataValue + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActiveQ2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 象限2
					energyInActiveQ2 = sign + energyInActiveQ2.substring(0, 6) + "." + energyInActiveQ2.substring(6);
					posIndex += 4;
					BigDecimal dataValue1 = new BigDecimal(energyInActiveQ2);
					dataMap.put("ENERGY_INACTIVE_Q2", dataValue1 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActiveQ3 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 象限3
					energyInActiveQ3 = sign + energyInActiveQ3.substring(0, 6) + "." + energyInActiveQ3.substring(6);
					posIndex += 4;
					BigDecimal dataValue2 = new BigDecimal(energyInActiveQ3);
					dataMap.put("ENERGY_INACTIVE_Q3", dataValue2 + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 4 - 1]);
					String energyInActiveQ4 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4, true); // 象限4
					energyInActiveQ4 = sign + energyInActiveQ4.substring(0, 6) + "." + energyInActiveQ4.substring(6);
					posIndex += 4;
					BigDecimal dataValue3 = new BigDecimal(energyInActiveQ4);
					dataMap.put("ENERGY_INACTIVE_Q4", dataValue3 + "");
				}
				posIndex += 1; 
				if (posIndex + 6 <= dataBytesLen && dataBytes[posIndex] != (byte) 0xAA) {
					String sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String demandActive = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true); // 有功
					demandActive = sign + demandActive.substring(0, 2) + "." + demandActive.substring(2);
					posIndex += 3;
					BigDecimal dataValue = new BigDecimal(demandActive);
					dataMap.put("DEMAND_ACTIVE", dataValue + "");
					sign = CommUtils.getSignStr(dataBytes[posIndex + 3 - 1]);
					String demandInActive = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true); // 无功
					demandInActive = sign + demandInActive.substring(0, 2) + "." + demandInActive.substring(2);
					posIndex += 3;
					BigDecimal dataValue1 = new BigDecimal(demandInActive);
					dataMap.put("DEMAND_INACTIVE", dataValue1 + "");
				}
			} else if (command.equals("0207FF00")||command.equals("02070100") || command.equals("02070200") || command.equals("02070300")) { 
				String data = CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, 3) + "." + data.substring(3);
				dataMap.put("PHASE_ANGLE", data);
			} else if (command.equals("03300100")) { 
				//清零次数
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				int times = Integer.parseInt(data);
				dataMap.put("CLEAR_TIMES", times + "");
			} else if(command.startsWith("050600")) { 
				//（上1次）日冻结时间
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String date = data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6);
				String time = data.substring(6, 8) + ":" + data.substring(8, data.length());
				dataMap.put("D1", date + " " + time);
			} else if (command.equals("0000FF00") || command.equals("0001FF00") 
					|| command.equals("0002FF00")|| command.indexOf("050601") != -1 
					|| command.indexOf("050602") != -1 || command.indexOf("050603") != -1
					|| command.indexOf("050604") != -1) {
				boolean checkSign = false; //是否有正负号
				String sign = "";
				if (command.equals("0000FF00")) {//组合有功无功要判断正负号
					checkSign = true;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[3]);
				}
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4,checkSign); 
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue + "");
				dataMap.put("T1", "");
				dataMap.put("T2", "");
				dataMap.put("T3", "");
				dataMap.put("T4", "");
				if (dataBytes.length < 8) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[7]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 4, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T1 = new BigDecimal(data);
				dataMap.put("T1", T1 + "");
				if (dataBytes.length < 12) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[11]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 8, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T2 = new BigDecimal(data);
				dataMap.put("T2", T2 + "");
				if (dataBytes.length < 16) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[15]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 12, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T3 = new BigDecimal(data);
				dataMap.put("T3", T3 + "");
				if (dataBytes.length < 20) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[19]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 16, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T4 = new BigDecimal(data);
				dataMap.put("T4", T4 + "");
				setDate200100(dataMap, dataValue, T1, T2, T3, T4);
			} else if (isCommand000XFFXX(command)){	
				get000xFFxx(ctrlWord, command, dataBytes, dataMap);
			} else if (command.equals("0003FF00") || command.equals("0004FF00")
					|| command.equals("0005FF00") || command.equals("0006FF00")
					|| command.equals("0007FF00") || command.equals("0008FF00")
					|| command.indexOf("050601") != -1) {
				boolean checkSign = false; 
				String sign = "";
				if (command.equals("0003FF00") || command.equals("0004FF00")) { //组合有功无功要判断正负号
					checkSign = true;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[3]);
				}
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue + "");
				dataMap.put("T1", "");
				dataMap.put("T2", "");
				dataMap.put("T3", "");
				dataMap.put("T4", "");
				if (dataBytes.length < 8) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[7]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 4, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T1 = new BigDecimal(data);
				dataMap.put("T1", T1 + "");
				if (dataBytes.length < 12) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[11]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 8, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T2 = new BigDecimal(data);
				dataMap.put("T2", T2 + "");
				if (dataBytes.length < 16) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[15]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 12, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T3 = new BigDecimal(data);
				dataMap.put("T3", T3 + "");
				if (dataBytes.length < 20) {
					return;
				}
				if (checkSign) {
					sign = CommUtils.getSignStr(dataBytes[19]);
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 16, 4, checkSign);
				data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
				BigDecimal T4 = new BigDecimal(data);
				dataMap.put("T4", T4 + "");
				setDate200100(dataMap,dataValue, T1, T2, T3, T4);
			} else if (command.startsWith("0101FF0") || command.startsWith("0102FF0")) { 
				//正(反)向有功总最大需量及发生时间(实时)
				int posIndex = 0;
				String dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4); //数据格式XX.XXXX
				BigDecimal dataVal = new BigDecimal(dataStr); 
				dataMap.put("DATAVALUE", dataVal);
				posIndex += 3;
				if(dataBytes.length < 8) {
					return;
				}
				dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3); 
				posIndex += 3;
				dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
				BigDecimal T1 = new BigDecimal(dataStr);
				dataMap.put("T1", T1);
				String time1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5);
				dataMap.put("TIME1", time1);
				posIndex += 5;
				if(dataBytes.length < 16) {
					return;
				}
				dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3); 
				posIndex += 3;
				dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
				BigDecimal T2 = new BigDecimal(dataStr);
				dataMap.put("T2", T2);
				String time2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5);
				dataMap.put("TIME2", time2);
				posIndex += 5;
				if(dataBytes.length < 24) {
					return;
				}
				dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3); 
				posIndex += 3;
				dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
				BigDecimal T3 = new BigDecimal(dataStr);
				dataMap.put("T3", T3);
				String time3 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5);
				dataMap.put("TIME3", time3);
				posIndex += 5;
				if(dataBytes.length < 32) {
					return;
				}
				dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3); 
				posIndex += 3;
				dataStr = dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
				BigDecimal T4 = new BigDecimal(dataStr);
				dataMap.put("T4", T4);
				String time4 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5);
				dataMap.put("TIME4", time4);
			} else if (command.startsWith("0103FF0") || command.startsWith("0104FF0")) { 
				//组合无功1,2(实时)
				int posIndex = 0;
				String sign = "";
//				System.out.println(StringUtils.encodeHex(new byte[] {dataBytes[2]}));
				sign = CommUtils.getSignStr(dataBytes[2]);
				String dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true); 
				posIndex += 3;
				dataStr = sign + dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
				BigDecimal dataVal = new BigDecimal(dataStr);
				dataMap.put("DATAVALUE", dataVal);
				for(int i = 1; i <= 4; i++) {
					if(dataBytes.length < i * 8) {
						return;
					}
					sign = CommUtils.getSignStr(dataBytes[posIndex + 2]); 
					dataStr = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3, true); 
					posIndex += 5;
					dataStr = sign + dataStr.substring(0, dataStr.length() - 4) + "." + dataStr.substring(dataStr.length() - 4);
					BigDecimal T = new BigDecimal(dataStr);
					String tName = "T" + i;
					dataMap.put(tName, T);
					String time = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5);
					String timeName = "TIME" + i;
					dataMap.put(timeName, time);
					posIndex += 3;
				}
			} else if (command.equals("04000501")||command.equals("04000502")||
					  command.equals("04000504")||command.equals("04000505")||
					  command.equals("04000506")||command.equals("04000507") ||
					  command.equals("04000503")) {
				
		        String str = StringCollectUtil.frontAppendByZero(Integer.toBinaryString(dataBytes[1] & 0xFF), 8);
		        str = str+StringCollectUtil.frontAppendByZero(Integer.toBinaryString(dataBytes[0] & 0xFF), 8);
		        dataMap.put("1", str);
				
			}else if (command.equals("0201FF00")) { 
				//三相电压
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("D1", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("D1", data + "");
				}
				if (dataBytes.length < 4) {
					return;
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 2, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("D2", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("D2", data + "");
				}
				if (dataBytes.length < 6) {
					return;
				}
				data = CommUtils.byteToHexStringLH(dataBytes, 4, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("D3", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("D3", data + "");
				}
			} else if (command.equals("02010100")) {
				if(dataBytes.length < 2) {
					return;
				} 
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("V_PHASEA", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("V_PHASEA", data + "");
				}
			} else if (command.equals("02010200")) {
				if(dataBytes.length < 2) {
					return;
				} 
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("V_PHASEB", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("V_PHASEB", data + "");
				}
			} else if (command.equals("02010300")) {
				if(dataBytes.length < 2) {
					return;
				} 
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = data.substring(0, data.length() - 1) + "." + data.substring(data.length() - 1);
				try {
					dataMap.put("D1", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("D1", data + "");
				}
			} else if (command.equals("0202FF00")) {
				//电流数据块
				dataMap.put("A_PHASEB", "");
				dataMap.put("A_PHASEC", "");
				String sign = CommUtils.getSignStr(dataBytes[2]);
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 3, true);
				data = sign + data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				try {
					dataMap.put("A_PHASEA", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("A_PHASEA", data + "");
				}
				if (dataBytes.length < 6) {
					return;
				}
				sign = CommUtils.getSignStr(dataBytes[5]);
				data = CommUtils.byteToHexStringLH(dataBytes, 3, 3, true);
				data = sign + data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				try {
					dataMap.put("A_PHASEB", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("A_PHASEB", data + "");
				}
				if (dataBytes.length < 9) {
					return;
				}
				sign = CommUtils.getSignStr(dataBytes[8]);
				data = CommUtils.byteToHexStringLH(dataBytes, 6, 3, true);
				data = sign + data.substring(0, data.length() - 3) + "." + data.substring(data.length() - 3);
				try {
					dataMap.put("A_PHASEC", new BigDecimal(data) + "");
				} catch (Exception e) {
					dataMap.put("A_PHASEC", data + "");
				}
			} else if ("02020100".equals(command) ||  "02020200".equals(command) || "02020300".equals(command)) {
				//A相电流 B相电流 C相电流
				String data = CommUtils.byteToHexStringLH(dataBytes);
				String data1 =data.substring(0, 3) + "." + data.substring(3, 6);
				dataMap.put("item1", data1);
			} else if (command.equals("04000101")) {
				//电表日期年月日周YYMMDDWW
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
				String date = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6);
				dataMap.put("D1", date);
			} else if (command.equals("04000102")) {
				//电表日期时间 hhmmss
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				String time = data.substring(0, 2) + ":" + data.substring(2, 4) + ":" + data.substring(4, 6);
				dataMap.put("", time);
			}  else if (command.equals("0400010C")) {
				//电表时间，YYMMDDWWhhmmss
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 7);
				String date = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6);
				String week=data.substring(6, 8);
				if("01".equals(week)){
					week = "星期一";
				}else if("02".equals(week)){
					week = "星期二";
				}else if("03".equals(week)){
					week = "星期三";
				}else if("04".equals(week)){
					week = "星期四";
				}else if("05".equals(week)){
					week = "星期五";
				}else if("06".equals(week)){
					week = "星期六";
				}else if("00".equals(week)){
					week = "星期日";
				}
				String time = data.substring(8, 10) + ":" + data.substring(10, 12) + ":" + data.substring(12, 14);
				String timer = date+week+time;
				dataMap.put("METER_TIMER", timer);
			} 
			else if (command.equals("04090B01")) {
				//过载事件有功功率触发下限NN.NNNN
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				String loadLimit = data.substring(0, 2) + "." + data.substring(2);
				try {
					dataMap.put("LOADLIMIT", new BigDecimal(loadLimit) + "");
				} catch (Exception e) {
					dataMap.put("LOADLIMIT", loadLimit);
				}
			} else if (command.equals("0206FF00")) { 
				//功率因数
				dataMap.put("PFACTOR_PHASEA", "");
				dataMap.put("PFACTOR_PHASEB", "");
				dataMap.put("PFACTOR_PHASEC", "");
				String sign = CommUtils.getSignStr(dataBytes[1]);
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2, true);
				data = sign + data.substring(0, 1) + "." + data.substring(1);
				dataMap.put("PFACTOR_TOTAL", data);
				if (dataBytes.length < 4) {
					return;
				}
				sign = CommUtils.getSignStr(dataBytes[3]);
				data = CommUtils.byteToHexStringLH(dataBytes, 2, 2, true);
				data = sign + data.substring(0, 1) + "." + data.substring(1);
				dataMap.put("PFACTOR_PHASEA", data);
				if (dataBytes.length < 6) {
					return;
				}
				sign = CommUtils.getSignStr(dataBytes[5]);
				data = CommUtils.byteToHexStringLH(dataBytes, 4, 2, true);
				data = sign + data.substring(0, 1) + "." + data.substring(1);
				dataMap.put("PFACTOR_PHASEB", data);
				if (dataBytes.length < 8) {
					return;
				}
				sign = CommUtils.getSignStr(dataBytes[7]);
				data = CommUtils.byteToHexStringLH(dataBytes, 6, 2, true);
				data = sign + data.substring(0, 1) + "." + data.substring(1);
				dataMap.put("PFACTOR_PHASEC", data);
			} else if (command.equals("1D000100")) { // 电表断闸次数
				int count = CommUtils.bytes2Int(dataBytes, 0, 3);
				dataMap.put("BREAKTIMES", count);
			} else if (command.indexOf("1D00FF") != -1 || command.indexOf("1E00FF") != -1) { 
				int byteCount = 0;
				//发生时刻 YYMMDDhhmmss 6
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 6);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("TIME", time);
				dataMap.put("BEGINTIME", time);
				//操作者代 C0C1C2C3 5
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
				byteCount += 4;
				dataMap.put("OPERATORID", data);
				//跳闸时正向有功电 XXXXXX.XX 4
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
				data = data.substring(0, 6) + "." + data.substring(6);
				byteCount += 4;
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("ZXYG", dataValue + "");

				//跳闸时反向有功电 XXXXXX.XX 4
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
				data = data.substring(0, 6) + "." + data.substring(6);
				byteCount += 4;
				BigDecimal dataValue1 = new BigDecimal(data);
				dataMap.put("FXYG", dataValue1 + "");
				//跳闸时第X象限无功总电 XXXXXX.XX 4
				if (dataBytes.length > (byteCount + 16)) {
					for (int i = 1; i <= 4; i++) {
						data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 4);
						data = data.substring(0, 6) + "." + data.substring(6);
						byteCount += 4;
						BigDecimal dataValue2 = new BigDecimal(data);
						dataMap.put("WG" + i, dataValue2 + "");
					}
				} else {
					dataMap.put("WG1", "");
					dataMap.put("WG2", "");
					dataMap.put("WG3", "");
					dataMap.put("WG4", "");
				}
			} else if ((command.startsWith("031100") ||command.startsWith("030600") && !command.endsWith("00"))) {
				int byteCount = 0;
				// 发生时刻 YYMMDDhhmmss 6
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 6);
				String beginTime = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item1", beginTime);
				// 结束时刻 YYMMDDhhmmss 6
				data = CommUtils.byteToHexStringLH(dataBytes, byteCount, 6);
				String endTime = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10) + ":" + data.substring(10, 12);
				byteCount += 6;
				dataMap.put("item2", endTime);
			} else if (command.equals("04000106") || command.equals("04000107")
					|| command.equals("04000108") || command.equals("04000109")) {
				// 年表切换时间 YYMMDDhhmm 5
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 5);
				String time = "20" + data.substring(0, 2) + "-" + data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8, 10);
				if (command.equals("04000106"))
					dataMap.put("YEARSWITCHTIME", time);
				else if (command.equals("04000107"))
					dataMap.put("DAYSWITCHTIME", time);
				else if (command.equals("04000108"))
					dataMap.put("PRICESWITCHTIME", time);
				else if (command.equals("04000109"))
					dataMap.put("LADDERSWITCHTIME", time);
			} else if (command.equals("04000201") || command.equals("04000202")
					|| command.equals("04000203") || command.equals("04000204")) { 
				//年表时区
				int sectionNum = dataBytes[0];
				String key = "";
				if (command.equals("04000201"))
					key = "SECTIONNUM";
				else if (command.equals("04000202"))
					key = "DAYTABLENUM";
				else if (command.equals("04000203"))
					key = "PERIODNUM";
				else if (command.equals("04000204"))
					key = "TXXNUM";
				dataMap.put(key, Integer.toHexString(sectionNum));
			} else if (command.equals("04000205")) { 
				//公共假日
				String holiday = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				dataMap.put("HOLIDAYNUM", Integer.parseInt(holiday) + "");
			} else if (command.equals("04000207")) { 
				//梯度数	
				int sectionNum = dataBytes[0];
				dataMap.put("LADDERRATE", Integer.toHexString(sectionNum));
			} else if (command.indexOf("040100") != -1 || command.indexOf("040200") != -1) {
				for (int i = 0; i < 14; i++) {
					String str = CommUtils.byteToHexStringLH(dataBytes, i * 3, 3); // hhmmNN
					dataMap.put("D" + (i+1), str);
					// 区别报文回复长度不同
					if (dataBytes.length - 3 * (i + 1) == 0) {
						break;
					}
				}
			} else if (command.indexOf("040300") != -1) {
				String str = CommUtils.byteToHexStringLH(dataBytes, 0, 4); // YYMMDDNN
				str = "20" + str.substring(0, 2) + "-" + str.substring(2, 4) + "-" + str.substring(4, 6) + " " + str.substring(6);
				if (str.indexOf("FF") != -1) str = "";
				dataMap.put("HOLIDAY", str);
			}else if ("04000304".equals(command)) { //显示功率（最大需量）小数位数
				String str = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
				int ctRatio = Integer.parseInt(str);
				dataMap.put("D1", ctRatio + "");
			}else if ("04000305".equals(command)) { //按键循环显示屏数
				String str = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
				int ctRatio = Integer.parseInt(str);
				dataMap.put("D2", ctRatio + "");
			}else if (command.indexOf("04000306") != -1) { //电流互感器变 NNNNNN
				String str = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				int ctRatio = Integer.parseInt(str);
				dataMap.put("CTRATIO", ctRatio + "");
			} else if (command.indexOf("04000307") != -1) { // 电压互感器变 NNNNNN
				String str = CommUtils.byteToHexStringLH(dataBytes, 0, 3);
				int ptRatio = Integer.parseInt(str);
				dataMap.put("PTRATIO", ptRatio + "");
			} else if (command.indexOf("040003") != -1){
		        String str = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
		        int ptRatio = Integer.parseInt(str);
		        dataMap.put("PTRATIO", ptRatio);
		    } else if (command.indexOf("04000801") != -1) { // 周休日特征字
				int mode = dataBytes[0] & 0xFF;
				String weekendMode = Integer.toString(mode);
				dataMap.put("WEEKENDMODE", weekendMode);
				for (int i = 0; i < 8; i++) {
					dataMap.put("WEEKENDMODE" + i, ((mode >> i) & 0x01) + "");
				}
			} else if (command.indexOf("04000802") != -1) { // 周休日采用的日时段表
				String d = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
				dataMap.put("WEEKENDDAYTABLE", d);
			} else if (command.indexOf("04001203") != -1) { // 日冻结时
				String time = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				time = time.substring(0, 2) + ":" + time.substring(2);
				dataMap.put("FREEZETIME", time);
			} else if (command.indexOf("04040104") != -1) { 
				String _data = CommUtils.byteToHexStringLH(dataBytes, 0, 5);
				dataMap.put("DISPLAY04", _data);
			}else if (command.indexOf("040401") != -1)
		      {
		        String _data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
		        String _data2 = CommUtils.byteToHexStringLH(dataBytes, 4, 1);
		        
		        dataMap.put("2", _data2);
		        dataMap.put("1", _data);
		      }
		      else if (command.indexOf("040402") != -1)
		      {
		        String _data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
		        String _data2 = CommUtils.byteToHexStringLH(dataBytes, 4, 1);
		        
		        dataMap.put("2", _data2);
		        dataMap.put("1", _data);
		      }
			
			else if (command.indexOf("04001001") != -1
					|| command.indexOf("04001002") != -1
					|| command.indexOf("04001003") != -1
					|| command.indexOf("04001004") != -1
					|| command.indexOf("04001005") != -1) { 
				// 报警金额1限 NNNNNN.NN
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
				String data1 = data.substring(0, 6);
				String data2 = data.substring(7);
				data = Integer.parseInt(data1) + "." + data2;
				String key = "AMOUNTALARM1";
				if (command.indexOf("04001002") != -1)
					key = "AMOUNTALARM2";
				else if (command.indexOf("04001003") != -1)
					key = "AMOUNTOVERDRAFT";
				else if (command.indexOf("04001004") != -1)
					key = "AMOUNTCORNER";
				else if (command.indexOf("04001005") != -1)
					key = "AMOUNTSWITCHON";
				dataMap.put(key, data);
			} else if (command.indexOf("04000B01") != -1) {
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				data = "sjzfparse.day" + data.substring(0, 2) + "       " + "sjzfparse.hour" + data.substring(2);
				dataMap.put("MONTHACCOUNTDAY1", data);
			} else if (command.equals("04000A01")) {
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4);
				data = data.substring(0, 2) + "-" + data.substring(2, 4) + " " + data.substring(4, 6) + ":" + data.substring(6);
				dataMap.put("LOADRECORDTIME", data);
			} else if (command.indexOf("04000A") != -1) {
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("LOADRECORDINTERVAL", dataValue + "");
			} else if (command.indexOf("030500") != -1) {
				String time1 = CommUtils.byteToHexStringLH(dataBytes, 0, 6);//YYMMddHHmmss
				String sign = CommUtils.getSignStr(dataBytes[8]);
				String A = CommUtils.byteToHexStringLH(dataBytes, 6, 3, true);
				String time2 = CommUtils.byteToHexStringLH(dataBytes, 9, 6);
				time1 = "20" + time1.substring(0, 2) + "-"
						+ time1.substring(2, 4) + "-" + time1.substring(4, 6)
						+ " " + time1.substring(6, 8) + ":"
						+ time1.substring(8, 10) + ":" + time1.substring(10);
				A = sign + A.substring(0, 3) + "." + A.substring(3);
				time2 = "20" + time2.substring(0, 2) + "-"
						+ time2.substring(2, 4) + "-" + time2.substring(4, 6)
						+ " " + time2.substring(6, 8) + ":"
						+ time2.substring(8, 10) + ":" + time2.substring(10);
				dataMap.put("VOLTAGEFOUL_TIME1", time1);
				dataMap.put("VOLTAGEFOUL_A", A);
				dataMap.put("VOLTAGEFOUL_TIME2", time2);
			} else if (command.indexOf("1001FF") != -1 || command.indexOf("1002FF") != -1  || command.indexOf("1003FF") != -1 ||
				      command.indexOf("1101FF") != -1  || command.indexOf("1102FF") != -1  ||command.indexOf("1103FF") != -1  ||
				      command.indexOf("1201FF") != -1  || command.indexOf("1202FF") != -1  ||command.indexOf("1203FF") != -1  ||
				      command.indexOf("1301FF") != -1  || command.indexOf("1302FF") != -1  ||command.indexOf("1303FF") != -1) {
				int _pos = 0;
				String time1 = CommUtils.byteToHexStringLH(dataBytes, _pos, 6); 
				_pos += 6;
				time1 = "20" + time1.substring(0, 2) + "-" + time1.substring(2, 4) + "-" + time1.substring(4, 6)
						+ " " + time1.substring(6, 8) + ":" + time1.substring(8, 10) + ":" + time1.substring(10);
				dataMap.put("VOLTAGEFOUL_TIME1", time1);
				_pos += 4 * 8;
				if (dataBytes.length < _pos + 2)
					return;
				String V = CommUtils.byteToHexStringLH(dataBytes, _pos, 2); // 发生时当前相电压
				_pos += 2;
				V = V.substring(0, 3) + "." + V.substring(3);
				dataMap.put("VOLTAGEFOUL_V", V);
				String sign = CommUtils.getSignStr(dataBytes[_pos + 3 - 1]);
				String A = CommUtils.byteToHexStringLH(dataBytes, _pos, 3); // 发生时当前相电流
				_pos += 3;
				A = sign + A.substring(0, 3) + "." + A.substring(3);
				dataMap.put("VOLTAGEFOUL_A", A);
				_pos += 82;
				if (dataBytes.length < _pos + 6)
					return;
				String time2 = CommUtils.byteToHexStringLH(dataBytes, _pos, 6); // 结束时刻
				time2 = "20" + time2.substring(0, 2) + "-" + time2.substring(2, 4) + "-" + time2.substring(4, 6)
						+ " " + time2.substring(6, 8) + ":" + time2.substring(8, 10) + ":" + time2.substring(10);
				dataMap.put("VOLTAGEFOUL_TIME2", time2);
			} else if (command.indexOf("1700FF") != -1 || command.indexOf("1600FF") != -1) {
				int _pos = 0;
				String time1 = CommUtils.byteToHexStringLH(dataBytes, _pos, 6); // 发生时刻
				_pos += 6;
				time1 = "20" + time1.substring(0, 2) + "-" + time1.substring(2, 4) + "-" + time1.substring(4, 6)
						+ " " + time1.substring(6, 8) + ":" + time1.substring(8, 10) + ":" + time1.substring(10);
				dataMap.put("CURRENTUNBALANCE_TIME1", time1);
				_pos += 67;
				if (dataBytes.length < _pos + 6)
					return;
				String time2 = CommUtils.byteToHexStringLH(dataBytes, _pos, 6); // 结束时刻
				time2 = "20" + time2.substring(0, 2) + "-" + time2.substring(2, 4) + "-" + time2.substring(4, 6)
						+ " " + time2.substring(6, 8) + ":" + time2.substring(8, 10) + ":" + time2.substring(10);
				dataMap.put("CURRENTUNBALANCE_TIME2", time2);
			} else if (command.indexOf("0400100A") != -1 || command.indexOf("0400100B") != -1) {// 短消
				String shortMessage = parseAsciiString(dataBytes);
				String xh = "1";
				if (command.indexOf("0400100B") != -1)
					xh = "2";
				dataMap.put("SHORTMESSAGE" + xh, shortMessage);
			} else if (command.equals("03330101") || command.equals("03320101")
					|| command.equals("0332010A") || command.equals("0333010A")) { //上一次购电日期
				String data = CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("PURCH_DATE", "20" + data.substring(0, 2) + "-"
						+ data.substring(2, 4) + "-" + data.substring(4, 6)
						+ " " + data.substring(6, 8) + ":" + data.substring(8));
			} else if (command.equals("03320201") || command.equals("0332020A")
					|| command.equals("03330201") || command.equals("0333020A")) {
				String data =  CommUtils.byteToHexStringLH(dataBytes);
				dataMap.put("PURCH_COUNT", Integer.valueOf(data) + "");
			} else if (command.equals("03320301") || command.equals("03320401")
					|| command.equals("03320501") || command.equals("03320601")
					|| command.equals("0332030A") || command.equals("0332040A")
					|| command.equals("0332050A") || command.equals("0332060A")
					|| command.equals("03330301") || command.equals("03330401")
					|| command.equals("03330501") || command.equals("03330601")
					|| command.equals("0333030A") || command.equals("0333040A")
					|| command.equals("0333050A") || command.equals("0333060A")) {
				String data =  CommUtils.byteToHexStringLH(dataBytes);
				data = data.substring(0, 6) + "." + data.substring(6, 8);
				dataMap.put("PURCH_ELECTRIC_RELA", data);
			} else if (command.indexOf("0333FF") != -1) { //购电记录
				int posIndex = 0;
				String time = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); //5个字 上N次购电时
				posIndex += 5;
				dataMap.put("LASTNTIME", "20" + time.substring(0, 2) + "-" + time.substring(2, 4) + "-" + time.substring(4, 6)
						+ " " + time.substring(6, 8) + ":" + time.substring(8));
				String totalNum = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);// 2个字 总购电次
				posIndex += 2;
				dataMap.put("TOTALNUM", totalNum);
				String tid = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);// 3个字
				posIndex += 3;
				// 3个字节为分钟,以基1993-01-01 00:00:00为基准的分钟数x
				// 解析结果显示为延后延后基准x分钟的时.
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				try {
					Date bd = sdf.parse("1993-01-01 00:00");
					long min = Long.parseLong(tid, 16);
					Date nd = new Date(bd.getTime() + min * 60 * 1000);
					dataMap.put("TID", sdf.format(nd));
				} catch (Exception e) {
					dataMap.put("TID", "");
				}
				String power = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 上N次购电电
				posIndex += 4;
				power = power.substring(0, 6) + "." + power.substring(6);
				BigDecimal dataValue = new BigDecimal(power);
				dataMap.put("LASTNPOWER", dataValue + "");
				String powerBefore = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 上N次购电前剩余电量
				posIndex += 4;
				powerBefore = powerBefore.substring(0, 6) + "." + powerBefore.substring(6);
				BigDecimal dataValue1 = new BigDecimal(powerBefore);
				dataMap.put("LASTNPOWERBEFORE", dataValue1 + "");
				String powerAfter = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 上N次购电后剩余电量
				posIndex += 4;
				powerAfter = powerAfter.substring(0, 6) + "." + powerAfter.substring(6);
				BigDecimal dataValue2 = new BigDecimal(powerBefore);
				dataMap.put("LASTNPOWERAFTER", dataValue2 + "");
				String powerAfterTotal = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 上N次购电后剩余总电
				posIndex += 4;
				powerAfterTotal = powerAfterTotal.substring(0, 6) + "." + powerAfterTotal.substring(6);
				BigDecimal dataValue3 = new BigDecimal(powerBefore);
				dataMap.put("LASTNPOWERTOTAL", dataValue3 + "");
			} else if (command.indexOf("030200") != -1) {// 欠压事件记录
				int posIndex = 0;
				String beginTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6); // 6个字 欠压始时
				posIndex += 6;
				dataMap.put("BEGINTIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				dataMap.put("TIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				String endTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6);// 6个字 欠压结束时间
				posIndex += 6;
				dataMap.put("ENDTIME", "20" + endTime.substring(0, 2) + "-" + endTime.substring(2, 4) + "-"
						+ endTime.substring(4, 6) + " " + endTime.substring(6, 8) + ":"
						+ endTime.substring(8, 10) + ":" + endTime.substring(10));
				String minVoltage = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);// 2个字 欠压过程中最低电
				posIndex += 2;
				dataMap.put("MINVOLTAGE", minVoltage.substring(0, 3) + "." + minVoltage.substring(3));
				String underVPhase = CommUtils.byteToHexStringLH(dataBytes, posIndex, 1);// 1个字 欠压相位
				posIndex += 1;
				dataMap.put("UNDERVPHASE", underVPhase);
			} else if (command.indexOf("030300") != -1) {// 过压事件记录
				int posIndex = 0;
				String beginTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6); // 6个字 过压始时
				posIndex += 6;
				dataMap.put("BEGINTIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				dataMap.put("TIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				String endTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6);// 6个字 过压结束时间
				posIndex += 6;
				dataMap.put("ENDTIME", "20" + endTime.substring(0, 2) + "-" + endTime.substring(2, 4) + "-"
						+ endTime.substring(4, 6) + " " + endTime.substring(6, 8) + ":"
						+ endTime.substring(8, 10) + ":" + endTime.substring(10));
				String maxVoltage = CommUtils.byteToHexStringLH(dataBytes, posIndex, 2);// 2个字 过压过程中最高电
				posIndex += 2;
				dataMap.put("MAXVOLTAGE", maxVoltage.substring(0, 3) + "." + maxVoltage.substring(3));
				String overVPhase = CommUtils.byteToHexStringLH(dataBytes, posIndex, 1);// 1个字 过压相位
				posIndex += 1;
				dataMap.put("OVERVPHASE", overVPhase);
			} else if (command.indexOf("030A00") != -1) {// 电流不平衡事件记
				int posIndex = 0;
				String beginTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6); // 6个字 始时
				posIndex += 6;
				dataMap.put("BEGINTIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				dataMap.put("TIME", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				String endTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6);// 6个字 结束时间
				posIndex += 6;
				dataMap.put("ENDTIME", "20" + endTime.substring(0, 2) + "-" + endTime.substring(2, 4) + "-"
						+ endTime.substring(4, 6) + " " + endTime.substring(6, 8) + ":"
						+ endTime.substring(8, 10) + ":" + endTime.substring(10));
			} else if (command.indexOf("033000") != -1) {// 编程事件记录
				int posIndex = 0;
				String programTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6); // 6个字 编程时间
				posIndex += 6;
				dataMap.put("item1", "20" + programTime.substring(0, 2) + "-" + programTime.substring(2, 4) + "-"
						+ programTime.substring(4, 6) + " " + programTime.substring(6, 8) + ":"
						+ programTime.substring(8, 10) + ":" + programTime.substring(10));
				String operatorId = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 操作者代
				posIndex += 4;
				dataMap.put("item2", operatorId);
				for (int i = 3; i < 13; i++) {
					String DI = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);// 4个字 编程的前10个DI
					posIndex += 4;
					dataMap.put("item" + i, DI);
				}
			} else if (command.indexOf("1F00FF") != -1) {// 总功率因素超下限事件记录
				int posIndex = 0;
				String beginTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6); // 6个字 始时
				posIndex += 6;
				dataMap.put("item1", "20" + beginTime.substring(0, 2) + "-" + beginTime.substring(2, 4) + "-"
						+ beginTime.substring(4, 6) + " " + beginTime.substring(6, 8) + ":"
						+ beginTime.substring(8, 10) + ":" + beginTime.substring(10));
				for (int i = 0; i < 4; i++) {
					String energy = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4); // 12个字**电能
					posIndex += 4;
					BigDecimal dataValue = new BigDecimal(energy);
					dataMap.put("item" + (i+2), dataValue + "");
				}
				String endTime = CommUtils.byteToHexStringLH(dataBytes, posIndex, 6);// 6个字 结束时间
				posIndex += 6;
				dataMap.put("item6", "20" + endTime.substring(0, 2) + "-" + endTime.substring(2, 4) + "-"
						+ endTime.substring(4, 6) + " " + endTime.substring(6, 8) + ":"
						+ endTime.substring(8, 10) + ":" + endTime.substring(10));
				for (int i = 0; i < 4; i++) {
					String energy = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4); // 16个字**电能
					posIndex += 4;
					BigDecimal dataValue = new BigDecimal(energy);
					dataMap.put("item" + (i+7), dataValue + "");
				}
			} else if (command.indexOf("02800102") != -1) {// 事件记录标志-2个字
				int curEventID = (dataBytes[0] & 0xFF) + ((dataBytes[1] & 0xFF) << 8);
				String str = Integer.toBinaryString(curEventID & 0xFFFF);
				str = "0000000000000000".substring(0, 16 - str.length()) + str;
				dataMap.put("curEventFlag", str);
			} else if (command.indexOf("0109FF00") != -1 || command.indexOf("010AFF00") != -1) {
				int posIndex = 0;
				// 正向视在大需量及发生时间
				String data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				posIndex += 3;
				data = data.substring(0, 2) + "." + data.substring(2);
				BigDecimal dataValue = new BigDecimal(data);
				dataMap.put("DATAVALUE", dataValue + "");
				String time = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // 5个字 时间
				posIndex += 5;
				dataMap.put("TIME", "20" + time.substring(0, 2) + "-" + time.substring(2, 4) + "-" + time.substring(4, 6)
						+ " " + time.substring(6, 8) + ":" + time.substring(8, 10));
				// 正向视在费率1大需量及发生时间
				if (dataBytes.length < 16)
					return;
				data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				posIndex += 3;
				data = data.substring(0, 2) + "." + data.substring(2);
				dataValue = new BigDecimal(data);
				dataMap.put("T1", dataValue + "");
				String time1 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // 5个字 时间
				posIndex += 5;
				dataMap.put("TIME1", "20" + time1.substring(0, 2) + "-"
						+ time1.substring(2, 4) + "-" + time1.substring(4, 6)
						+ " " + time1.substring(6, 8) + ":" + time1.substring(8, 10));
				// 正向视在费率2大需量及发生时间
				if (dataBytes.length < 24)
					return;
				data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				posIndex += 3;
				data = data.substring(0, 2) + "." + data.substring(2);
				dataValue = new BigDecimal(data);
				dataMap.put("T2", dataValue + "");
				String time2 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // 5个字 时间
				posIndex += 5;
				dataMap.put("TIME2", "20" + time2.substring(0, 2) + "-"
						+ time2.substring(2, 4) + "-" + time2.substring(4, 6)
						+ " " + time2.substring(6, 8) + ":" + time2.substring(8, 10));
				// 正向视在费率3大需量及发生时间
				if (dataBytes.length < 32)
					return;
				data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				posIndex += 3;
				data = data.substring(0, 2) + "." + data.substring(2);
				dataValue = new BigDecimal(data);
				dataMap.put("T3", dataValue + "");
				String time3 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // 5个字 时间
				posIndex += 5;
				dataMap.put("TIME3", "20" + time3.substring(0, 2) + "-" + time3.substring(2, 4) + "-" + time3.substring(4, 6)
						+ " " + time3.substring(6, 8) + ":" + time3.substring(8, 10));
				// 正向视在费率4大需量及发生时间
				if (dataBytes.length < 40)
					return;
				data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3);
				posIndex += 3;
				data = data.substring(0, 2) + "." + data.substring(2);
				dataValue = new BigDecimal(data);
				dataMap.put("T4", dataValue + "");
				String time4 = CommUtils.byteToHexStringLH(dataBytes, posIndex, 5); // 5个字 时间
				posIndex += 5;
				dataMap.put("TIME4", "20" + time4.substring(0, 2) + "-" + time4.substring(2, 4) + "-" + time4.substring(4, 6)
						+ " " + time4.substring(6, 8) + ":" + time4.substring(8, 10));
			} else if(command.equals("06000001") || command.equals("06010001") || command.equals("06020001") 
					|| command.equals("06030001") || command.equals("06040001") || command.equals("06050001") 
					|| command.equals("06060001")) {
				String time = CommUtils.byteToHexStringLH(dataBytes, 0, 5);
				dataMap.put("TIME", "20" + time.substring(0, 2) + "-" + time.substring(2, 4) + "-" + time.substring(4, 6)
						+ " " + time.substring(6, 8) + ":" + time.substring(8, 10));
			} else if(command.equals("06000000") || command.equals("06000002") || command.equals("06010000") || command.equals("06010002")
					|| command.equals("06020000") || command.equals("06020002") ||
					command.equals("06030000") || command.equals("06030002") ||
					command.equals("06040000") || command.equals("06040002") ||
					command.equals("06050000") || command.equals("06050002") ||
					command.equals("06060000") || command.equals("06060002") ) {
				String data = CommUtils.byteToHexStringLH(dataBytes, 0, 1);
				dataMap.put("T", data);
			}
		} else if (ctrlWord == 0xD1) { //电能表示值返回异常
			String error = CommUtils.byteToHexStringLH(dataBytes, 0, 1,false);
			dataMap.put("ERROR", error);
		} else if (ctrlWord == 0x9C) { //控制命令 跳合闸  正常应答
			dataMap.put("SECCESS", 1);
		} else if (ctrlWord == 0xDC) { //控制命令 跳合闸  异常应答
			String error = CommUtils.byteToHexStringLH(dataBytes, 0, 1,false);
			dataMap.put("ERROR", error);
		} else if (ctrlWord == 0xC3) { //身份认证异常应答
			String error = CommUtils.byteToHexStringLH(dataBytes, 0, 2,false);
			dataMap.put("ERROR", error);
		}else if (ctrlWord == 0xD4) { //写数据异常应答
			String error = CommUtils.byteToHexStringLH(dataBytes, 0, 1,false);
			dataMap.put("ERROR", error);
		} else if (ctrlWord == 0x83) { //身份认证正常应答
			if(command.equals("070000FF") || command.equals("07000001") || 
					command.equals("07000002") ||command.equals("07000003")) {//身份认证
				boolean checkSign = false; //是否有正负号
				String randomNum2 = CommUtils.byteToHexStringLH(dataBytes, 0, 4, checkSign); //随机数
				String easmNo = CommUtils.byteToHexStringLH(dataBytes, 4, 8, checkSign); //EASM序列号
				dataMap.put("RANDOMNUM", randomNum2);
				dataMap.put("EASMNO", easmNo);
			}
			
			if(command.equals("078102FF")) {//状态查询
				
				String leftMonery = CommUtils.bytes2Int(dataBytes, 0, 4)+"";
				String MAC1 = CommUtils.byteToHexStringLH(dataBytes, 4, 4); //MAC1
				String buyTimes = CommUtils.bytes2Int(dataBytes, 8, 4)+"";
				String MAC2 = CommUtils.byteToHexStringLH(dataBytes, 12, 4); //MAC2
				String userNo = CommUtils.byteToHexStringLH(dataBytes, 16, 6); //客户编号
				String secState = CommUtils.byteToHexStringLH(dataBytes, 22, 4); //客户编号
				
				dataMap.put("D1", new BigDecimal(leftMonery).divide(new BigDecimal(100)).toString());
				dataMap.put("D2", MAC1);
				dataMap.put("D3", buyTimes);
				dataMap.put("D4", MAC2);
				dataMap.put("D5", userNo);
				dataMap.put("D6", secState);
			}
			
		} else if (ctrlWord == 0x92) {
			if (command.indexOf("050620") != -1) {
				int posIndex = 0;
				String datadate = CommUtils.byteToHexStringLH(dataBytes, posIndex, 3); // 3个字节日期yymmdd
				posIndex += 3;
				dataMap.put("DATADATE", "20" + datadate.substring(0, 2) + "-"
						+ datadate.substring(2, 4) + "-" + datadate.substring(4, 6));
				posIndex += 1;
				for (int i = 0; i < 48; i++) {
					String data = CommUtils.byteToHexStringLH(dataBytes, posIndex, 4);
					posIndex += 4;
					data = data.substring(0, 6) + "." + data.substring(6);
					try {
						dataMap.put("DATAVALUE" + i, new BigDecimal(data) + "");
					} catch (Exception e) {
						dataMap.put("DATAVALUE" + i, data);
					}
				}
			}
		} else if (ctrlWord == 0x94) {
			if ("04001009".equals(command)){
	        byte byte1 = dataBytes[0];
	        byte byte2 = dataBytes[1];
	        String msg = "";
	        if (byte1 == 0)
	        {
	          msg = "Token Accept!";
	          if (byte2 == 0)
	          {
	            String str = CommUtils.byteToHexStringLH(dataBytes, 3, 4);
	            str = str.substring(0, 6) + "." + str.substring(6);
	            msg = msg + "<br>Charge amount:" + str;
	          }
	        }
	        else
	        {
	          switch (byte1)
	          {
	          case 1: 
	          case 2: 
	          case 6: 
	          case 8: 
	          case 9: 
	          case 10: 
	          case 11: 
	            msg = "Token Error!";
	            break;
	          case 3: 
	            msg = "Old Token!";
	            break;
	          case 4: 
	            msg = "Used Token!";
	            break;
	          case 5: 
	            msg = "Key Expired!";
	            break;
	          case 7: 
	            msg = "Credit Overflow!";
	            break;
	          case 12: 
	            msg = "Set first KCT!";
	            break;
	          case 13: 
	            msg = "Set second KCT!";
	          }
	        }
	        dataMap.put("STSMSG", msg);
	      }
		} else if (ctrlWord == 0x1F) {
			if (command.indexOf("99990001") != -1) {
				int onlineState = dataBytes[0] & 0xFF;
				dataMap.put("1F_ONLINESTATE", onlineState);
			} else if (command.indexOf("99990002") != -1) {
				int posCount = 0;
				int maxCount = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				int maxOnlineCount = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				int onlineClientCount = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				int maxOnlineClientCount = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				dataMap.put("TOTAL", maxCount);
				dataMap.put("MAXONLINECOUNT", maxOnlineCount);
				dataMap.put("ONLINECLIENTCOUNT", onlineClientCount);
				dataMap.put("MAXONLINECLIENTCOUNT", maxOnlineClientCount);
			} else if (command.indexOf("99990003") != -1) {
				int posCount = 0;
				int startSec, startMin, startHour, startDay, startMonth, startYear;
				int sec, min, hour, day, month, year;
				int almSec, almMin, almHour, almDay, almMonth, almYear;
				startSec = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				startMin = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				startHour = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				startDay = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				startMonth = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				startYear = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				String startTime = String.format("%d-%02d-%02d %02d:%02d:%02d",
						startYear, startMonth, startDay, startHour, startMin, startSec);
				sec = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				min = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				hour = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				day = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				month = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				year = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				String currentTime = String.format("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec);
				// 事件时间
				almSec = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				almMin = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				almHour = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				almDay = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				almMonth = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				almYear = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				String lastAlmTime = String.format("%d-%02d-%02d %02d:%02d:%02d", almYear, almMonth, almDay, almHour, almMin, almSec);
				dataMap.put("STARTTIME", startTime);
				dataMap.put("CURRENTTIME", currentTime);
				dataMap.put("LASTALMTIME", lastAlmTime);
			} else if (command.indexOf("99990004") != -1) {
				int posCount = 0;
				int onlineCount, currentCount;
				onlineCount = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				currentCount = CommUtils.bytes2Int(dataBytes, posCount, 1);
				posCount += 1;
				List<Map<Object, Object>> listMap = new ArrayList<Map<Object, Object>>();
				for (int i = 0; i < currentCount; i++) {
					String meterComId = "";
					meterComId = CommUtils.byteToHexStringLH(dataBytes, posCount, 6); // 行政区划
					posCount += 6;
					int heart = 0;
					heart = CommUtils.bytes2Int(dataBytes, posCount, 1);
					posCount += 1;
					int sec = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					int min = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					int hour = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					int day = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					int month = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					int year = CommUtils.bcd2Int(dataBytes[(posCount++)]);
					String lastTime = String.format("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec);
					String ip = CommUtils.bytesToOctetString(dataBytes, posCount, 4);
					posCount += 4;
					int port = CommUtils.bytes2Int(dataBytes, posCount, 2);
					posCount += 2;
					Map<Object, Object> map = new HashMap<Object, Object>();
					map.put("METERCOMID", meterComId);
					map.put("LASTTIME", lastTime);
					map.put("IP", ip);
					map.put("PORT", port);
					map.put("HEART", heart);
					listMap.add(map);
				}
				dataMap.put("TERMINALCOUNT", onlineCount);
				dataMap.put("CURRENTCOUNT", currentCount);
				dataMap.put("LISTMAP", listMap);
				// 自定义报文单个终端信
			} else if (command.indexOf("99990005") != -1) {
				int posCount = 0;
				int heart = dataBytes[(posCount++)] & 0xFF;
				int sec = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				int min = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				int hour = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				int day = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				int month = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				int year = CommUtils.bcd2Int(dataBytes[(posCount++)]);
				String lastTime = String.format("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec);
				String ip = CommUtils.bytesToOctetString(dataBytes, posCount, 4);
				posCount += 4;
				int port = CommUtils.bytes2Int(dataBytes, posCount, 2);
				posCount += 2;
				dataMap.put("HEART", heart);
				dataMap.put("IP", ip);
				dataMap.put("PORT", port);
				dataMap.put("LASTTIME", lastTime);
			} 
		}
	}
	
	/**
	 * <p>日冻结</p>
	 * @param ctrlWord
	 * @param command
	 * @param dataBytes
	 * @param dataMap
	 * @author 曾凡
	 * @time 2013-9-6 上午09:12:54
	 */
	private static void get000xFFxx(int ctrlWord, String command, byte[] dataBytes, Map<Object, Object> dataMap) {
		boolean checkSign = false; 
		String sign = "";
		if (checkSign) {
			sign = CommUtils.getSignStr(dataBytes[3]);
		}
		String data = CommUtils.byteToHexStringLH(dataBytes, 0, 4, checkSign); 
		data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		BigDecimal dataValue = new BigDecimal(data);
		dataMap.put("DATAVALUE", dataValue + "");
		if (dataBytes.length < 8) {
			return;
		}
		if (checkSign) {
			sign = CommUtils.getSignStr(dataBytes[7]);
		}
		data = CommUtils.byteToHexStringLH(dataBytes, 4, 4, checkSign);
		data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		BigDecimal T1 = new BigDecimal(data);
		if (dataBytes.length < 12) {
			return;
		}
		if (checkSign) {
			sign = CommUtils.getSignStr(dataBytes[11]);
		}
		data = CommUtils.byteToHexStringLH(dataBytes, 8, 4, checkSign);
		data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		BigDecimal T2 = new BigDecimal(data);
		if (dataBytes.length < 16) {
			return;
		}
		if (checkSign) {
			sign = CommUtils.getSignStr(dataBytes[15]);
		}
		data = CommUtils.byteToHexStringLH(dataBytes, 12, 4, checkSign);
		data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		BigDecimal T3 = new BigDecimal(data);
		if (dataBytes.length < 20) {
			return;
		}
		if (checkSign) {
			sign = CommUtils.getSignStr(dataBytes[19]);
		}
		data = CommUtils.byteToHexStringLH(dataBytes, 16, 4, checkSign);
		data = sign + data.substring(0, data.length() - 2) + "." + data.substring(data.length() - 2);
		BigDecimal T4 = new BigDecimal(data);
		setDate200100(dataMap, dataValue, T1, T2, T3, T4);
	}
	
	/**
	 * <p>判断日冻结的条件</p>
	 * @param command
	 * @return
	 * @author 曾凡
	 * @time 2013-9-6 上午09:50:04
	 */
	private static boolean isCommand000XFFXX(String command) {
		return command.matches("\\d{4}FF0[1-9a-fA-F]");
	}
	
	private static String parseAsciiString(byte[] asciiBytes) {
		String asciiString = new String(asciiBytes);
		asciiString = (asciiString == null) ? "NULL" : asciiString.trim();
		return asciiString;
	}
	
	private static final List<String> OTREODOD = new ArrayList<String>();
	static {
		OTREODOD.add("03300D01");
		OTREODOD.add("03300D02");
		OTREODOD.add("03300D03");
		OTREODOD.add("03300D04");
		OTREODOD.add("03300D05");
		OTREODOD.add("03300D06");
		OTREODOD.add("03300D07");
		OTREODOD.add("03300D08");
		OTREODOD.add("03300D09");
		OTREODOD.add("03300D0A");
		OTREODOD.add("03300E01");
		OTREODOD.add("03300E02");
		OTREODOD.add("03300E03");
		OTREODOD.add("03300E04");
		OTREODOD.add("03300E05");
		OTREODOD.add("03300E06");
		OTREODOD.add("03300E07");
		OTREODOD.add("03300E08");
		OTREODOD.add("03300E09");
		OTREODOD.add("03300E0A");
	}
	
	
	public static void main(String[] args) {
		//A4BD0B00 
		byte[] dataBytes = {(byte) 0xA4,(byte) 0XBD,0X0B,0X00};
		String data = CommUtils.byteToHexStringLH(dataBytes);
		System.out.println(data);
		
		System.out.println(new BigDecimal("769755").divide(new BigDecimal(100)).toString());
	}
}
