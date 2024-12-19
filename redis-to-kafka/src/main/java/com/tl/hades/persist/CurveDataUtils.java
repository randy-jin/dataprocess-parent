package com.tl.hades.persist;

import java.util.Date;
import java.util.List;

/**
 * 批量曲线
 * @author easb
 * dateMap1.put("00100200", "231005");	//日测量点总电能示值曲线		1
 *	dateMap1.put("20000200", "232013");//日测量点电压曲线 1 2 3
 *	dateMap1.put("20010200", "232016");//日测量点电流曲线 1 2 3
 *	dateMap1.put("200A0200", "232020");//日测量点功率因数曲线 0 1 2 3 
 *	dateMap1.put("20040200", "232005");//日测量点功率曲线 1 2 3 4 
 *	dateMap1.put("20050200", "232009");//日测量点无功功率曲线 wu 5 6 7 8
 *	oop-curve-dataprocessor.xml
 */
public class CurveDataUtils {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void addDataTypeByOop(List dataValue, String dataitem,int index) {
		switch (dataitem) {
		case "231005"://shizhi
			dataValue.add(1);
			break;
		case "231006":
			dataValue.add(2);
			break;
		case "231007":
			dataValue.add(5);
			break;
		case "231008":
			dataValue.add(6);
			break;
		case "232009"://gonglv
			dataValue.add(index+5);
			break;
		case "232005":
			dataValue.add(index+1);
			break;
		case "232020":
			dataValue.add(index);
			break;
		default:
			dataValue.add(index+1);
			break;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void addDataType(List dataValue, int fn) {
		switch (fn) {
		case 92:// A相电流曲线e_mp_cur_curve
			dataValue.add(1);
			break;
		case 93:// B相电流曲线e_mp_cur_curve
			dataValue.add(2);
			break;
		case 94:// C相电流曲线e_mp_cur_curve
			dataValue.add(3);
			break;
		case 95:// 零序电流曲线e_mp_cur_curve
			dataValue.add(0);
			break;
		case 89:// A相电压曲线e_mp_vol_curve
			dataValue.add(1);
			break;
		case 90:// B相电压曲线e_mp_vol_curve
			dataValue.add(2);
			break;
		case 91:// C相电压曲线e_mp_vol_curve
			dataValue.add(3);
			break;
		case 73:// 总加组有功功率曲线e_total_power_curve
			dataValue.add(1);
			break;
		case 74:// 总加组无功功率曲线e_total_power_curve
			dataValue.add(2);
			break;
		case 75:// 总加组有功电能量曲线E_TOTAL_ENERGY_CURVE
			dataValue.add(1);
			break;
		case 76:// 总加组无功电能量曲线E_TOTAL_ENERGY_CURVE
			dataValue.add(2);
			break;
		case 138:// 直流模拟量数据曲线e_mp_analog_curve
//			dataValue.add(1);
			break;
		case 105:// 总功率因数e_mp_factor_curve
			dataValue.add(0);
			break;
		case 106:// A相功率因数e_mp_factor_curve
			dataValue.add(1);
			break;
		case 107:// B相功率因数e_mp_factor_curve
			dataValue.add(2);
			break;
		case 108:// C相功率因数e_mp_factor_curve
			dataValue.add(3);
			break;
		case 81:// 有功功率曲线e_mp_power_curve
			dataValue.add(1);
			break;
		case 82:// A相有功功率曲线e_mp_power_curve
			dataValue.add(2);
			break;
		case 83:// B相有功功率曲线e_mp_power_curve
			dataValue.add(3);
			break;
		case 84:// C相有功功率曲线e_mp_power_curve
			dataValue.add(4);
			break;
		case 85:// 无功功率曲线e_mp_power_curve
			dataValue.add(5);
			break;
		case 86:// A相无功功率曲线e_mp_power_curve
			dataValue.add(6);
			break;
		case 87:// B相无功功率曲线e_mp_power_curve
			dataValue.add(7);
			break;
		case 88:// C相无功功率曲线e_mp_power_curve
			dataValue.add(8);
			break;
		case 101:// 正向有功总电能示值e_mp_read_curve
			dataValue.add(1);
			break;
		case 102:// 正向无功总电能示值e_mp_read_curve
			dataValue.add(2);
			break;
		case 103:// 反向有功总电能示值e_mp_read_curve
			dataValue.add(5);
			break;
		case 104:// 反向无功总电能示值e_mp_read_curve
			dataValue.add(6);
			break;
		case 145:// 一象限无功总电能示值曲线e_mp_read_curve
			dataValue.add(3);
			break;
		case 146:// 四象限无功总电能示值曲线e_mp_read_curve
			dataValue.add(4);
			break;
		case 147:// 二象限无功总电能示值曲线e_mp_read_curve
			dataValue.add(7);
			break;
		case 148:// 三象限无功总电能示值曲线e_mp_read_curve
			dataValue.add(8);
			break;
		case 97:// 正向有功总电能量e_mp_energy_curve
			dataValue.add(1);
			break;
		case 98:// 正向无功总电能量e_mp_energy_curve
			dataValue.add(2);
			break;
		case 99:// 反向有功总电能量e_mp_energy_curve
			dataValue.add(5);
			break;
		case 100:// 反向无功总电能量e_mp_energy_curve
			dataValue.add(6);
			break;

		default:
			break;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static boolean allNull(List list) {
		boolean allNull = true;
		for (Object o : list) {
			if (o instanceof List) {
				allNull((List) o);
			} else {
				if (o != null && !(o instanceof Date)) {
					allNull = false;
					return allNull;
				}
			}
		}
		return allNull;
	}
}
