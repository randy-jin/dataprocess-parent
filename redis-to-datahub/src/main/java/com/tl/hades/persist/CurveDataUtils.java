package com.tl.hades.persist;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 批量曲线
 *
 * @author easb
 * dateMap1.put("00100200", "231005");	//日测量点总电能示值曲线		1
 * dateMap1.put("20000200", "232013");//日测量点电压曲线 1 2 3
 * dateMap1.put("20010200", "232016");//日测量点电流曲线 1 2 3
 * dateMap1.put("200A0200", "232020");//日测量点功率因数曲线 0 1 2 3
 * dateMap1.put("20040200", "232005");//日测量点功率曲线 1 2 3 4
 * dateMap1.put("20050200", "232009");//日测量点无功功率曲线 wu 5 6 7 8
 * oop-curve-dataprocessor.xml
 */
public class CurveDataUtils {

    //    <!-- 日测量点功率因数曲线 -->
//            <businessDataitemIds>232020,232021,232022,232023
//    <!-- 日测量点功率曲线 -->
//            <businessDataitemIds>232005,232006,232007,232008,232009,232010,232011,232012,70413,70414,70415,70416
//    <!-- 日测量点总电能示值曲线 -->
//            <businessDataitemIds>231005,231006,231007,231008,231010,231011,231012,231013,701102,701103,701104,701105
//    <!-- 日测量点电压曲线 -->
//            <businessDataitemIds>2002001,232013,232014,232015,70205,70206,70207
//    <!-- 日测量点电流曲线 -->
//            <businessDataitemIds>2003001,232016,232017,232018,232019,70306,70307,70308
    private static String[] curveItems = {
            "232020", "232021", "232022", "232023", "232005", "232006", "232007", "232008", "232009", "232010", "232011", "232012",
            "70413", "70414", "70415", "70416",
            "231005", "231006", "231007", "231008","231010", "231011", "231012", "231013",
            "2002001", "232013", "232014", "232015",
            "70205", "70206", "70207",
            "2003001", "232016", "232017", "232018", "232019",
            "70306", "70307", "70308",
            "701102","701103","701104","701105",
            "71405","71406","71407","71408"};

    public static List<String> curveList=Arrays.asList(curveItems);

    public static void main(String[] args) {
        System.out.println(curveList.contains("232007"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void addDataTypeByOop(List dataValue, String dataitem, int index) {
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
            case "231010"://象限shizhi
                dataValue.add(3);
                break;
            case "231011":
                dataValue.add(4);
                break;
            case "231012":
                dataValue.add(7);
                break;
            case "231013":
                dataValue.add(8);
                break;
            case "232009"://gonglv
                    dataValue.add(index + 5);
                break;
            case "232005":
                dataValue.add(index + 1);
                break;
            case "232020":
                dataValue.add(index);
                break;
            case "232019":
                dataValue.add(0);//零线电流
                break;
            default:
                dataValue.add(index + 1);
                break;
        }
    }

    public static void addMeterCurveTypeByOop(List dataValue, String dataitem) {
        switch (dataitem) {
            case "70205"://A
            case "70306"://A
            case "70413"://功率 总
            case "71406"://A
            case "701102"://z y
                dataValue.add(1);
                break;
            case "70206"://B
            case "70307"://B
            case "70414"://功率 A
            case "71407"://b
            case "701104"://z w
                dataValue.add(2);
                break;
            case "70207"://C
            case "70308"://C
            case "70415"://功率 b
            case "71408"://b
                dataValue.add(3);
                break;
            case "701103"://f y
                dataValue.add(5);
                break;
            case "701105":// f w
                dataValue.add(6);
                break;
            case "70416":// 功率 C
                dataValue.add(4);
                break;
            default:
                dataValue.add(0);


        }
    }
    public static int getDataPoint(int interval, int min) {
        return min / interval;
    }

    /**
     * 曲线数据冻结密度
     *
     * @param m
     * @return
     * @throws Exception
     */
    public static int getInterval(int m) throws Exception {
        // 数据密度
        int interval = 0;
        switch (m) {
            case 0:
                throw new RuntimeException("错误的冻结密度，无法运行");
            case 1:
                interval = 15;//96
                break;
            case 2:
                interval = 30;//48
                break;
            case 3:
                interval = 60;//24
                break;
            case 254:
                interval = 5;//288
                break;
            case 255:
                interval = 1;//1440
                break;
        }
        return interval;
    }

    public static String getType(String dataItemId){
        String type="0";
        switch (dataItemId) {
            case "71405"://功率因数
                type="0";
                break;
            case "71406":
                type="1";
                break;
            case "71407":
                type="2";
                break;
            case "71408":
                type="3";
                break;
            case "70205"://电压
                type="1";
                break;
            case "70206":
                type="2";
                break;
            case "70207":
                type="3";
                break;
            case "70306"://电流
                type="1";
                break;
            case "70307":
                type="2";
                break;
            case "70308":
                type="3";
                break;
            case "70413"://功率(负荷)
                type="1";
                break;
            case "70414":
                type="2";
                break;
            case "70415":
                type="3";
                break;
            case "70416":
                type="4";
                break;
            case "701102"://示值
                type="1";
                break;
            case "701103":
                type="5";
                break;
            case "701104":
                type="2";
                break;
            case "701105":
                type="6";
                break;
        }
        return type;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
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
    public static void addDataSrc(List dataValue, int fn,String dataSrc) {
        switch (fn) {
            case 92:// A相电流曲线e_mp_cur_curve
                dataValue.add(dataSrc);
                break;
            case 93:// B相电流曲线e_mp_cur_curve
                dataValue.add(dataSrc);
                break;
            case 94:// C相电流曲线e_mp_cur_curve
                dataValue.add(dataSrc);
                break;
            case 95:// 零序电流曲线e_mp_cur_curve
                dataValue.add(dataSrc);
                break;
            case 89:// A相电压曲线e_mp_vol_curve
                dataValue.add(dataSrc);
                break;
            case 90:// B相电压曲线e_mp_vol_curve
                dataValue.add(dataSrc);
                break;
            case 91:// C相电压曲线e_mp_vol_curve
                dataValue.add(dataSrc);
                break;
            case 105:// 总功率因数e_mp_factor_curve
                dataValue.add(dataSrc);
                break;
            case 106:// A相功率因数e_mp_factor_curve
                dataValue.add(dataSrc);
                break;
            case 107:// B相功率因数e_mp_factor_curve
                dataValue.add(dataSrc);
                break;
            case 108:// C相功率因数e_mp_factor_curve
                dataValue.add(dataSrc);
                break;
            case 81:// 有功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 82:// A相有功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 83:// B相有功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 84:// C相有功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 85:// 无功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 86:// A相无功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 87:// B相无功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 88:// C相无功功率曲线e_mp_power_curve
                dataValue.add(dataSrc);
                break;
            case 101:// 正向有功总电能示值e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 102:// 正向无功总电能示值e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 103:// 反向有功总电能示值e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 104:// 反向无功总电能示值e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 145:// 一象限无功总电能示值曲线e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 146:// 四象限无功总电能示值曲线e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 147:// 二象限无功总电能示值曲线e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 148:// 三象限无功总电能示值曲线e_mp_read_curve
                dataValue.add(dataSrc);
                break;
            case 96:// 剩余金额曲线  e_mp_balance_curve_ud_source
                dataValue.add(dataSrc);
                break;
            case 97:// z y电能量曲线e_mp_energy_curve
                dataValue.add(dataSrc);
                break;
            case 98:// 电能量曲线e_mp_energy_curve
                dataValue.add(dataSrc);
                break;
            case 99:// 电能量曲线e_mp_energy_curve
                dataValue.add(dataSrc);
                break;
            case 100:// 电能量曲线e_mp_energy_curve
                dataValue.add(dataSrc);
                break;
            default:
                break;
        }
    }


}
