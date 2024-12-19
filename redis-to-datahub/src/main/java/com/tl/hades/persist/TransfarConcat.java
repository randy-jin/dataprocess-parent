package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.framew.terminal.archivesmanager.Protocol3761ArchivesObject;
import com.ls.athena.framew.terminal.archivesmanager.ProtocolArchives;
import com.ls.athena.framew.terminal.archivesmanager.TerminalArchivesObject;
import com.tl.dataprocess.param.ParamConstants;
import com.tl.easb.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.tl.hades.persist.CommonUtils.getArchives;

/**
 * @Author chendongwei
 * Date 2020/4/10 19:03
 * @Descrintion 3761
 */
public class TransfarConcat {

    private final static Logger logger = LoggerFactory.getLogger(TransfarConcat.class);

    private static Map<String, String> codeMap = new HashMap<>();

    private final static Map<String, Object> dateMap1 = new HashMap<>();
    private final static Map<String, Object> dateMap2 = new HashMap<>();
    private final static Map<String, Object> dateMap5 = new HashMap<>();
    private final static Map<String, Object> dateMap6 = new HashMap<>();

    static {
        dateMap1.put("0001FF00", 1);//正向有功
        dateMap1.put("05060101", 1);
        dateMap1.put("05060102", 2);
        dateMap1.put("05060103", 3);
        dateMap1.put("05060104", 4);
        dateMap1.put("05060105", 5);
        dateMap1.put("05060106", 6);
        dateMap1.put("05060107", 7);
        dateMap1.put("05060108", 8);
        dateMap1.put("05060109", 9);
        dateMap1.put("05060110", 10);
        dateMap1.put("0506010A", 10);

        dateMap2.put("0003FF00", 1);//正向无功
        dateMap2.put("00030000", 1);
        dateMap2.put("05060301", 1);
        dateMap2.put("05060302", 2);
        dateMap2.put("05060303", 3);
        dateMap2.put("05060304", 4);
        dateMap2.put("05060305", 5);
        dateMap2.put("05060306", 6);
        dateMap2.put("05060307", 7);
        dateMap2.put("05060308", 8);
        dateMap2.put("05060309", 9);
       // dateMap2.put("05060310", 10);
        dateMap2.put("0506030A", 10);
        dateMap2.put("0506030B",11);
        dateMap2.put("0506030C",12);
        dateMap2.put("0506030D",13);
        dateMap2.put("0506030E",14);
        dateMap2.put("0506030F",15);
        dateMap2.put("05060310",16);
        dateMap2.put("05060311",17);
        dateMap2.put("05060312",18);
        dateMap2.put("05060313",19);
        dateMap2.put("05060314",20);
        dateMap2.put("05060315",21);
        dateMap2.put("05060316",22);
        dateMap2.put("05060317",23);
        dateMap2.put("05060318",24);
        dateMap2.put("05060319",25);
        dateMap2.put("0506031A",26);
        dateMap2.put("0506031B",27);
        dateMap2.put("0506031C",28);
        dateMap2.put("0506031D",29);
        dateMap2.put("0506031E",30);
        dateMap2.put("0506031F",31);
        dateMap2.put("05060320",32);
        dateMap2.put("05060321",33);
        dateMap2.put("05060322",34);
        dateMap2.put("05060323",35);
        dateMap2.put("05060324",36);
        dateMap2.put("05060325",37);
        dateMap2.put("05060326",38);
        dateMap2.put("05060327",39);
        dateMap2.put("05060328",40);
        dateMap2.put("05060329",41);
        dateMap2.put("0506032A",42);
        dateMap2.put("0506032B",43);
        dateMap2.put("0506032C",44);
        dateMap2.put("0506032D",45);
        dateMap2.put("0506032E",46);
        dateMap2.put("0506032F",47);
        dateMap2.put("05060330",48);
        dateMap2.put("05060331",49);
        dateMap2.put("05060332",50);
        dateMap2.put("05060333",51);
        dateMap2.put("05060334",52);
        dateMap2.put("05060335",53);
        dateMap2.put("05060336",54);
        dateMap2.put("05060337",55);
        dateMap2.put("05060338",56);
        dateMap2.put("05060339",57);
        dateMap2.put("0506033A",58);
        dateMap2.put("0506033B",59);
        dateMap2.put("0506033C",60);
        dateMap2.put("0506033D",61);
        dateMap2.put("0506033E",62);

        dateMap5.put("0002FF00", 1);//反向有功
        dateMap5.put("05060201", 1);
        dateMap5.put("05060202", 2);
        dateMap5.put("05060203", 3);
        dateMap5.put("05060204", 4);
        dateMap5.put("05060205", 5);
        dateMap5.put("05060206", 6);
        dateMap5.put("05060207", 7);
        dateMap5.put("05060208", 8);
        dateMap5.put("05060209", 9);
      //  dateMap5.put("05060210", 10);
        dateMap5.put("0506020A", 10);
        dateMap5.put("0506020B",11);
        dateMap5.put("0506020C",12);
        dateMap5.put("0506020D",13);
        dateMap5.put("0506020E",14);
        dateMap5.put("0506020F",15);
        dateMap5.put("05060210",16);
        dateMap5.put("05060211",17);
        dateMap5.put("05060212",18);
        dateMap5.put("05060213",19);
        dateMap5.put("05060214",20);
        dateMap5.put("05060215",21);
        dateMap5.put("05060216",22);
        dateMap5.put("05060217",23);
        dateMap5.put("05060218",24);
        dateMap5.put("05060219",25);
        dateMap5.put("0506021A",26);
        dateMap5.put("0506021B",27);
        dateMap5.put("0506021C",28);
        dateMap5.put("0506021D",29);
        dateMap5.put("0506021E",30);
        dateMap5.put("0506021F",31);
        dateMap5.put("05060220",32);
        dateMap5.put("05060221",33);
        dateMap5.put("05060222",34);
        dateMap5.put("05060223",35);
        dateMap5.put("05060224",36);
        dateMap5.put("05060225",37);
        dateMap5.put("05060226",38);
        dateMap5.put("05060227",39);
        dateMap5.put("05060228",40);
        dateMap5.put("05060229",41);
        dateMap5.put("0506022A",42);
        dateMap5.put("0506022B",43);
        dateMap5.put("0506022C",44);
        dateMap5.put("0506022D",45);
        dateMap5.put("0506022E",46);
        dateMap5.put("0506022F",47);
        dateMap5.put("05060230",48);
        dateMap5.put("05060231",49);
        dateMap5.put("05060232",50);
        dateMap5.put("05060233",51);
        dateMap5.put("05060234",52);
        dateMap5.put("05060235",53);
        dateMap5.put("05060236",54);
        dateMap5.put("05060237",55);
        dateMap5.put("05060238",56);
        dateMap5.put("05060239",57);
        dateMap5.put("0506023A",58);
        dateMap5.put("0506023B",59);
        dateMap5.put("0506023C",60);
        dateMap5.put("0506023D",61);
        dateMap5.put("0506023E",62);

        dateMap6.put("0004FF00", 1);//反向无功
        dateMap6.put("05060401", 1);
        dateMap6.put("05060402", 2);
        dateMap6.put("05060403", 3);
        dateMap6.put("05060404", 4);
        dateMap6.put("05060405", 5);
        dateMap6.put("05060406", 6);
        dateMap6.put("05060407", 7);
        dateMap6.put("05060408", 8);
        dateMap6.put("05060409", 9);
//        dateMap6.put("05060410", 10);
        dateMap6.put("0506040A", 10);
        dateMap6.put("0506040B",11);
        dateMap6.put("0506040C",12);
        dateMap6.put("0506040D",13);
        dateMap6.put("0506040E",14);
        dateMap6.put("0506040F",15);
        dateMap6.put("05060410",16);
        dateMap6.put("05060411",17);
        dateMap6.put("05060412",18);
        dateMap6.put("05060413",19);
        dateMap6.put("05060414",20);
        dateMap6.put("05060415",21);
        dateMap6.put("05060416",22);
        dateMap6.put("05060417",23);
        dateMap6.put("05060418",24);
        dateMap6.put("05060419",25);
        dateMap6.put("0506041A",26);
        dateMap6.put("0506041B",27);
        dateMap6.put("0506041C",28);
        dateMap6.put("0506041D",29);
        dateMap6.put("0506041E",30);
        dateMap6.put("0506041F",31);
        dateMap6.put("05060420",32);
        dateMap6.put("05060421",33);
        dateMap6.put("05060422",34);
        dateMap6.put("05060423",35);
        dateMap6.put("05060424",36);
        dateMap6.put("05060425",37);
        dateMap6.put("05060426",38);
        dateMap6.put("05060427",39);
        dateMap6.put("05060428",40);
        dateMap6.put("05060429",41);
        dateMap6.put("0506042A",42);
        dateMap6.put("0506042B",43);
        dateMap6.put("0506042C",44);
        dateMap6.put("0506042D",45);
        dateMap6.put("0506042E",46);
        dateMap6.put("0506042F",47);
        dateMap6.put("05060430",48);
        dateMap6.put("05060431",49);
        dateMap6.put("05060432",50);
        dateMap6.put("05060433",51);
        dateMap6.put("05060434",52);
        dateMap6.put("05060435",53);
        dateMap6.put("05060436",54);
        dateMap6.put("05060437",55);
        dateMap6.put("05060438",56);
        dateMap6.put("05060439",57);
        dateMap6.put("0506043A",58);
        dateMap6.put("0506043B",59);
        dateMap6.put("0506043C",60);
        dateMap6.put("0506043D",61);
        dateMap6.put("0506043E",62);

    }

    static {
        codeMap.put("01010000", "E_MP_DAY_DEMAND_SOURCE");//直抄实时当前正向有功最大需量发生时间
        codeMap.put("01020000", "E_MP_DAY_DEMAND_SOURCE");//直抄实时当前反向有功最大需量发生时间
        codeMap.put("01030000", "E_MP_DAY_DEMAND_SOURCE");//直抄实时当前组合无功一需量发生时间
        codeMap.put("01040000", "E_MP_DAY_DEMAND_SOURCE");//直抄实时当前组合无功二需量发生时间

        codeMap.put("01010001", "E_MP_DAY_DEMAND_SOURCE");//直抄冻结正向有功最大需量发生时间
        codeMap.put("01020001", "E_MP_DAY_DEMAND_SOURCE");//直抄冻结反向有功最大需量发生时间
        codeMap.put("01030001", "E_MP_DAY_DEMAND_SOURCE");//直抄冻结组合无功一需量发生时间
        codeMap.put("01040001", "E_MP_DAY_DEMAND_SOURCE");//直抄冻结组合无功二需量发生时间

        codeMap.put("0101FF01", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF02", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF03", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF04", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF05", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF06", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF07", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF08", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF09", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF0A", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF0B", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间
        codeMap.put("0101FF0C", "E_MP_MON_DEMAND_SOURCE");//直抄表月冻结最大需量发生时间

        codeMap.put("04000501", "E_METER_RUN_STATUS_NUM1_SOURCE");//状态字一
        codeMap.put("04000503", "E_METER_RUN_STATUS_NUM3_SOURCE");//状态字三
        codeMap.put("03300D00", "E_EVENT_METER_TOTAL_NUM_SOURCE");//开表盖次数


        //全事件
        codeMap.put("03300D01", "E_METER_EVENT_OPEN_LID");//开表盖事件
        codeMap.put("03300D02", "E_METER_EVENT_OPEN_LID");//开表盖事件2
        codeMap.put("03300D03", "E_METER_EVENT_OPEN_LID");//开表盖事件3
        codeMap.put("03300D04", "E_METER_EVENT_OPEN_LID");//开表盖事件4
        codeMap.put("03300D05", "E_METER_EVENT_OPEN_LID");//开表盖事件5
        codeMap.put("03300D06", "E_METER_EVENT_OPEN_LID");//开表盖事件6
        codeMap.put("03300D07", "E_METER_EVENT_OPEN_LID");//开表盖事件7
        codeMap.put("03300D08", "E_METER_EVENT_OPEN_LID");//开表盖事件8
        codeMap.put("03300D09", "E_METER_EVENT_OPEN_LID");//开表盖事件9
        codeMap.put("03300D0A", "E_METER_EVENT_OPEN_LID");//开表盖事件10

        codeMap.put("1001FF01", "E_METER_EVENT_LOSE_VOL");//电能表失压事件记录
        codeMap.put("1002FF01", "E_METER_EVENT_LOSE_VOL");
        codeMap.put("1003FF01", "E_METER_EVENT_LOSE_VOL");
        codeMap.put("1101FF01", "E_METER_EVENT_LOW_VOL");//电能表欠压事件记录
        codeMap.put("1102FF01", "E_METER_EVENT_LOW_VOL");
        codeMap.put("1103FF01", "E_METER_EVENT_LOW_VOL");
        codeMap.put("1201FF01", "E_METER_EVENT_OVER_VOL");//电能表过压事件记录
        codeMap.put("1202FF01", "E_METER_EVENT_OVER_VOL");
        codeMap.put("1203FF01", "E_METER_EVENT_OVER_VOL");
        codeMap.put("1301FF01", "E_METER_EVENT_VOL_BREAK");//电能表断相事件记录
        codeMap.put("1302FF01", "E_METER_EVENT_VOL_BREAK");
        codeMap.put("1303FF01", "E_METER_EVENT_VOL_BREAK");
        codeMap.put("1C01FF01", "E_METER_EVENT_OVER_LOAD");//电能表负荷过载事件记录
        codeMap.put("1C02FF01", "E_METER_EVENT_OVER_LOAD");
        codeMap.put("1C03FF01", "E_METER_EVENT_OVER_LOAD");
        codeMap.put("1A01FF01", "E_METER_EVENT_NO_CURR");//电能表断流事件记录
        codeMap.put("1A02FF01", "E_METER_EVENT_NO_CURR");
        codeMap.put("1A03FF01", "E_METER_EVENT_NO_CURR");
        codeMap.put("1500FF01", "E_METER_EVENT_I_REVE_PHASE");//上一次电流逆相序记录
        codeMap.put("03300701", "E_METER_EVENT_WEEKDAY_PRO");//00
        codeMap.put("03300A01", "E_METER_EVENT_RQ_GROUP_PRO");//00
        codeMap.put("03300B01", "E_METER_EVENT_RQ_GROUP_PRO");
        codeMap.put("03300601", "E_METER_EVENT_TZONE_PRO");//上1次时区表编程记录
        codeMap.put("03300901", "E_METER_EVENT_AP_GROUP_PRO");//00
        codeMap.put("03300C01", "E_METER_EVENT_SETTLEDAY_PRO");//012 //上一次结算日编程记录
        codeMap.put("03300801", "E_METER_EVENT_HOLIDAY_PRO");//00

        codeMap.put("03300401", "E_METER_EVENT_CHEK_TIME");//00
        codeMap.put("03300001", "E_METER_EVENT_PROGRAM");//上一次编程记录
        codeMap.put("03301201", "E_METER_EVENT_ESAM_KEY_PRO");//上一次密钥更新记录内容
        codeMap.put("2000FF01", "E_METER_EVENT_CUR_HIGH_UNBALAN");//00

        codeMap.put("03110001", "E_MTER_EVENT_NO_POWER_SOURCE");//掉电事件
        codeMap.put("03110002", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110003", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110004", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110005", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110006", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110007", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110008", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("03110009", "E_MTER_EVENT_NO_POWER_SOURCE");
        codeMap.put("0311000A", "E_MTER_EVENT_NO_POWER_SOURCE");


        codeMap.put("03060001", "E_METER_EVENT_ASSI_POWER_SHUT");//上1次辅助电源失电

        codeMap.put("1E00FF01", "E_METER_EVENT_SWITCH_ON");//上一次合闸记录
        codeMap.put("1F00FF01", "E_METER_EVENT_FACT_LOWER_LIMIT");//上一次总功率因数超下限记录
        codeMap.put("1901FF01", "E_METER_EVENT_OVER_I");//上一次A B C过流记录
        codeMap.put("1902FF01", "E_METER_EVENT_OVER_I");
        codeMap.put("1903FF01", "E_METER_EVENT_OVER_I");
        codeMap.put("1D00FF01", "E_METER_EVENT_TRIP");//上一次跳闸记录
        codeMap.put("03360001", "E_METER_EVENT_SWITCH_ERR");//电能表负荷开关误动拒动
        codeMap.put("03370001", "E_METER_EVENT_POWER_ABN");//上一次电源异常记录内容
        codeMap.put("1400FF01", "E_METER_EVENT_REVE_PHASE");//上1次电压逆相序
        codeMap.put("1600FF01", "E_METER_EVENT_VOL_UNBALANCE");//上一次电压不平衡
        codeMap.put("03120201", "E_METER_EVENT_RAP_DEMD_OVER_LI");//上1次反向有功需量超限记录
        codeMap.put("03120101", "E_METER_EVENT_PAP_DEMD_OVER_LI");//上1次正向有功需量超限记录
        codeMap.put("1B01FF01", "E_METER_EVENT_P_RETURN");// 上1次A相功率反向
        codeMap.put("1B02FF01", "E_METER_EVENT_P_RETURN");
        codeMap.put("1B03FF01", "E_METER_EVENT_P_RETURN");
        codeMap.put("03350001", "E_METER_EVENT_MAG_INTERF");// 上1次恒定磁场干扰记录
        codeMap.put("03301001", "E_METER_EVENT_STAIR_PRO");// 上1次阶梯表编程记录内容
        codeMap.put("03301301", "E_METER_EVENT_INSERT_CARD");//上一次异常插卡记录
        codeMap.put("03300501", "E_METER_EVENT_TFRAME_PRO");// 上1次时段表编程记录内容
        codeMap.put("1700FF01", "E_METER_EVENT_CUR_UNBALANCE");//电能表电流不平衡事件记录
        codeMap.put("03340001", "E_METER_EVENT_BACK_FEE");//上1次退费记录内容
        codeMap.put("21000001", "E_METER_EVENT_CURR_RETURN");//上1次潮流反向记录内容
        codeMap.put("03300F01", "E_METER_EVENT_RATE_PRO");//上1次费率参数表编程记录内容
        codeMap.put("03300E01", "E_METER_EVENT_OPEN_TERM_LID");//上1次开端钮盒记录
        codeMap.put("03300101", "E_METER_EVENT_CLEAR");//电能表清零事件记录记录
        codeMap.put("03300301", "E_METER_EVENT_CLEAR_EVENT");//00上1次事件清零记录
        codeMap.put("03300201", "E_METER_EVENT_CLEAR_DEMAND");//电能表需量清零事件记录
        codeMap.put("03050001", "E_METER_EVENT_ALL_VOL_LOSE");//全失压

        codeMap.put("1801FF01", "E_METER_EVENT_LOSE_CURR");//写入datahub异常
        codeMap.put("1802FF01", "E_METER_EVENT_LOSE_CURR");
        codeMap.put("1803FF01", "E_METER_EVENT_LOSE_CURR");

        //风光路
        codeMap.put("02010100", "E_EDC_CONS_VOLT_SOURCE");
        codeMap.put("02010200", "E_EDC_CONS_VOLT_SOURCE");
        codeMap.put("02010300", "E_EDC_CONS_VOLT_SOURCE");
//        codeMap.put("03300D00", "E_EDC_CONS_OPENLID_SOURCE");
        codeMap.put("0201FF00", "E_EDC_CONS_VOLT_SOURCE");

        codeMap.put("0203FF00", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02030000", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02030100", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02030200", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02030300", "E_EDC_CONS_POWER_SOURCE");

        codeMap.put("0204FF00", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02040000", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02040100", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02040200", "E_EDC_CONS_POWER_SOURCE");
        codeMap.put("02040300", "E_EDC_CONS_POWER_SOURCE");

        codeMap.put("02800001", "E_EDC_CONS_CUR_SOURCE");//零序电流
        codeMap.put("0202FF00", "E_EDC_CONS_CUR_SOURCE");//电流
        codeMap.put("02020100", "E_EDC_CONS_CUR_SOURCE");
        codeMap.put("02020200", "E_EDC_CONS_CUR_SOURCE");
        codeMap.put("02020300", "E_EDC_CONS_CUR_SOURCE");

        codeMap.put("02060000", "E_EDC_CONS_FACTOR_SOURCE");//总功率
        codeMap.put("02060100", "E_EDC_CONS_FACTOR_SOURCE");
        codeMap.put("02060200", "E_EDC_CONS_FACTOR_SOURCE");
        codeMap.put("02060300", "E_EDC_CONS_FACTOR_SOURCE");


        codeMap.put("0207FF00", "E_EDC_CONS_PHASE_SOURCE");//相角数
        codeMap.put("02070100", "E_EDC_CONS_PHASE_SOURCE");//相角
        codeMap.put("02070200", "E_EDC_CONS_PHASE_SOURCE");
        codeMap.put("02070300", "E_EDC_CONS_PHASE_SOURCE");
        codeMap.put("0400010C", "E_METER_TIME_09_SOURCE");
        codeMap.put("04000101", "E_METER_TIME_09_SOURCE");
        codeMap.put("04000102", "E_METER_TIME_09_SOURCE");
        codeMap.put("04000103", "E_METER_TIME_09_SOURCE");
        //分时电价
        codeMap.put("04000106", "E_METER_TIME_DIVISION_SOURCE");//两套时区表切换时间
        codeMap.put("04000107", "E_METER_TIME_DIVISION_SOURCE");//两套时段表切换时间
        codeMap.put("04000201", "E_METER_COUNT_DIVISION_SOURCE");//年时区数
        codeMap.put("04000202", "E_METER_COUNT_DIVISION_SOURCE");//日时段表数
        codeMap.put("04010000", "E_METER_ZONE_DIVISION_SOURCE");//第一套时区表数据
        codeMap.put("04020000", "E_METER_ZONE_DIVISION_SOURCE");//第二套时区表数据
        codeMap.put("04010001", "E_METER_SECT_DIVISION_SOURCE");//第一套第1日时段表数据
        codeMap.put("04020001", "E_METER_SECT_DIVISION_SOURCE");//第二套第1日时段表数据

        codeMap.put("078102FF", "E_MPED_REAL_BUY_RECORD_SOURCE");//购电状态
        codeMap.put("00900201", "E_MPED_REAL_OVERDRAFT_SOURCE");//透支金额
        codeMap.put("00900200", "E_MPED_REAL_BUY_RECORD_SOURCE");//剩余金额
        //未测试

        //无
        //无


        //		codeMap.put("null","E_METER_EVENT_CLOCK_FAULT");
        //		codeMap.put("null","E_METER_EVENT_MEASURE_CHIP");
        //		codeMap.put("null","E_METER_EVENT_BUY_ENERGY");
        //		codeMap.put("03300201","E_METER_EVENT_RQ_DEMD_OVER_LIM");


    }


    public static void getDataList(TerminalDataObject terminalDataObject, int protocolId, List<DataObject> listDataObj, DataItemObject data, String sType) throws Exception {
        List dataList = data.getList();//本测量点的数据项xxx.xx...
        boolean allNull = CommonUtils.allNull(dataList);
        if (allNull) {//对每个数据监测，若为空或date或int，则list为空，执行下一个测量点
            return;
        }
        int fn = data.getFn();
        String meterTimebid = dataList.get(0).toString();
        if (Arrays.asList("04000101", "04000102", "04000103", "0400010C").contains(meterTimebid)) {
            String maddr = dataList.get(1).toString();
            Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, terminalDataObject.getAFN(), fn);
            TerminalArchivesObject tao = getArchives(terminalDataObject.getAreaCode(), terminalDataObject.getTerminalAddr(), maddr);
            if (tao == null) {
                return;
            }
            if ("04000103".equals(meterTimebid)) {
                meterTimebid = "0400010C";
            }
            String mpedIdStr = tao.getID();
            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, meterTimebid).getBusiDataItemId();
            BigDecimal mpedId = new BigDecimal(mpedIdStr);
            List dataListFinal = new ArrayList();

            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            dataListFinal.add(mpedId);
            String type = null;
            Object objlist = dataList.get(4);
            List timeList = null;
            if (objlist == null) {
                return;
            } else if (objlist instanceof List) {
                timeList = (List) objlist;
            }
            switch (meterTimebid) {
                case "04000101":
                    type = "01";
                    break;
                case "04000102":
                    type = "02";
                    break;
                case "04000103":
                case "0400010C":
                    type = "03";
                default:
            }
            dataListFinal.add(timeList.get(0));

            Date uptime = terminalDataObject.getUpTime();
            if (uptime == null) {
                uptime = new Date();
            }
            dataListFinal.add(uptime);
            dataListFinal.add(type);
            dataListFinal.add(new Date());//insert_time
            dataListFinal.add(tao.getPowerUnitNumber().substring(0, 5));//shard_no

            Object[] refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedId, "00000000000000", businessDataitemId, protocolId);
            CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataListFinal, refreshKey, listDataObj);
        } else if (Arrays.asList("02010100", "02010200", "02010300").contains(meterTimebid)) {//A相电压
            Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, terminalDataObject.getAFN(), fn);
            String meterAdd = dataList.get(1).toString();
            TerminalArchivesObject tao = getArchives(terminalDataObject.getAreaCode(), terminalDataObject.getTerminalAddr(), meterAdd);
            if (tao == null) {
                return;
            }
            String mpedIdStr = tao.getID();
            if (dataList.size() <= 4) {
                return;
            }
            List<Object> dlist = (List<Object>) dataList.get(4);
            if (dlist == null) {
                return;
            }

            String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, dataList.get(0).toString()).getBusiDataItemId();
            BigDecimal mpedId = new BigDecimal(mpedIdStr);
            List dataListFinal = new ArrayList();

            String date = dataList.get(3).toString();
            Date time;
            if (date == null) {
                time = new Date();
            } else {
                String year = DateUtil.getCurrentYYYYMM();
                time = DateUtil.parse(year + date, DateUtil.defaultDatePattern_YMDHMS);
            }
            dataListFinal.add(mpedId);
            dataListFinal.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
            String object = dataList.get(0).toString();
            switch (object) {
                case "02010100":
                    dataListFinal.add("1");
                    break;
                case "02010200":
                    dataListFinal.add("2");
                    break;
                case "02010300":
                    dataListFinal.add("3");
                    break;
                default:
            }
            dataListFinal.add(tao.getPowerUnitNumber());
            dataListFinal.add(dlist.get(0));
            dataListFinal.add(time);
            dataListFinal.add("00");
            if ("auto".equals(sType)) {
                dataListFinal.add("30");
            } else {
                dataListFinal.add("34");
            }
            dataListFinal.add(new Date());
            dataListFinal.add(tao.getPowerUnitNumber().substring(0, 5));

            Object[] refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
            CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataListFinal, refreshKey, listDataObj);
        } else {
            List<Object> data645List = new ArrayList<>();
            try {
                data645List = Split645DataPacket.split645Monitor(data);
            } catch (Exception e) {
                logger.error("无法解析成645" + data.getList());
            }
            if (data645List == null || data645List.size() == 0) {
                return;
            }

            String dataItemId = data645List.get(data645List.size() - 1).toString();

            String itemName = codeMap.get(dataItemId);

            String meterAdd = data645List.get(data645List.size() - 2).toString();
            TerminalArchivesObject tao = getArchives(terminalDataObject.getAreaCode(), terminalDataObject.getTerminalAddr(), meterAdd);
            if (tao == null) {
                return;
            }
            String meterId = tao.getMeterId();
            if (meterId == null || "".equals(meterId)) {
                return;
            }

            if (itemName != null && !"".equals(itemName)) {
                String businessDataitemId;
                LinkedList<Object> finalDataList = new LinkedList<>();
                finalDataList.add(new BigDecimal(Long.parseLong(meterId)));
                if (data645List.get(0).toString().contains("F") || data645List.get(0) == null) {
                    return;
                }
                Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, terminalDataObject.getAFN(), fn);
                businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, dataItemId).getBusiDataItemId();
                Object[] refreshKey = null;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                switch (itemName) {
                    case "E_EVENT_METER_TOTAL_NUM_SOURCE":
                        if (ParamConstants.startWith.equals("11")) {
                            finalDataList.add(new BigDecimal(tao.getID()));
                            finalDataList.add(businessDataitemId);//业务数据相id
                            finalDataList.add(tao.getTerminalId());//业务数据相id
                            finalDataList.add(data645List.get(0));//业务数据相id
                            finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                            finalDataList.add(new Date());
                            finalDataList.remove(0);
                            refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                            dataItemId = businessDataitemId;
                            break;
                        }else{
                            finalDataList.remove(0);
                            finalDataList.add(tao.getID());
                            finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                            finalDataList.add(tao.getPowerUnitNumber());
                            finalDataList.add(BigDecimal.valueOf(Long.parseLong(data645List.get(0).toString())));
                            finalDataList.add(new Date());
                            finalDataList.add("00");
                            if ("auto".equals(sType)) {
                                finalDataList.add("30");
                            } else {
                                finalDataList.add("34");
                            }
                            finalDataList.add(new Date());
                            finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                            refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                            dataItemId = businessDataitemId;
                            break;
                        }
                    case "E_METER_EVENT_OPEN_LID":
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(EventUtils.getDate(data645List, 0));
                        finalDataList.add(EventUtils.getDate(data645List, 1));
                        for (int i = 2; i < data645List.size() - 2; i++) {
                            Object v = data645List.get(i);
                            if (v != null && v.toString().contains("F")) {
                                v = null;
                            }
                            finalDataList.add(v);
                        }
                        finalDataList.add(0);
                        finalDataList.add("");
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_EDC_CONS_CUR_SOURCE":
                    case "E_EDC_CONS_PHASE_SOURCE":
                    case "E_EDC_CONS_FACTOR_SOURCE":
                    case "E_EDC_CONS_VOLT_SOURCE":
                    case "E_EDC_CONS_POWER_SOURCE":
                        LinkedList dataAbc = new LinkedList();
                        List result;
                        finalDataList.remove(0);
                        finalDataList.add(tao.getID());
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        switch (dataItemId) {
                            case "02800001":
                            case "02060000":
                                finalDataList.add("0");
                                break;
                            case "02020100":
                            case "02070100":
                            case "02060100":
                            case "02010100":
                            case "02030000":
                                finalDataList.add("1");
                                break;
                            case "02020200":
                            case "02070200":
                            case "02060200":
                            case "02010200":
                            case "02030100":
                                finalDataList.add("2");
                                break;
                            case "02020300":
                            case "02070300":
                            case "02060300":
                            case "02010300":
                            case "02030200":
                                finalDataList.add("3");
                                break;
                            case "02030300":
                                finalDataList.add("4");
                                break;
                            case "02040000":
                                finalDataList.add("5");
                                break;
                            case "02040100":
                                finalDataList.add("6");
                                break;
                            case "02040200":
                                finalDataList.add("7");
                                break;
                            case "02040300":
                                finalDataList.add("8");
                                break;
                            default:
                                finalDataList.add("");
                        }
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(Double.parseDouble(data645List.get(0).toString()));
                        finalDataList.add(new Date());
                        finalDataList.add("00");
                        if ("auto".equals(sType)) {
                            finalDataList.add("30");
                        } else {
                            finalDataList.add("34");
                        }
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        if (Arrays.asList("0203FF00", "0204FF00", "0201FF00", "0202FF00", "0207FF00").contains(dataItemId)) {
                            int m;
                            switch (dataItemId) {
                                case "0203FF00":
                                    m = 1;
                                    break;
                                case "0204FF00":
                                    m = 5;
                                    break;
                                default:
                                    m = 0;
                            }
                            for (int i = 1; i <= 3; i++) {
                                Object v = data645List.get(i - 1);
                                if (v.toString().contains("F")) {
                                    continue;
                                }
                                result = Arrays.asList(new Object[finalDataList.size()]);
                                Collections.copy(result, finalDataList);
                                result.set(2, i + m + "");
                                result.set(4, Double.parseDouble(v.toString()));
                                dataAbc.add(result);
                            }
                            finalDataList = dataAbc;
                        }
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;
                        break;
                    case "E_METER_EVENT_RQ_GROUP_PRO":
                    case "E_METER_EVENT_WEEKDAY_PRO":
                    case "E_METER_EVENT_AP_GROUP_PRO":
                    case "E_METER_EVENT_TZONE_PRO":
                    case "E_METER_EVENT_SETTLEDAY_PRO":
                    case "E_METER_EVENT_ESAM_KEY_PRO":
                    case "E_METER_EVENT_HOLIDAY_PRO":
                    case "E_METER_EVENT_CUR_HIGH_UNBALAN":
                    case "E_METER_EVENT_TFRAME_PRO":
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(EventUtils.getDate(data645List, 0));
                        finalDataList.add(null);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_PROGRAM":
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(EventUtils.getDate(data645List, 0));
                        finalDataList.add(new BigDecimal(data645List.get(1).toString()));
                        for (int i = 2, j = data645List.size() - 2; i < j; i++) {
                            String v = (String) data645List.get(i);
                            if (v.contains("F")) {
                                v = v.replaceAll("F", "0");
                            }
                            finalDataList.add(v);
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_CHEK_TIME":
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(EventUtils.getDate(data645List, 1));
                        finalDataList.add(EventUtils.getDate(data645List, 2));
                        finalDataList.add(new BigDecimal(data645List.get(0).toString()));
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_MTER_EVENT_NO_POWER_SOURCE":
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        Date stTime = EventUtils.getDate(data645List, 0);
                        Date endTime = EventUtils.getDate(data645List, 1);
                        String dataType = "0";
                        if (stTime == null) {
                            return;
                        }
                        if (endTime != null) {
                            dataType = "1";
                        }

                        finalDataList.add(dataType);
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        boolean err = DateFilter.betweenDay(DateUtil.format(stTime, "yyyy-MM-dd HH:mm:ss"), 0, 180);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        if (!err) {
                            dataItemId = "no_power_err";
                        } else {
                            dataItemId = businessDataitemId;
                        }
                        break;
                    case "E_METER_EVENT_LOSE_VOL":
                    case "E_METER_EVENT_LOW_VOL":
                    case "E_METER_EVENT_OVER_VOL":
                    case "E_METER_EVENT_VOL_BREAK":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 36);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        switch (dataItemId) {
                            case "1001FF01":
                            case "1101FF01":
                            case "1201FF01":
                            case "1301FF01":
                                finalDataList.add("1");
                                break;
                            case "1002FF01":
                            case "1102FF01":
                            case "1202FF01":
                            case "1302FF01":
                                finalDataList.add("2");
                                break;
                            case "1003FF01":
                            case "1103FF01":
                            case "1203FF01":
                            case "1303FF01":
                                finalDataList.add("3");
                            default:
                        }
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        for (int i = 1; i < 32; i++) {
                            finalDataList.add(Double.valueOf(data645List.get(i).toString()));
                        }
                        for (int i = 37; i < data645List.size() - 2; i++) {
                            finalDataList.add(Double.valueOf(data645List.get(i).toString()));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_OVER_LOAD":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 17);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        switch (dataItemId) {
                            case "1C01FF01":
                                finalDataList.add("1");
                                break;
                            case "1C02FF01":
                                finalDataList.add("2");
                                break;
                            case "1C03FF01":
                                finalDataList.add("3");
                            default:
                        }
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_I_REVE_PHASE":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 17);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_NO_CURR":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 32);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        switch (dataItemId) {
                            case "1A01FF01":
                                finalDataList.add("1");
                                break;
                            case "1A02FF01":
                                finalDataList.add("2");
                                break;
                            case "1A03FF01":
                                finalDataList.add("3");
                            default:
                        }
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_ASSI_POWER_SHUT":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 1);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_SWITCH_ON":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(terminalDataObject.getUpTime());//上报日期
                        finalDataList.add(stTime);
                        finalDataList.add(data645List.get(1));
                        for (int i = 2; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_FACT_LOWER_LIMIT":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 5);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(terminalDataObject.getUpTime());//上报日期
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_OVER_I":
                    case "E_METER_EVENT_LOSE_CURR":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 32);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        switch (dataItemId) {
                            case "1901FF01":
                            case "1801FF01":
                                finalDataList.add("1");
                                break;
                            case "1902FF01":
                            case "1802FF01":
                                finalDataList.add("2");
                                break;
                            case "1903FF01":
                            case "1803FF01":
                                finalDataList.add("3");
                            default:
                        }
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        for (int i = 1; i < 32; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        for (int i = 33; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_TRIP":
                    case "E_METER_EVENT_CLEAR":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(data645List.get(1));
                        for (int i = 2; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_SWITCH_ERR":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 1);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(data645List.get(2));
                        for (int i = 3; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_POWER_ABN":
                    case "E_METER_EVENT_MAG_INTERF":
                    case "E_METER_EVENT_OPEN_TERM_LID":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 1);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        for (int i = 2; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_REVE_PHASE":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 17);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        for (int i = 1; i <= 16; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        for (int i = 18; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_VOL_UNBALANCE":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 18);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(data645List.get(17));
                        for (int i = 1; i <= 16; i++) {
                            finalDataList.add(data645List.get(i));
                            if (i == 8 || i == 12 || i == 16) {
                                for (int j = 0; j < 5; j++) {
                                    finalDataList.add(null);
                                }
                            }
                        }
                        for (int i = 19; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_RAP_DEMD_OVER_LI":
                    case "E_METER_EVENT_PAP_DEMD_OVER_LI":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(EventUtils.getDate(data645List, 1));
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_P_RETURN":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 17);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        switch (dataItemId) {
                            case "1B01FF01":
                                finalDataList.add("1");
                                break;
                            case "1B02FF01":
                                finalDataList.add("2");
                                break;
                            case "1B03FF01":
                                finalDataList.add("3");
                            default:
                        }
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        for (int i = 1; i <= 16; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        for (int i = 18; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_CUR_UNBALANCE":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 18);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_CURR_RETURN":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        for (int i = 2; i < data645List.size() - 2; i++) {
                            finalDataList.add(data645List.get(i));
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_CLEAR_EVENT":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(null);
                        finalDataList.add(terminalDataObject.getUpTime());
                        finalDataList.add(stTime);
                        finalDataList.add(data645List.get(1).toString());
                        finalDataList.add(data645List.get(2).toString());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_CLEAR_DEMAND":
                        stTime = EventUtils.getDate(data645List, 0);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(tao.getID());//mped_id
                        finalDataList.add(stTime);
                        finalDataList.add(data645List.get(1));
                        for (int i = 1; i <= 20; i++) {
                            finalDataList.add(data645List.get(i * 2));
                        }
                        for (int i = 0; i < 4; i++) {
                            finalDataList.add(null);
                        }
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(stTime, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_EVENT_ALL_VOL_LOSE":
                        stTime = EventUtils.getDate(data645List, 0);
                        endTime = EventUtils.getDate(data645List, 2);
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(stTime);
                        finalDataList.add(endTime);
                        finalDataList.add(Double.parseDouble(data645List.get(1).toString()));
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
//                    case "E_EDC_CONS_OPENLID_SOURCE":
//                        finalDataList.remove(0);
//                        finalDataList.add(tao.getID());
//                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
//                        finalDataList.add(tao.getPowerUnitNumber());
//                        finalDataList.add(BigDecimal.valueOf(Long.parseLong(data645List.get(0).toString())));
//                        finalDataList.add(new Date());
//                        finalDataList.add("00");
//                        if ("auto".equals(sType)) {
//                            finalDataList.add("30");
//                        } else {
//                            finalDataList.add("34");
//                        }
//                        finalDataList.add(new Date());
//                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
//                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
//                        break;
                    case "E_METER_TIME_09_SOURCE":
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        finalDataList.add(tao.getID());
                        String type = null;
                        Object datas = data645List.get(0);
                        String meterTime;
                        if (datas == null) {
                            return;
                        } else {
                            meterTime = datas.toString();
                            String[] dateArr = meterTime.split(" ");
                            if (dateArr.length != 3) {
                                return;
                            }
                            meterTime = dateArr[0] + " " + dateArr[2];
                        }
                        switch (dataItemId) {
                            case "04000101":
                                type = "01";
                                break;
                            case "04000102":
                                type = "02";
                                break;
                            case "04000103":
                            case "0400010C":
                                type = "03";
                            default:
                        }
                        finalDataList.add(sdf.parse(meterTime));
                        Date uptime = terminalDataObject.getUpTime();
                        if (uptime == null) {
                            uptime = new Date();
                        }
                        finalDataList.add(uptime);
                        finalDataList.add(type);
                        finalDataList.add(new Date());//insert_time
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//shard_no
                        finalDataList.remove(0);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;
                        break;
                    case "E_METER_TIME_DIVISION_SOURCE":
                        finalDataList.add(tao.getID());
                        switch (dataItemId) {
                            case "04000106":
                                finalDataList.add("01");//时区 01 时段 02
                                break;
                            case "04000107":
                                finalDataList.add("02");//时区 01 时段 02
                                break;
                            default:
                                return;
                        }
                        Object divTime = data645List.get(0);
                        if (divTime != null && !"".equals(divTime)) {
                            finalDataList.add(DateUtil.parse(divTime.toString(), "yyyy-MM-dd HH:mm"));
                        } else {
                            return;
                        }
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_COUNT_DIVISION_SOURCE":
                        finalDataList.add(tao.getID());
                        switch (dataItemId) {
                            case "04000201":
                                finalDataList.add("01");//年时区数 01 日时段表数 02
                                break;
                            case "04000202":
                                finalDataList.add("02");//年时区数 01 日时段表数 02
                                break;
                            default:
                                return;
                        }
                        Object numbers = data645List.get(0);
                        if (numbers != null && !"".equals(numbers)) {
                            finalDataList.add(numbers);
                        } else {
                            return;
                        }
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_METER_ZONE_DIVISION_SOURCE":
                    case "E_METER_SECT_DIVISION_SOURCE":
                        finalDataList.add(tao.getID());
                        switch (dataItemId) {
                            case "04010000":
                            case "04010001":
                                finalDataList.add("01");//第一套第1日时段表数据 01 第一套第1日时段表数据 02
                                break;
                            case "04020000":
                            case "04020001":
                                finalDataList.add("02");//第一套第1日时段表数据 01 第一套第1日时段表数据 02
                                break;
                            default:
                        }

                        if (data645List.size() > 2) {
                            StringBuffer sb = new StringBuffer();
                            for (int i = 0; i < data645List.size() - 2; i++) {
                                sb.append(data645List.get(i) + ",");
                            }
                            if (sb.length() == 0) {
                                return;
                            }
                            finalDataList.add(sb.toString().substring(0, sb.length() - 1));
                        } else {
                            return;
                        }
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        break;
                    case "E_MP_DAY_DEMAND_SOURCE":
                        finalDataList.remove(0);
                        finalDataList.add(tao.getID());
                        switch (dataItemId) {
                            case "01010000"://直抄实时
                            case "01010001"://直抄冻结
                                finalDataList.add("1");//flag
                                break;
                            case "01020000":
                            case "01020001":
                                finalDataList.add("5");//flag
                                break;
                            case "01030000":
                            case "01030001":
                                finalDataList.add("2");//flag
                                break;
                            case "01040000":
                            case "01040001":
                                finalDataList.add("6");//flag
                            default:
                        }
                        finalDataList.add(terminalDataObject.getUpTime());//抄表时间
                        Object demand_v = data645List.get(0);
                        if (demand_v == null) {
                            return;
                        }
                        Object d = data645List.get(1);//demandtime
                        Date demandDate = null;
                        if (d != null) {
                            try {
                                demandDate = sdf.parse(d + ":00");
                            } catch (Exception e) {
                                StringBuffer sb = new StringBuffer();
                                sb.append("20");
                                sb.append(d);
                                sb.append(":00");
                                demandDate = sdf.parse(sb.toString());
                            }
                        }
                        finalDataList.add(demand_v);//demand_v
                        finalDataList.add(demandDate);
                        for (int i = 0; i < 8; i++) {
                            finalDataList.add(null);
                        }
                        finalDataList.add(tao.getPowerUnitNumber());//供电单位
                        finalDataList.add("00");//未分析
                        Date data_date = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
                        if ("auto".equals(sType)) {
                            if (dataItemId.endsWith("0000")) {//直抄实时，数据日期为前一天
                                finalDataList.add("30");
                            } else {
                                finalDataList.add("20");
                                data_date = DateFilter.getPrevMonthLastDay(new Date());//直抄冻结，数据日期为上月最后一天
                            }

                        } else {
                            if (dataItemId.endsWith("0000")) {//直抄实时，数据日期为前一天
                                finalDataList.add("34");
                            } else {
                                finalDataList.add("24");
                                data_date = DateFilter.getPrevMonthLastDay(new Date());//直抄冻结，数据日期为上月最后一天
                            }

                        }
                        finalDataList.add(new Date());//入库时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));

                        finalDataList.add(DateUtil.format(data_date, DateUtil.defaultDatePattern_YMD));
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;
                        break;
                    case "E_MP_MON_DEMAND_SOURCE":
                        finalDataList.remove(0);
                        //月冻结需量
                        finalDataList.add(tao.getID());//ID:BIGINT:测量点标识（MPED_ID）
                        finalDataList.add("1");//DATA_TYPE:VARCHAR:数据类型(0：分相需量、1：正向有功、2：正向无功、3：一象限无功、4：四象限无功、5：反向有功、6：反向无功、7：二象限无功、8：三象限无功)
                        finalDataList.add(terminalDataObject.getUpTime());//COL_TIME:DATETIME:终端抄表时间
                        for (int ad = 0; ad < 5; ad++) {
                            List dlist = (List) data645List.get(ad);
                            if (dlist == null) {
                                finalDataList.add(null);
                                finalDataList.add(null);
                                continue;
                            }

                            Object obj_val = dlist.get(0); //最大需量
                            if (obj_val == null) {
                                if (ad == 0) {
                                    return;
                                } else {
                                    finalDataList.add(null);
                                }
                            } else {
                                finalDataList.add(obj_val);
                            }
                            Object obj_date = dlist.get(1);//需量发生时间
                            if (obj_date == null) {
                                finalDataList.add(null);
                            } else {
                                if (obj_date.toString().contains("2000-00")) {
                                    SimpleDateFormat sdf2000 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                                    finalDataList.add(sdf2000.parse(obj_date.toString()));
                                } else {
                                    SimpleDateFormat sdfEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                                    finalDataList.add(sdfEnd.parse(obj_date.toString()));
                                }
                            }
                        }
                        finalDataList.add(tao.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位编号:本实体记录的唯一标识，创建供电单位的唯一编码。
                        finalDataList.add("00");//STATUS:VARCHAR:数据状态00：未分析
                        if ("auto".equals(sType)) {
                            finalDataList.add("20");//DATA_SRC:VARCHAR:抄表方式
                        } else {
                            finalDataList.add("24");//DATA_SRC:VARCHAR:抄表方式
                        }
                        finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                        String num=dataItemId.substring(dataItemId.length()-1,dataItemId.length());
                        int score=1;
                        if(num.equals("A")){
                            score=10;
                        }else if(num.equals("B")){
                            score=11;
                        }else if(num.equals("C")){
                            score=12;
                        }else{
                            score=Integer.parseInt(num);
                        }
                        String dataDate=lastDayByMonth(score);
                        finalDataList.add(dataDate);//DATA_DATE:DATE:数据日期
                        refreshKey = CommonUtils.refreshKey(tao.getID(), tao.getID(),"00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;
                        break;
                    case "E_METER_RUN_STATUS_NUM1_SOURCE":
                        String statusStr = data645List.get(0).toString();
                        int i = Integer.parseInt(statusStr, 16);
                        String value = Integer.toBinaryString(i);
                        value = "0000000000000000".substring(0, 16 - value.length()) + value;
                        List list = new ArrayList();
                        list.add(value.charAt(15));
                        list.add(value.charAt(14));
                        list.add(value.charAt(13));
                        list.add(value.charAt(12));
                        list.add(value.charAt(11));
                        list.add(value.charAt(10));
                        list.add(0);
                        list.add(0);
                        list.add(value.charAt(7));
                        list.add(value.charAt(6));
                        list.add(0);
                        list.add(0);
                        list.add(value.charAt(3));
                        list.add(value.charAt(2));
                        list.add(value.charAt(1));
                        list.add(value.charAt(0));

                        StringBuilder sb = new StringBuilder();
                        for (i = 0; i < list.size(); i++) {
                            sb.append(list.get(i));
                        }
                        finalDataList.add(tao.getID());
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        finalDataList.add(tao.getPowerUnitNumber());
                        finalDataList.add(sb.toString());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(new Date());
                        finalDataList.remove(0);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;
                        break;
                    case "E_METER_RUN_STATUS_NUM3_SOURCE":
                        String ss = data645List.get(0).toString();
                        i = Integer.parseInt(ss, 16);
                        value = Integer.toBinaryString(i);
                        value = "0000000000000000".substring(0, 16 - value.length()) + value;
                        System.out.println(value);
                        list = new ArrayList();
                        list.add(value.charAt(15));
                        list.add(value.charAt(14) + "" + value.charAt(13) + "");
                        list.add(value.charAt(12));
                        list.add(value.charAt(11));
                        list.add(value.charAt(10));
                        list.add(value.charAt(9));
                        list.add(value.charAt(8));
                        list.add(value.charAt(7) + "" + value.charAt(6) + "");
                        list.add(value.charAt(5));
                        list.add(value.charAt(4));
                        list.add(value.charAt(3));
                        list.add(value.charAt(2));
                        list.add(value.charAt(1));
                        list.add(value.charAt(0));

                        String mpedId = tao.getID();
                        finalDataList.add(mpedId);
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));
                        finalDataList.add(tao.getPowerUnitNumber());

                        sb = new StringBuilder();
                        sb.append(list.get(0));//当前运行时段
                        sb.append(list.get(1));//供电方式
                        sb.append(list.get(2));//红外认证
                        sb.append(list.get(3));//跳合闸
                        sb.append(list.get(4));//当前运行时区
                        sb.append(list.get(5));//继电器命令状态
                        sb.append(list.get(6));//预跳闸报警状态
                        sb.append(list.get(7));//电能表类型

                        sb.append(list.get(8));//保留
                        sb.append(list.get(9));//保留

                        sb.append(list.get(10));//保电状态

                        if(ParamConstants.startWith.equals("11")) {
                            sb.append(list.get(11));//身份认证状态
                            sb.append(list.get(12));//本地开户
                        }else {
                            sb.append(list.get(12));//本地开户
                            sb.append(list.get(11));//身份认证状态
                        }
                            sb.append(list.get(13));//远程开户
                        finalDataList.add(sb.reverse().toString());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.add(new Date());
                        finalDataList.remove(0);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", businessDataitemId, protocolId);
                        dataItemId = businessDataitemId;


                        //保电状态添加
                        String otherDi="switch-status";
                        List switchStatusList=new ArrayList();
                        switchStatusList.add(mpedId);
                        switchStatusList.add(tao.getTerminalId());
                        switchStatusList.add(0);//mped_index
                        switchStatusList.add(list.get(3).toString());
                        switchStatusList.add(list.get(6).toString());
                        switchStatusList.add(list.get(10).toString());
                        switchStatusList.add(tao.getPowerUnitNumber().substring(0, 5));
                        switchStatusList.add(new Date());

                        CommonUtils.putToDataHub(otherDi,mpedId,switchStatusList,null,listDataObj);

                        break;
                    case "E_MPED_REAL_BUY_RECORD_SOURCE":
                        String mpedIdStr = tao.getID();
                        finalDataList.add(new BigDecimal(mpedIdStr));
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//dataDate
                        finalDataList.add(new Date());//抄表日期
                        finalDataList.add(tao.getPowerUnitNumber());//orgNo占位
                        finalDataList.add(null);//剩余电量

                        finalDataList.add(data645List.get(0));//剩余金额
                        finalDataList.add(null);//报警电量
                        finalDataList.add(null);//故障电量
                        finalDataList.add(null);//累计购电量
                        finalDataList.add(null);//累计购电金额
                        if ("00900200".equals(dataItemId)) {
                            finalDataList.add(null);
                        } else {
                            finalDataList.add(data645List.get(2));//购电次数
                        }
                        finalDataList.add(null);//赊欠门限值
                        finalDataList.add(null);//透支电量
                        finalDataList.add(new Date());
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));
                        finalDataList.remove(0);
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), tao.getID(), "00000000000000", "6010401", protocolId);
                        businessDataitemId = dataItemId;
                        break;
                    case "E_MPED_REAL_OVERDRAFT_SOURCE"://透支金额
                        finalDataList.remove();
                        String id = tao.getID();
                        finalDataList.add(id);//ID:BIGINT:测量点标识（MPED_ID）
                        finalDataList.add(DateUtil.format(new Date(), DateUtil.defaultDatePattern_YMD));//DATA_DATE:DATE:数据日期
                        finalDataList.add(tao.getPowerUnitNumber());//ORG_NO:VARCHAR:供电单位编号
                        finalDataList.add(data645List.get(0));//OVERDR_MONEY:DECIMAL:透支金额
                        finalDataList.add(tao.getPowerUnitNumber().substring(0, 5));//SHARD_NO:VARCHAR:分库字段
                        finalDataList.add(new Date());//INSERT_TIME:DATETIME:插入或更新时间
                        refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), id, "00000000000000", businessDataitemId, protocolId);
                        businessDataitemId = dataItemId;
                        break;
                    default:
                }
                if (!(finalDataList.get(0) instanceof List) && finalDataList.size() < 2) {
                    return;
                }
                if (!"auto".equals(sType)) {
                    refreshKey = null;
                }
                if (finalDataList.get(0) instanceof List) {
                    for (int i = 0, j = finalDataList.size(); i < j; i++) {
                        List df = (List) finalDataList.get(i);
                        if (i > 0) {
                            refreshKey = null;
                        }
                        CommonUtils.putToDataHub(businessDataitemId, meterId, df, refreshKey, listDataObj);
                    }
                } else {
                    CommonUtils.putToDataHub(dataItemId, meterId, finalDataList, refreshKey, listDataObj);
                }
            } else {
                String data_type = null;
                Date dataTime;
                String data_src;
                if ("auto".equals(sType)) {
                    data_src = "30";
                } else {
                    data_src = "34";
                }
                Date now = new Date();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(now);
                if (dateMap1.containsKey(dataItemId)) {
                    int obj1 = (int) dateMap1.get(dataItemId);
                    if (obj1 != 1 || dataItemId.equals("05060101")) {
                        if ("auto".equals(sType)) {
                            data_src = "20";
                        } else {
                            data_src = "24";
                        }
                    }
                    data_type = "1";
                    calendar.add(Calendar.DATE, -obj1);
                }
                if (dateMap2.containsKey(dataItemId)) {
                    int obj2 = (int) dateMap2.get(dataItemId);
                    if (obj2 != 1 || dataItemId.equals("05060301")) {
                        if ("auto".equals(sType)) {
                            data_src = "20";
                        } else {
                            data_src = "24";
                        }
                    }
                    data_type = "2";
                    calendar.add(Calendar.DATE, -obj2);
                }
                if (dateMap5.containsKey(dataItemId)) {
                    int obj5 = (int) dateMap5.get(dataItemId);
                    if (obj5 != 1 || dataItemId.equals("05060201")) {
                        if ("auto".equals(sType)) {
                            data_src = "20";
                        } else {
                            data_src = "24";
                        }
                    }
                    data_type = "5";
                    calendar.add(Calendar.DATE, -obj5);
                }
                if (dateMap6.containsKey(dataItemId)) {
                    int obj6 = (int) dateMap6.get(dataItemId);
                    if (obj6 != 1 || dataItemId.equals("05060401")) {
                        if ("auto".equals(sType)) {
                            data_src = "20";
                        } else {
                            data_src = "24";
                        }
                    }
                    data_type = "6";
                    calendar.add(Calendar.DATE, -obj6);
                }
                dataTime = calendar.getTime();
                if (data_type == null) {
                    return;
                }
//                if (data_src == "30") {
//                    if (data_type != "5" && data_type != "6") {
//                        return;
//                    }
//                }
                String mpedIdStr = tao.getID();
                Protocol3761ArchivesObject protocol3761ArchivesObject = CommonUtils.getProArchive(protocolId, terminalDataObject.getAFN(), fn);
                String businessDataitemId = ProtocolArchives.getInstance().getProtocolArchivesObjectBy645(protocol3761ArchivesObject, dataItemId).getBusiDataItemId();
                List dataListFinal = new ArrayList();
                String data_date = DateUtil.format(dataTime, DateUtil.defaultDatePattern_YMD);
                dataListFinal.add(mpedIdStr + "_" + data_date);
                dataListFinal.add(mpedIdStr);//从缓存取测量点标识，这里占位
                dataListFinal.add(data_type);
                dataListFinal.add(new Date());

                for (int i = 0; i < 5; i++) {
                    Object d;
                    if (i < data645List.size() - 2) {
                        d = data645List.get(i);
                    } else {
                        dataListFinal.add(null);
                        continue;
                    }
                    dataListFinal.add((new BigDecimal(d.toString())).doubleValue());// j f p g
                }
                for (int i = 0; i < 10; i++) {
                    dataListFinal.add(null);
                }

                dataListFinal.add(tao.getPowerUnitNumber());//供电单位编号
                dataListFinal.add("00");//未分析
                dataListFinal.add(data_src);//shougongcunku
                dataListFinal.add(new Date());//入库时间
                dataListFinal.add(tao.getPowerUnitNumber().substring(0, 5));//前闭后开  前5位 不包括[5]
                dataListFinal.add(data_date);//入库时间


                Object[] refreshKey = CommonUtils.refreshKey(tao.getTerminalId(), mpedIdStr, "00000000000000", businessDataitemId, protocolId);
                if (!"auto".equals(sType)) {
                    refreshKey = null;
                }
                CommonUtils.putToDataHub(businessDataitemId, mpedIdStr, dataListFinal, refreshKey, listDataObj);

                //风光路透传入其他表
                if (Arrays.asList("0001FF00", "0002FF00", "0003FF00", "0004FF00").contains(dataItemId)) {
                    CommonUtils.putToDataHub("other_freeze", mpedIdStr, dataListFinal, null, listDataObj);
                }
            }
        }
    }

    private static String lastDayByMonth(int score) throws Exception {
        Calendar first = Calendar.getInstance();
        first.setTime(new Date());
        Calendar last = Calendar.getInstance();
        last.set(Calendar.YEAR, first.get(Calendar.YEAR));
//        last.add(Calendar.MONTH, first.get(Calendar.MONTH));
        last.add(Calendar.MONTH, -score);
        last.set(Calendar.DAY_OF_MONTH, 1);
        return DateUtil.format(last.getTime(), DateUtil.defaultDatePattern_YMD);
    }
}
