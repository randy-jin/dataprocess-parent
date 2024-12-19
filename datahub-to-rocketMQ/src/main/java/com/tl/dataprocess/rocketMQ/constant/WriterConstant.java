package com.tl.dataprocess.rocketMQ.constant;

/**
 * @author ：chenguanxing
 * @date ：Created in 2022/7/6 11:17
 * @description ：RocketMQ实例化
 * @version: 1.0.0.0
 */
public class WriterConstant {

    /*******************实例化writer时的魔法值************************/
    /**
     * 线程池大小
     */
    public static final Integer THREAD_POOL_SIZE = 0;

    /**
     * 执行判据
     */
    public static final Integer RUN_FLAG = 1;

    /***********************传递的map的初始容量**************************/
    /**
     * 事件的
     */
    public static final int EVENT_INITIAL_CAPACITY = 12;
    /**
     * 电压的
     */
    public static final int VOL_INITIAL_CAPACITY = 9;

    /*****************向MQ传递数据中的key值*****************************/

    /**
     * ORG_NO
     */
    public static final String ORG_NO = "ORG_NO";

    /**
     * 终端id
     */
    public static final String TERMINAL_ID = "TERMINAL_ID";

    /**
     * TG_ID
     */
    public static final String TG_ID = "TG_ID";

    /**
     * CONS_NO
     */
    public static final String CONS_NO = "CONS_NO";

    /**
     * OUTAGE_BEGIN_TIME
     */
    public static final String OUTAGE_BEGIN_TIME = "OUTAGE_BEGIN_TIME";

    /**
     * OUTAGE_END_TIME
     */
    public static final String OUTAGE_END_TIME = "OUTAGE_END_TIME";

    /**
     * OUTAGE_FLAG
     */
    public static final String OUTAGE_FLAG = "OUTAGE_FLAG";

    /**
     * event_type
     */
    public static final String EVENT_TYPE = "EVENT_TYPE";

    /**
     *
     * input_time
     */
    public static final String INPUT_TIME = "INPUT_TIME";

    /**
     * EVENT_TIME
     */
    public static final String EVENT_TIME = "EVENT_TIME";

    /**
     * no_power_sd
     */
    public static final String NO_POWER_SD = "NO_POWER_SD";

    /**
     * no_power_ed
     */
    public static final String NO_POWER_ED = "NO_POWER_ED";

    /**
     * RESTART_TIME
     */
    public static final String RESTART_TIME = "RESTART_TIME";

    /**
     * id
     */
    public static final String ID = "ID";

    /**
     * METER_ID
     */
    public static final String METER_ID = "METER_ID";

    /**
     * DATA_DATE
     */
    public static final String DATA_DATE = "DATA_DATE";

    /**
     * COL_TIME
     */
    public static final String COL_TIME = "COL_TIME";

    /**
     * INSERT_TIME
     */
    public static final String INSERT_TIME = "INSERT_TIME";

    /**
     * PHASE_FLAG
     */
    public static final String PHASE_FLAG = "PHASE_FLAG";

    /**
     * DATA_TYPE
     */
    public static final String DATA_TYPE = "DATA_TYPE";


    /****************    redis中的key    ****************/
    public static final String TB_KEY = "TB$";

    public static final String ORG = "ORG";

    public static final String ADDR = "ADDR";

    public static final String M_KEY = "M${}#{}";

    public static final String P_KEY = "P${}#{}#{}";

    public static final String TGID = "TGID";

    /****************   writer中MQ的topic和tag    ****************/
    /**
     * 停电事件topic
     */
    public static final String EVENT_TOPIC = "cms_tg_outage";
    /**
     * 停电事件Tag
     */
    public static final String EVENT_TAG = "outage";

    /**
     * 电压topic
     */
    public static final String VOL_CURVE_TOPIC = "cms_volt_curve";
    /**
     * 电压Tag
     */
    public static final String VOL_CURVE_TAG = "voltCurve";

    /**
     * 电流topic
     */
    public static final String CUR_CURVE_TOPIC = "cms_cur_curve";
    /**
     * 电流Tag
     */
    public static final String CUR_CURVE_TAG = "curCurve";

    /**
     * 功率topic
     */
    public static final String POWER_CURVE_TOPIC = "cms_power_curve";
    /**
     * 功率Tag
     */
    public static final String POWER_CURVE_TAG = "powerCurve";

    /**
     * 功率因数topic
     */
    public static final String FACTOR_CURVE_TOPIC = "cms_factor_curve";
    /**
     * 功率因数Tag
     */
    public static final String FACTOR_CURVE_TAG = "factorCurve";

    /**
     * 示值topic
     */
    public static final String READ_CURVE_TOPIC = "cms_read_curve";
    /**
     * 示值Tag
     */
    public static final String READ_CURVE_TAG = "readCurve";

    /**
     * 用户停电topic
     */
    public static final String USER_POWER_CUT_TOPIC = "cms_cons_outage";
    /**
     * 用户停电Tag
     */
    public static final String USER_POWER_CUT_TAG = "userPowerCut";


    /****************   writer中datahub的topic    ****************/

    /**
     * 台区停电事件topic
     */
    public static final String DATA_EVENT_TOPIC = "e_event_erc14_source";

    /**
     * 电压事件topic
     */
    public static final String DATA_VOL_CURVE_TOPIC = "e_mp_vol_curve_ud_source";

    /**
     * 电流事件topic
     */
    public static final String DATA_CUR_CURVE_TOPIC = "e_mp_cur_curve_ud_source";

    /**
     * 功率事件topic
     */
    public static final String DATA_POWER_CURVE_TOPIC = "e_mp_power_curve_ud_source";

    /**
     * 功率事件topic
     */
    public static final String DATA_FACTOR_CURVE_TOPIC = "e_mp_factor_curve_ud_source";

    /**
     * 示值topic
     */
    public static final String DATA_READ_CURVE_TOPIC = "e_mp_read_curve_ud_source";

    /**
     * 用户停电事件topic
     */
    public static final String DATA_USER_NO_POWER_TOPIC = "e_meter_event_no_power_source";
}
