package com.tl.hades.persist.entity;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Data13161Entity {
	/*	命名规则
	 * 	String packName = "com.tl.hades.persist.entity.";
		return packName + "Data" + afn + fn + "Entity1";
	*/
	
	
	//public static final String PARAM_NAMES1 = "dataDate,collTime,papR,papR1,papR2,papR3,"
	//		+ "papR4,papR5,papR6,papR7,papR8,papR9,papR10,papR11,papR12,papR13,papR14,orgNo,mpedId,stat,insertDate";
	public static final String PARAM_NAMES = "id,readtype,datadate,colltime,r,r1,r2,r3,"
											 +"r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,orgno,status,datasrc,inserttime,shardno";
	public static final String TOPIC_NAME = "e_mp_day_read";
	public static final String TOPIC_NAME_D_5 = "E_MP_DAY_ENERGY";
	//public static final String TOPIC_NAME = "FREEZE0DF161";e_mp_day_read
	public static final Map<String, String> TOPIC_PARAM_MAP = new HashMap<String, String>();

	static {
		TOPIC_PARAM_MAP.put(TOPIC_NAME, PARAM_NAMES);
		TOPIC_PARAM_MAP.put(TOPIC_NAME_D_5, PARAM_NAMES);
	}
	//见数据库
	private Integer id;
	private String readtype;
	private Date datadate;//date:yy-MM-dd  t_d数据日期 ： 年月日
	private Date colltime;//抄表时间datetime 年月日 时分
	private Double r;
	private Double r1;
	private Double r2;
	private Double r3;
	private Double r4;
	private Double r5;
	private Double r6;
	private Double r7;
	private Double r8;
	private Double r9;
	private Double r10;
	private Double r11;
	private Double r12;
	private Double r13;
	private Double r14;
	private String orgno;
	private String status;
	private String datasrc;
	private Date inserttime; //datetime
	private String shardno;

	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getReadtype() {
		return readtype;
	}
	public void setReadtype(String readtype) {
		this.readtype = readtype;
	}
	public Date getColltime() {
		return colltime;
	}
	public void setColltime(Date colltime) {
		this.colltime = colltime;
	}
	public Double getR() {
		return r;
	}
	public void setR(Double r) {
		this.r = r;
	}
	public Double getR1() {
		return r1;
	}
	public void setR1(Double r1) {
		this.r1 = r1;
	}
	public Double getR2() {
		return r2;
	}
	public void setR2(Double r2) {
		this.r2 = r2;
	}
	public Double getR3() {
		return r3;
	}
	public void setR3(Double r3) {
		this.r3 = r3;
	}
	public Double getR4() {
		return r4;
	}
	public void setR4(Double r4) {
		this.r4 = r4;
	}
	public Double getR5() {
		return r5;
	}
	public void setR5(Double r5) {
		this.r5 = r5;
	}
	public Double getR6() {
		return r6;
	}
	public void setR6(Double r6) {
		this.r6 = r6;
	}
	public Double getR7() {
		return r7;
	}
	public void setR7(Double r7) {
		this.r7 = r7;
	}
	public Double getR8() {
		return r8;
	}
	public void setR8(Double r8) {
		this.r8 = r8;
	}
	public Double getR9() {
		return r9;
	}
	public void setR9(Double r9) {
		this.r9 = r9;
	}
	public Double getR10() {
		return r10;
	}
	public void setR10(Double r10) {
		this.r10 = r10;
	}
	public Double getR11() {
		return r11;
	}
	public void setR11(Double r11) {
		this.r11 = r11;
	}
	public Double getR12() {
		return r12;
	}
	public void setR12(Double r12) {
		this.r12 = r12;
	}
	public Double getR13() {
		return r13;
	}
	public void setR13(Double r13) {
		this.r13 = r13;
	}
	public Double getR14() {
		return r14;
	}
	public void setR14(Double r14) {
		this.r14 = r14;
	}
	public String getOrgno() {
		return orgno;
	}
	public void setOrgno(String orgno) {
		this.orgno = orgno;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getDatasrc() {
		return datasrc;
	}
	public void setDatasrc(String datasrc) {
		this.datasrc = datasrc;
	}
	public Date getInserttime() {
		return inserttime;
	}
	public void setInserttime(Date inserttime) {
		this.inserttime = inserttime;
	}
	public String getShardno() {
		return shardno;
	}
	public void setShardno(String shardno) {
		this.shardno = shardno;
	}
	public Date getDatadate() {
		return datadate;
	}
	public void setDatadate(Date datadate) {
		this.datadate = datadate;
	}
}