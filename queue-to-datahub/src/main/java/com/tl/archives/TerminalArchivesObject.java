package com.tl.archives;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 终端档案
 * @author Administrator
 *
 */
@Data
@Getter
@Setter
public class TerminalArchivesObject implements Serializable{

	private static final long serialVersionUID = -201403062006L;
	
	private String powerUnitNumber; //供电单位编号
	private String terminalId;//终端标识
	private String mpedId;//测量点标识
	private String meterId;//电能表id

	private String areaCode;//行政区划码
	private String terminalAddr;//终端地址

}
