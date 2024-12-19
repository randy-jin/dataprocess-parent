package com.tl.dataprocess.ConversionUtil;

import com.tl.archives.TerminalArchivesObject;

import java.util.List;

/**
 * Created with IDEA
 * User：wangjunjie
 * Date: 2021/12/22
 * Time: 14:46
 */
public interface IConversionUtil {

    List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc);
}
