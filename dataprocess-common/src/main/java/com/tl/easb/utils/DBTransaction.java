package com.tl.easb.utils;

import java.sql.Connection;

/**
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-22    Administrator
 */
public abstract interface DBTransaction {

	public abstract Object doInConnection(Connection conn); 
}
