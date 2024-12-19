package com.tl.hades.persist;

import java.sql.*;

/**
 * @author Dongwei-Chen
 * @Date 2019/7/29 14:12
 * @Description 获取属性名及类型
 */
public class ColumnReadToList {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://drds9n7a819j5hfdpublic.drds.aliyuncs.com:3306/easdb?autoReconnect=true&amp;rewriteBatchedStatements=true";
    private static final String USERNAME = "easdb";
    private static final String PASSWORD = "03o06G2T10ZU";

    public static void main(String[] args) {
        String result = resultColumn("E_MPED_REAL_APPARENT_POWER");
        System.out.println(result);
    }

    private static final String SQL = "SELECT * FROM ";

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //获取数据库连接
    public static Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static String resultColumn(String tableName) {
        Connection conn = getConnection();
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        StringBuffer sb = new StringBuffer();
        try {
            pStemt = conn.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            DatabaseMetaData dm = conn.getMetaData();
            ResultSet rs = dm.getTables(null, "%", tableName, new String[]{"TABLE"});
            //表列数
            int size = rsmd.getColumnCount();
            StringBuffer toSubList = new StringBuffer();
            String prefix = "dataListFinal.add(\"";
            String suffix = "\");//";

            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                if (tableName.equals(table)) {
                    for (int i = 0; i < size; i++) {
                        ResultSet resultSet = conn.getMetaData().getColumns(null, getSchema(conn), table, "%");
                        String columnName = (rsmd.getColumnName(i + 1)).toUpperCase();
                        String columnType = rsmd.getColumnTypeName(i + 1);
                        if ("decimal".equalsIgnoreCase(columnType)) {
                            sb.append(columnName + ":DOUBLE,");
                        } else if ("bigint".equalsIgnoreCase(columnType) || "int".equalsIgnoreCase(columnType)) {
                            sb.append(columnName + ":BIGINT,");
                        } else if ("datetime".equalsIgnoreCase(columnType)) {
                            sb.append(columnName + ":TIMESTAMP,");
                        } else {
                            sb.append(columnName + ":STRING,");
                        }
                        toSubList.append(prefix);
                        toSubList.append(suffix);
                        String remark = "";
                        while (resultSet.next()) {
                            String colName = resultSet.getString("COLUMN_NAME");
                            if (columnName.equals(colName))
                                remark = resultSet.getString("REMARKS");
                        }
                        toSubList.append(columnName + ":" + columnType + ":" + remark.replace("\r|\n", "") + "\n");
                    }
                }
            }
            System.out.println(toSubList);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    closeConnection(conn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        String result = sb.toString();
        return result.substring(0, result.length() - 1);
    }

    private static String getSchema(Connection connection) throws SQLException {
        String schema = connection.getMetaData().getUserName();
        return schema.toUpperCase().toString();
    }


    private static void createToList() {

    }
}
