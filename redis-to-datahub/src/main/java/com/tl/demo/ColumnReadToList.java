package com.tl.demo;

import java.sql.*;

/**
 * @author Dongwei-Chen
 * @Date 2019/7/29 14:12
 * @Description 获取属性名及类型
 */
public class ColumnReadToList {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://drdsusrzv2mv8n20vpc.drds.res.sgmc.sgcc.com.cn:3306/bjeascsdb?autoReconnect=true&amp;rewriteBatchedStatements=true&amp;useOldAliasMetadataBehavior=true&amp;connectTimeout=30000&amp;socketTimeout=60000";
    private static final String USERNAME = "bjeascsdb";
    private static final String PASSWORD = "Am_1234567";
    private static final String TABLENAME = "e_event_erc62";
    public static void main(String[] args) {
        String result= resultColumn(TABLENAME);
        System.out.println("---------------------------sqlmapping---------------------------");
        System.out.println("<SqlMapping>");
        System.out.println("    <!-- "+TABLENAME+" -->");
        System.out.println("    <businessDataitemIds>null</businessDataitemIds>");
        System.out.println("    <pnType>P</pnType>");
        System.out.println("    <projectName>eas</projectName>");
        System.out.println("    <topicName>"+TABLENAME.toUpperCase()+"_SOURCE</topicName>");
        System.out.println("    <shardCount>1</shardCount>");
        System.out.println("    <lifeCycle>3</lifeCycle>");
        System.out.println("     <fields>"+result+"</fields>");
        System.out.println("</SqlMapping>");
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
            String filed="";
            StringBuffer toSubList = new StringBuffer();
            String prefix = "dataListFinal.add(\"";
            String suffix = "\");//";

            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                if (tableName.equalsIgnoreCase(table)) {
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
                            if (columnName.equalsIgnoreCase(colName)) {
                                remark = resultSet.getString("REMARKS");
                                break;
                            }
                        }

                        filed=columnName + ":" + columnType + ":" + remark.replace("\r|\n", "") ;
                        toSubList.append(filed+"\n");
                    }
                }
                break;
            }
            System.out.println("---------------------------datalist---------------------------");
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
        return schema.toUpperCase();
    }


    private static void createToList() {

    }
}
