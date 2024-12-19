package com.tl;

import com.google.common.net.HostAndPort;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class PgSql {

    public static void main(String[] args) throws Exception {

        String host = "localhost";
        Integer port = 5432;
        String user ="postgres";
        String password = "123456";
        Connection conn = getConn(host,port,user,password,"easb");
        String sql = "select * from \"user\" ";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs= stmt.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
            }
        }finally {
            conn.close();
        }

    }

    public static Connection getConn(String host, Integer port, String user, String passWord, String dbName) throws Exception {

        String jdbcUrl = "jdbc:postgresql://%s/" + dbName;
        HostAndPort endpoint = HostAndPort.fromParts(host, port);
        String url = String.format(jdbcUrl, endpoint);
        Properties prop = new Properties();
        prop.setProperty("user", user);
        prop.setProperty("password", passWord);
        prop.setProperty("preferQueryMode", "simple");
        prop.setProperty("connectTimeout", "6000");
        prop.setProperty("socketTimeout", "7000");
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, prop);
    }


}
