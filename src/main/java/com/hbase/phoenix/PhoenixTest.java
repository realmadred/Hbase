package com.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixTest {

    public static Connection getConnection() {
        Connection cc = null;
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
         String url = "jdbc:phoenix:192.168.206.21:2181";
        try {
            Class.forName(driver);
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        }

        if(cc == null) {
            try {
                cc = DriverManager.getConnection(url);

            } catch(SQLException e) {
                e.printStackTrace();
            }
        }
        return cc;
    }
}
