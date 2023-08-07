package org.example;

import lombok.val;
import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcUtil {
    public static Connection getConnection(Datasource datasource) throws Exception {
        Class.forName(datasource.getDriver());
        return DriverManager.getConnection(datasource.getUrl(), datasource.getUsername(), datasource.getPassword());
    }

    public static <T> T coalease(T... args) {
        for (val t : args) {
            if (t != null) {
                return t;
            }
        }
        return null;
    }
}
