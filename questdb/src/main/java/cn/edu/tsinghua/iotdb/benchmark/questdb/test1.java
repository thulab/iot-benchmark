package cn.edu.tsinghua.iotdb.benchmark.questdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * @Author stormbroken
 * Create by 2021/07/28
 * @Version 1.0
 **/

public class test1 {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");

        final Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:8812/qdb", properties);
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW TABLES");
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
        System.out.println("Done");
        connection.close();
    }
}
