package cn.edu.tsinghua.iotdb.benchmark.concurrency;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class App {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		int maxConnectionNum = 40000;
		for(int i = 0; i < maxConnectionNum;i++){
			Connection connection =  DriverManager.getConnection("jdbc:tsfile://192.168.130.15:6667/", "root", "root");
			System.out.println(String.format("Open %d connection", i));
		}
	}

}
