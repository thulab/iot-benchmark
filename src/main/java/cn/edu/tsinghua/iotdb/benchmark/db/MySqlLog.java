package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

public class MySqlLog {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private Connection mysqlConnection;
	private Config config;
	private String localName;
	
	public MySqlLog() {
		config = ConfigDescriptor.getInstance().getConfig();
		initMysql();
	}
	
	
	public void initMysql() {
		try {
			Class.forName(Constants.MYSQL_DRIVENAME);
			mysqlConnection = DriverManager.getConnection(Constants.MYSQL_URL);
			Statement stat = mysqlConnection.createStatement();
			if(config.SERVER_MODE&&!hasTable("SERVER_MODE")) {
				stat.executeUpdate("creat table SERVER_MODE(id BIGINT,)");
				return ;
			}
			if (!hasTable(config.DB_SWITCH+"QueryResult")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"QueryResult(id BIGINT, queryNum BIGINT, point BIGINT,"
						+ " time BIGINT, clientNum INTEGER, rate DOUBLE, errorNum BIGINT, serverIP varchar(20),localName varchar(50),query_type INTEGER,"
						+ "QUERY_SENSOR_NUM INTEGER, QUERY_DIVICE_NUM INTEGER, QUERY_AGGREGATE_FUN varchar(30),"
						+ "QUERY_INTERVAL BIGINT, QUERY_LOWER_LIMIT DOUBLE, IS_EMPTY_PRECISE_POINT_QUERY boolean,primary key(id))");
			}
			if (!hasTable(config.DB_SWITCH+"QueryProcess")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"QueryProcess(id BIGINT, clientName varchar(50), "
						+ "loopIndex INTEGER, point INTEGER, time INTEGER, cur_rate DOUBLE, serverIP varchar(20),localName varchar(50),query_type INTEGER,"
						+ "QUERY_SENSOR_NUM INTEGER, QUERY_DIVICE_NUM INTEGER, QUERY_AGGREGATE_FUN varchar(30),"
						+ "QUERY_INTERVAL BIGINT, QUERY_LOWER_LIMIT DOUBLE, IS_EMPTY_PRECISE_POINT_QUERY boolean,primary key(id,clientName))");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("mysql 连接初始化失败，原因是：{}", e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LOGGER.error("mysql 连接初始化失败，原因是：{}", e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void closeMysql() {
		if (mysqlConnection != null) {
			try {
				mysqlConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOGGER.error("mysql 连接关闭失败，原因是：{}", e.getMessage());
				e.printStackTrace();
			}
		}
	}
	/** 数据库中是否已经存在名字为table的表 */
	private Boolean hasTable(String table) throws SQLException {
		String checkTable = "show tables like \"" + table + "\"";
		Statement stmt = mysqlConnection.createStatement();

		ResultSet resultSet = stmt.executeQuery(checkTable);
		if (resultSet.next()) {
			LOGGER.info("table {} exist!", table);
			return true;
		} else {
			LOGGER.info("table {} not exist!", table);
			return false;
		}

	}
}
