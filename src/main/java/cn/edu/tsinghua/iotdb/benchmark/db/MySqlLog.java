package cn.edu.tsinghua.iotdb.benchmark.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
	private static final Logger LOGGER = LoggerFactory.getLogger(MySqlLog.class);
	private Connection mysqlConnection = null;
	private Config config = ConfigDescriptor.getInstance().getConfig();
	private String localName="";
	
	public MySqlLog() {
		try {
			InetAddress localhost = InetAddress.getLocalHost();
			localName = localhost.getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			LOGGER.error("获取本机主机名称失败;UnknownHostException：{}",e.getMessage());
			e.printStackTrace();
		} 
	}
	
	public void initMysql() {
		try {
			Class.forName(Constants.MYSQL_DRIVENAME);
			mysqlConnection = DriverManager.getConnection(Constants.MYSQL_URL);
			Statement stat = mysqlConnection.createStatement();
			if(config.SERVER_MODE&&!hasTable("SERVER_MODE")) {
				stat.executeUpdate("create table SERVER_MODE(id BIGINT,localName varchar(50),cpu_usage DOUBLE,"
						+ "mem_usage DOUBLE,diskIo_usage DOUBLE,net_recv_usage DOUBLE,net_send_usage DOUBLE, remark varchar(100),primary key(id,localName))");
				LOGGER.info("Table SERVER_MODE create success!");
				return ;
			}
			if (config.IS_QUERY_TEST&&!hasTable(config.DB_SWITCH+"QueryResult")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"QueryResult(id BIGINT, queryNum BIGINT, point BIGINT,"
						+ " time DOUBLE, clientNum INTEGER, rate DOUBLE, errorNum BIGINT, serverIP varchar(20),localName varchar(50),query_type INTEGER,"
						+ "QUERY_SENSOR_NUM INTEGER, QUERY_DIVICE_NUM INTEGER, QUERY_AGGREGATE_FUN varchar(30),"
						+ "QUERY_INTERVAL BIGINT, QUERY_LOWER_LIMIT DOUBLE, IS_EMPTY_PRECISE_POINT_QUERY boolean, remark varchar(100),primary key(id))");
				LOGGER.info("Table {}QueryResult create success!",config.DB_SWITCH);
			}
			if (config.IS_QUERY_TEST&&!hasTable(config.DB_SWITCH+"QueryProcess")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"QueryProcess(id BIGINT, clientName varchar(50), "
						+ "loopIndex INTEGER, point INTEGER, time DOUBLE, cur_rate DOUBLE, serverIP varchar(20),localName varchar(50),query_type INTEGER,"
						+ "QUERY_SENSOR_NUM INTEGER, QUERY_DIVICE_NUM INTEGER, QUERY_AGGREGATE_FUN varchar(30),"
						+ "QUERY_INTERVAL BIGINT, QUERY_LOWER_LIMIT DOUBLE, IS_EMPTY_PRECISE_POINT_QUERY boolean, remark varchar(100),primary key(id,clientName))");
				LOGGER.info("Table {}QueryProcess create success!",config.DB_SWITCH);
			}
			if(!config.IS_QUERY_TEST&&!hasTable(config.DB_SWITCH+"InsertResult")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"InsertResult(id BIGINT, totalPoints BIGINT,"
						+ " time DOUBLE, clientNum INTEGER, rate DOUBLE, errorPoint BIGINT, createSchemaTime DOUBLE,serverIP varchar(20),localName varchar(50),"
						+ "GROUP_NUMBER INTEGER,DEVICE_NUMBER INTEGER,SENSOR_NUMBER INTEGER,CACHE_NUM INTEGER,POINT_STEP BIGINT,LOOP_NUM BIGINT, remark varchar(100),primary key(id))");
				LOGGER.info("Table {}InsertResult create success!",config.DB_SWITCH);
			}
			if (!config.IS_QUERY_TEST&&!hasTable(config.DB_SWITCH+"InsertProcess")) {
				stat.executeUpdate("create table "+config.DB_SWITCH+"InsertProcess(id BIGINT, clientName varchar(50), "
						+ "loopIndex INTEGER, costTime DOUBLE, totalTime DOUBLE, cur_rate DOUBLE, errorPoint BIGINT,serverIP varchar(20),localName varchar(50),"
						+ "GROUP_NUMBER INTEGER,DEVICE_NUMBER INTEGER,SENSOR_NUMBER INTEGER,CACHE_NUM INTEGER,POINT_STEP BIGINT,LOOP_NUM BIGINT, remark varchar(100),primary key(id,clientName))");
				LOGGER.info("Table {}InsertProcess create success!",config.DB_SWITCH);
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
	public void saveInsertProcess(int index, double costTime,double totalTime,long errorPoint,String remark) {
		String mysqlSql = String.format("insert into "+config.DB_SWITCH+"InsertProcess values(%d,%s,%d,%f,%f,%f,%d,%s,%s,%d,%d,%d,%d,%d,%d,%s)",
				System.currentTimeMillis(), "'"+Thread.currentThread().getName() +"'", index,
				costTime, totalTime,
				(config.CACHE_NUM * config.SENSOR_NUMBER / costTime) ,
				errorPoint,"'"+config.host+"'","'"+localName+"'",
				config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
				config.CACHE_NUM, config.POINT_STEP,config.LOOP,"'"+remark+"'");
		Statement stat;
		try {
			stat = mysqlConnection.createStatement();
			stat.executeUpdate(mysqlSql);
			stat.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("{} save queryProcess info into mysql failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
			LOGGER.error("{}", mysqlSql);
			e.printStackTrace();
		}
	}
	public void saveInsertResult(long totalPoints, double time, int clientNum,
			long errorNum, double creatSchemaTime, String remark) {
		// TODO Auto-generated method stub
		Statement stat;
		String sql = "";
		try {
			stat = mysqlConnection.createStatement();
			sql = String.format("insert into "+config.DB_SWITCH+"InsertResult values(%d,%d,%f,%d,%f,%d,%f,%s,%s,%d,%d,%d,%d,%d,%d,%s)", 
					System.currentTimeMillis(), totalPoints,time, clientNum, (totalPoints-errorNum)/time, errorNum,creatSchemaTime,"'"+config.host+"'","'"+localName+"'", 
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP,config.LOOP,"'"+remark+"'");
			stat.executeUpdate(sql);
			stat.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("{}将查询结果写入mysql失败，because ：{}", sql,e.getMessage());
			e.printStackTrace();
		}
	}
	public void saveQueryResult(long id, long queryNum, long point, double time, int clientNum, double rate,
			long errorNum, String remark) {
		// TODO Auto-generated method stub
		Statement stat;
		String sql = "";
		try {
			stat = mysqlConnection.createStatement();
			sql = String.format("insert into "+config.DB_SWITCH+"QueryResult values(%d,%d,%d,%f,%d,%f,%d,%s,%s,%d,%d,%d,%s,%d,%f,%b,%s)", id, queryNum, point,
					time, clientNum, rate, errorNum,"'"+config.host+"'","'"+localName+"'", config.QUERY_CHOICE, config.QUERY_SENSOR_NUM,
					config.QUERY_DIVICE_NUM, "'"+config.QUERY_AGGREGATE_FUN+"'", config.QUERY_INTERVAL,
					config.QUERY_LOWER_LIMIT,config.IS_EMPTY_PRECISE_POINT_QUERY,"'"+remark+"'");
			stat.executeUpdate(sql);
			stat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("{}将查询结果写入mysql失败，because ：{}", sql,e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void saveQueryProcess(int index,int point, double time,String remark) {
		String mysqlSql = String.format("insert into "+config.DB_SWITCH+"QueryProcess values(%d,%s,%d,%d,%f,%f,%s,%s,%d,%d,%d,%s,%d,%f,%b,%s)",
				System.currentTimeMillis(), "'"+Thread.currentThread().getName() +"'", index,
				point,time, 
				point/time,
				"'"+config.host+"'","'"+localName+"'",config.QUERY_CHOICE, config.QUERY_SENSOR_NUM,
				config.QUERY_DIVICE_NUM, "'"+config.QUERY_AGGREGATE_FUN+"'", config.QUERY_INTERVAL,
				config.QUERY_LOWER_LIMIT,config.IS_EMPTY_PRECISE_POINT_QUERY,"'"+remark+"'");
		Statement stat;
		try {
			stat = mysqlConnection.createStatement();
			stat.executeUpdate(mysqlSql);
			stat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("{} save queryProcess info into mysql failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
			LOGGER.error("{}", mysqlSql);
			e.printStackTrace();
		}
	}
	public void insertSERVER_MODE(double cpu, double mem, double io, double net_recv, double net_send,String remark){
		Statement stat;
		String sql = "";
		try {
			stat = mysqlConnection.createStatement();
			sql = String.format("insert into SERVER_MODE values(%d,%s,%f,%f,%f,%f,%f,%s)",
					System.currentTimeMillis(),"'"+localName+"'",cpu,mem,io,net_recv,net_send,"'"+remark+"'");
			stat.executeUpdate(sql);
			stat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("{}将SERVER_MODE写入mysql失败，because ：{}", sql,e.getMessage());
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
			return true;
		} else {
			return false;
		}
	}

	public Connection getMysqlConnection() {
		return mysqlConnection;
	}

	public String getLocalName() {
		return localName;
	}
}
