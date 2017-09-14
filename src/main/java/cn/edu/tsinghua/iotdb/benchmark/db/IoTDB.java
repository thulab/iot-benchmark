package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class IoTDB implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	
	private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=GORILLA";
	private static final String setStorageLevelSQL = "set storage group to %s";
	private Connection connection;

	public IoTDB() throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
	}
	
	@Override
	public void createSchema() throws SQLException {
		for(String device : Config.newInstance().DEVICE_CODES){
			for(String sensor : Config.newInstance().SENSOR_CODES){
				createTimeseries(device, sensor);
			}
			setStorgeGroup(device);
		}
	}

	@Override
	public void insert() {
		// TODO Auto-generated method stub

	}
	
	private void createTimeseries(String device, String sensor) throws SQLException{
		Statement statement = connection.createStatement();
		statement.execute(String.format(createStatementSQL, Constants.ROOT_SERIES_NAME+"."+device+"."+sensor));
		statement.close();
	}
	
	private void setStorgeGroup(String device) throws SQLException{
		Statement statement = connection.createStatement();
		statement.execute(String.format(setStorageLevelSQL, Constants.ROOT_SERIES_NAME+"."+device));
		statement.close();
	}

	@Override
	public void createConnection() throws SQLException {
		connection = DriverManager.getConnection(String.format(Constants.URL, Config.newInstance().host, Config.newInstance().port), Constants.USER, Constants.PASSWD);
	}

	@Override
	public void close() throws SQLException {
		if(connection != null){
			connection.close();
		}
	}

}
