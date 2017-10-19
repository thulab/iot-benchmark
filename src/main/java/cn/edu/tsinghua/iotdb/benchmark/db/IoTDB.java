package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class IoTDB implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	
	private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=GORILLA";
//	private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=RLE";
	private static final String setStorageLevelSQL = "set storage group to %s";
	private Connection connection;
	private Config config;

	public IoTDB() throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		config = ConfigDescriptor.getInstance().getConfig();
	}
	
	@Override
	public void createSchema() throws SQLException {
		for(String device : config.DEVICE_CODES){
			setStorgeGroup(device);
			for(String sensor : config.SENSOR_CODES){
				createTimeseries(device, sensor);
			}
		}
	}

	@Override
	public void insertOneBatch(String device, int batchIndex) {
		Statement statement;
		try {
			statement = connection.createStatement();
			for(int i = 0; i < config.CACHE_NUM;i++){
				String sql = createSQLStatment(batchIndex, i, device);
				statement.addBatch(sql);
			}
			long startTime = System.currentTimeMillis();

			statement.executeBatch();
			statement.clearBatch();
			statement.close();
			long endTime = System.currentTimeMillis();
			LOGGER.info("{} execute {} batch, it costs {}ms, throughput {} points/ms", 
					Thread.currentThread().getName(),
					batchIndex,
					endTime-startTime,
					config.CACHE_NUM*config.SENSOR_NUMBER / (double) (endTime-startTime));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createTimeseries(String device, String sensor){
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(String.format(createStatementSQL, Constants.ROOT_SERIES_NAME+"."+device+"."+sensor));
			statement.close();
		} catch (SQLException e) {
			LOGGER.warn(e.getMessage());
		}

	}
	
	private void setStorgeGroup(String device) {
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(String.format(setStorageLevelSQL, Constants.ROOT_SERIES_NAME+"."+device));
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void init() throws SQLException {
		connection = DriverManager.getConnection(String.format(Constants.URL,config.host, config.port), Constants.USER, Constants.PASSWD);
	}

	@Override
	public void close() throws SQLException {
		if(connection != null){
			connection.close();
		}
	}
	
	private String createSQLStatment(int batch, int index, String device){
		StringBuilder builder = new StringBuilder();
		builder.append("insert into ").append(Constants.ROOT_SERIES_NAME).append(".").append(device).append("(timestamp");
		
		for(String sensor: config.SENSOR_CODES){
			builder.append(",").append(sensor);
		}
		builder.append(") values(");
		long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
		builder.append(currentTime);
		for(String sensor: config.SENSOR_CODES){
			FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
			builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
		}
		builder.append(")");
		return builder.toString();
	}

}
