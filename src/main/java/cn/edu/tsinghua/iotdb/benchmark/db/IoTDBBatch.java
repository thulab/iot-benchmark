package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.tsfile.common.utils.Pair;

public class IoTDBBatch implements Callable<Pair<Long, Long>>{
	private Connection connection;
	private Config config;
	private String device;
	private int batch;
	
	public IoTDBBatch(String device, int batch) throws SQLException, ClassNotFoundException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		connection = DriverManager.getConnection(String.format(Constants.URL, Config.newInstance().host, Config.newInstance().port), Constants.USER, Constants.PASSWD);
		config = Config.newInstance();
		this.device = device;
		this.batch = batch;
	}
		
	@Override
	public Pair<Long, Long> call() throws Exception {
		Statement statement = connection.createStatement();
		long startTime = System.currentTimeMillis();
		for(int i = 0; i < config.CACHE_NUM;i++){
			String sql = createSQLStatment(batch, i);
			statement.addBatch(sql);
		}
		statement.executeBatch();
		long endTime = System.currentTimeMillis();
		
		return new Pair<Long, Long>(startTime, endTime);
	}
	
	public String createSQLStatment(int batch, int index){
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
	
	public static void main(String[] args){
		Config config = Config.newInstance();
		config.initDeviceCodes();
		config.initSensorCodes();
		config.initSensorFunction();
		
//		IoTDBBatch batch = new IoTDBBatch("1", 1);
//		System.out.println(batch.createSQLStatment(0, 0));
//		System.out.println(batch.createSQLStatment(0, 1));
		
	}

}
