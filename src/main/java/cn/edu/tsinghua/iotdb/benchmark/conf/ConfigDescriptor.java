package cn.edu.tsinghua.iotdb.benchmark.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

	private Config config;

	private static class ConfigDescriptorHolder {
		private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
	}

	private ConfigDescriptor() {
		config = new Config();
		loadProps();
		config.initInnerFucntion();
		config.initDeviceCodes();
		config.initSensorCodes();
		config.initSensorFunction();
	}

	public static final ConfigDescriptor getInstance() {
		return ConfigDescriptorHolder.INSTANCE;
	}

	public Config getConfig(){
		return config;
	}

	private void loadProps() {
		String url = System.getProperty(Constants.BENCHMARK_CONF, null);
		if (url != null) {
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(new File(url));
			} catch (FileNotFoundException e) {
				LOGGER.warn("Fail to find config file {}", url);
				return;
			}
			Properties properties = new Properties();
			try {
				properties.load(inputStream);
				config.host = properties.getProperty("HOST", "no host");
				config.port = properties.getProperty("PORT", "no port");
				config.DEVICE_NUMBER = Integer.parseInt(properties.getProperty("DEVICE_NUMBER", config.DEVICE_NUMBER+""));
				config.SENSOR_NUMBER = Integer.parseInt(properties.getProperty("SENSOR_NUMBER", config.SENSOR_NUMBER+""));

				config.POINT_STEP = Long.parseLong(properties.getProperty("POINT_STEP", config.POINT_STEP+""));
				config.CACHE_NUM = Integer.parseInt(properties.getProperty("CACHE_NUM", config.CACHE_NUM+""));
				config.LOOP = Long.parseLong(properties.getProperty("LOOP", config.LOOP+""));
				config.LINE_RATIO = Double.parseDouble(properties.getProperty("LINE_RATIO", config.LINE_RATIO+""));
				config.SIN_RATIO = Double.parseDouble(properties.getProperty("SIN_RATIO", config.SIN_RATIO+""));
				config.SQUARE_RATIO = Double.parseDouble(properties.getProperty("SQUARE_RATIO", config.SQUARE_RATIO+""));
				config.RANDOM_RATIO = Double.parseDouble(properties.getProperty("RANDOM_RATIO", config.RANDOM_RATIO+""));
				config.CONSTANT_RATIO = Double.parseDouble(properties.getProperty("CONSTANT_RATIO", config.CONSTANT_RATIO+""));

				config.READ_FROM_FILE = Boolean.parseBoolean(properties.getProperty("READ_FROM_FILE", config.READ_FROM_FILE+""));
				config.FILE_PATH = properties.getProperty("FILE_PATH", "no file");
				config.BATCH_OP_NUM = Integer.parseInt(properties.getProperty("BATCH_OP_NUM", config.BATCH_OP_NUM+""));
				config.TAG_PATH = Boolean.parseBoolean(properties.getProperty("TAG_PATH", config.TAG_PATH+""));
				config.STORE_MODE = Integer.parseInt(properties.getProperty("STORE_MODE", config.STORE_MODE+""));
				config.INTERVAL = Integer.parseInt(properties.getProperty("INTERVAL", config.INTERVAL+""));
				config.CLIENT_NUMBER = Integer.parseInt(properties.getProperty("CLIENT_NUMBER", config.CLIENT_NUMBER+""));
				config.GROUP_NUMBER = Integer.parseInt(properties.getProperty("GROUP_NUMBER", config.GROUP_NUMBER+""));

				config.DB_URL = properties.getProperty("DB_URL", "localhost");
				config.DB_NAME = properties.getProperty("DB_NAME", "test");

				config.DB_SWITCH = properties.getProperty("DB_SWITCH", Constants.DB_IOT);

				config.QUERY_CHOICE = Integer.parseInt(properties.getProperty("QUERY_CHOICE", config.QUERY_CHOICE+""));
				config.QUERY_SENSOR_NUM  = Integer.parseInt(properties.getProperty("QUERY_SENSOR_NUM", config.QUERY_SENSOR_NUM+""));
				config.QUERY_DEVICE_NUM  = Integer.parseInt(properties.getProperty("QUERY_DEVICE_NUM", config.QUERY_DEVICE_NUM+""));
				config.QUERY_AGGREGATE_FUN = properties.getProperty("QUERY_AGGREGATE_FUN", config.QUERY_AGGREGATE_FUN);
				config.QUERY_INTERVAL = Long.parseLong(properties.getProperty("QUERY_INTERVAL", config.QUERY_INTERVAL+""));
				config.QUERY_LOWER_LIMIT = Double.parseDouble(properties.getProperty("QUERY_LOWER_LIMIT", config.QUERY_LOWER_LIMIT+""));
				config.QUERY_SEED = Long.parseLong(properties.getProperty("QUERY_SEED", config.QUERY_SEED+""));
				config.IS_EMPTY_PRECISE_POINT_QUERY = Boolean.parseBoolean(properties.getProperty("IS_EMPTY_PRECISE_POINT_QUERY", config.IS_EMPTY_PRECISE_POINT_QUERY+""));
				config.REMARK = properties.getProperty("REMARK", "-");


				config.MYSQL_URL = properties.getProperty("MYSQL_URL", "jdbc:mysql://166.111.141.168:3306/benchmark?"
						+ "user=root&password=Ise_Nel_2017&useUnicode=true&characterEncoding=UTF8&useSSL=false");
				config.IS_USE_MYSQL = Boolean.parseBoolean(properties.getProperty("IS_USE_MYSQL", config.IS_USE_MYSQL+""));
				config.IS_SAVE_DATAMODEL = Boolean.parseBoolean(properties.getProperty("IS_SAVE_DATAMODEL", config.IS_SAVE_DATAMODEL+""));
				config.TIME_UNIT = Long.parseLong(properties.getProperty("TIME_UNIT", config.TIME_UNIT+""));
				config.VERSION = properties.getProperty("VERSION", "");

				config.LOG_STOP_FLAG_PATH = properties.getProperty("LOG_STOP_FLAG_PATH", "/home/liurui");
				config.ENCODING = properties.getProperty("ENCODING", "PLAIN");
				config.MUL_DEV_BATCH = Boolean.parseBoolean(properties.getProperty("MUL_DEV_BATCH", config.MUL_DEV_BATCH+""));
				config.NET_DEVICE = properties.getProperty("NET_DEVICE", "e");
				config.SERVER_MODE_INFO_FILE = properties.getProperty("SERVER_MODE_INFO_FILE", "");

				config.STORAGE_GROUP_NAME = properties.getProperty("STORAGE_GROUP_NAME", config.STORAGE_GROUP_NAME);
				config.TIMESERIES_NAME = properties.getProperty("TIMESERIES_NAME", config.TIMESERIES_NAME);
				config.TIMESERIES_TYPE = properties.getProperty("TIMESERIES_TYPE", config.TIMESERIES_TYPE);
				config.TIMESERIES_VALUE_SCOPE = properties.getProperty("TIMESERIES_VALUE_SCOPE", config.TIMESERIES_VALUE_SCOPE);
				config.GEN_DATA_FILE_PATH = properties.getProperty("GEN_DATA_FILE_PATH", config.GEN_DATA_FILE_PATH);
				config.IS_OVERFLOW = Boolean.parseBoolean(properties.getProperty("IS_OVERFLOW", config.IS_OVERFLOW+""));
				config.OVERFLOW_RATIO = Double.parseDouble(properties.getProperty("OVERFLOW_RATIO", config.OVERFLOW_RATIO+""));
				config.LAST_RESULT_PATH = properties.getProperty("LAST_RESULT_PATH", config.LAST_RESULT_PATH);
				config.SQL_FILE = properties.getProperty("SQL_FILE", config.SQL_FILE);

				config.BENCHMARK_WORK_MODE = properties.getProperty("BENCHMARK_WORK_MODE", "");
				config.IMPORT_DATA_FILE_PATH = properties.getProperty("IMPORT_DATA_FILE_PATH", "");
				config.METADATA_FILE_PATH= properties.getProperty("METADATA_FILE_PATH", "");
				config.BATCH_EXECUTE_COUNT = Integer.parseInt(properties.getProperty("BATCH_EXECUTE_COUNT", config.BATCH_EXECUTE_COUNT+""));
				config.OVERFLOW_MODE = Integer.parseInt(properties.getProperty("OVERFLOW_MODE", config.OVERFLOW_MODE+""));
				config.MAX_K = Integer.parseInt(properties.getProperty("MAX_K", config.MAX_K+""));
				config.LAMBDA = Double.parseDouble(properties.getProperty("LAMBDA", config.LAMBDA+""));
				config.IS_RANDOM_TIMESTAMP_INTERVAL = Boolean.parseBoolean(properties.getProperty("IS_RANDOM_TIMESTAMP_INTERVAL", config.IS_RANDOM_TIMESTAMP_INTERVAL+""));
				config.START_TIMESTAMP_INDEX = Integer.parseInt(properties.getProperty("START_TIMESTAMP_INDEX", config.START_TIMESTAMP_INDEX+""));
				config.USE_OPS = Boolean.parseBoolean(properties.getProperty("USE_OPS", config.USE_OPS+""));
				config.CLIENT_MAX_WRT_RATE = Double.parseDouble(properties.getProperty("CLIENT_MAX_WRT_RATE", config.CLIENT_MAX_WRT_RATE+""));
				config.QUERY_LIMIT_N = Integer.parseInt(properties.getProperty("QUERY_LIMIT_N", config.QUERY_LIMIT_N+""));
				config.QUERY_LIMIT_OFFSET = Integer.parseInt(properties.getProperty("QUERY_LIMIT_OFFSET", config.QUERY_LIMIT_OFFSET+""));
				config.QUERY_SLIMIT_N = Integer.parseInt(properties.getProperty("QUERY_SLIMIT_N", config.QUERY_SLIMIT_N+""));
				config.QUERY_SLIMIT_OFFSET = Integer.parseInt(properties.getProperty("QUERY_SLIMIT_OFFSET", config.QUERY_SLIMIT_OFFSET+""));
				config.CREATE_SCHEMA = Boolean.parseBoolean(properties.getProperty("CREATE_SCHEMA", config.CREATE_SCHEMA+""));
				config.DATA_TYPE = properties.getProperty("DATA_TYPE", "FLOAT");
				config.COMPRESSOR = properties.getProperty("COMPRESSOR", "UNCOMPRESSOR");
				config.OPERATION_PROPORTION = properties.getProperty("OPERATION_PROPORTION", config.OPERATION_PROPORTION);
				config.INIT_WAIT_TIME = Long.parseLong(properties.getProperty("INIT_WAIT_TIME", config.INIT_WAIT_TIME+""));
				config.DATA_SEED = Long.parseLong(properties.getProperty("DATA_SEED", config.DATA_SEED+""));
				config.LIMIT_CLAUSE_MODE = Integer.parseInt(properties.getProperty("LIMIT_CLAUSE_MODE", config.LIMIT_CLAUSE_MODE + ""));
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					LOGGER.error("Fail to close config file input stream", e);
				}
			}
		} else {
			LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
		}
	}



	public static void main(String[] args) {


	}

}
