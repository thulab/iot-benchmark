package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.workload.reader.DataSet;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
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
		config.initInnerFunction();
		config.initDeviceCodes();
		config.initSensorCodes();
		config.initSensorFunction();
		config.initRealDataSetSchema();
	}

	public static final ConfigDescriptor getInstance() {
		return ConfigDescriptorHolder.INSTANCE;
	}

	public Config getConfig(){
		return config;
	}

	private void loadProps() {
		String url = System.getProperty(Constants.BENCHMARK_CONF, "conf/config.properties");
		if (url != null) {
			InputStream inputStream;
			try {
				inputStream = new FileInputStream(new File(url));
			} catch (FileNotFoundException e) {
				LOGGER.warn("Fail to find config file {}", url);
				return;
			}
			Properties properties = new Properties();
			try {
				properties.load(inputStream);
				config.HOST = properties.getProperty("HOST", "no HOST");
				config.PORT = properties.getProperty("PORT", "no PORT");
				config.DEVICE_NUMBER = Integer.parseInt(properties.getProperty("DEVICE_NUMBER", config.DEVICE_NUMBER+""));
				config.SENSOR_NUMBER = Integer.parseInt(properties.getProperty("SENSOR_NUMBER", config.SENSOR_NUMBER+""));

				config.FILE_PATH = properties.getProperty("FILE_PATH", "no file");

				String dataset = properties.getProperty("DATA_SET", "NULL");
				switch (properties.getProperty("DATA_SET", "REDD")) {
					case "GEOLIFE": config.DATA_SET = DataSet.GEOLIFE; break;
					case "REDD": config.DATA_SET = DataSet.REDD; break;
					case "TDRIVE": config.DATA_SET = DataSet.TDRIVE; break;
					default: throw new RuntimeException("not support dataset: " + dataset);
				}

				config.POINT_STEP = Long.parseLong(properties.getProperty("POINT_STEP", config.POINT_STEP+""));
				config.BATCH_SIZE = Integer.parseInt(properties.getProperty("BATCH_SIZE", config.BATCH_SIZE +""));
				config.SG_STRATEGY = properties.getProperty("SG_STRATEGY", "hash");
				config.LOOP = Long.parseLong(properties.getProperty("LOOP", config.LOOP+""));
				config.LINE_RATIO = Double.parseDouble(properties.getProperty("LINE_RATIO", config.LINE_RATIO+""));
				config.SIN_RATIO = Double.parseDouble(properties.getProperty("SIN_RATIO", config.SIN_RATIO+""));
				config.SQUARE_RATIO = Double.parseDouble(properties.getProperty("SQUARE_RATIO", config.SQUARE_RATIO+""));
				config.RANDOM_RATIO = Double.parseDouble(properties.getProperty("RANDOM_RATIO", config.RANDOM_RATIO+""));
				config.CONSTANT_RATIO = Double.parseDouble(properties.getProperty("CONSTANT_RATIO", config.CONSTANT_RATIO+""));

				config.INTERVAL = Integer.parseInt(properties.getProperty("INTERVAL", config.INTERVAL+""));
				config.CLIENT_NUMBER = Integer.parseInt(properties.getProperty("CLIENT_NUMBER", config.CLIENT_NUMBER+""));
				config.GROUP_NUMBER = Integer.parseInt(properties.getProperty("GROUP_NUMBER", config.GROUP_NUMBER+""));

				config.DB_URL = properties.getProperty("DB_URL", "localhost");
				config.DB_NAME = properties.getProperty("DB_NAME", "test");
				config.DB_SWITCH = properties.getProperty("DB_SWITCH", Constants.DB_IOT);
				config.INSERT_MODE = properties.getProperty("INSERT_MODE", config.INSERT_MODE);

				config.QUERY_SENSOR_NUM  = Integer.parseInt(properties.getProperty("QUERY_SENSOR_NUM", config.QUERY_SENSOR_NUM+""));
				config.QUERY_DEVICE_NUM  = Integer.parseInt(properties.getProperty("QUERY_DEVICE_NUM", config.QUERY_DEVICE_NUM+""));
				config.QUERY_AGGREGATE_FUN = properties.getProperty("QUERY_AGGREGATE_FUN", config.QUERY_AGGREGATE_FUN);
				config.QUERY_INTERVAL = Long.parseLong(properties.getProperty("QUERY_INTERVAL", config.QUERY_INTERVAL+""));
				config.QUERY_LOWER_LIMIT = Double.parseDouble(properties.getProperty("QUERY_LOWER_LIMIT", config.QUERY_LOWER_LIMIT+""));
				config.QUERY_SEED = Long.parseLong(properties.getProperty("QUERY_SEED", config.QUERY_SEED+""));
				config.IS_EMPTY_PRECISE_POINT_QUERY = Boolean.parseBoolean(properties.getProperty("IS_EMPTY_PRECISE_POINT_QUERY", config.IS_EMPTY_PRECISE_POINT_QUERY+""));
				config.REMARK = properties.getProperty("REMARK", "-");

				config.TEST_DATA_STORE_PORT = properties.getProperty("TEST_DATA_STORE_PORT", config.TEST_DATA_STORE_PORT);
				config.TEST_DATA_STORE_DB = properties.getProperty("TEST_DATA_STORE_DB", config.TEST_DATA_STORE_DB);
				config.TEST_DATA_STORE_IP = properties.getProperty("TEST_DATA_STORE_IP", config.TEST_DATA_STORE_IP);
				config.TEST_DATA_STORE_USER = properties.getProperty("TEST_DATA_STORE_USER", config.TEST_DATA_STORE_USER);
				config.TEST_DATA_STORE_PW = properties.getProperty("TEST_DATA_STORE_PW", config.TEST_DATA_STORE_PW);
				config.TIME_UNIT = Long.parseLong(properties.getProperty("TIME_UNIT", config.TIME_UNIT+""));
				config.VERSION = properties.getProperty("VERSION", "");

				config.DB_DATA_PATH = properties.getProperty("DB_DATA_PATH", "/home/liurui");
				String dataDir = properties.getProperty("IOTDB_DATA_DIR", "/home/liurui/data/data");
				Collections.addAll(config.IOTDB_DATA_DIR, dataDir.split(","));
				String walDir = properties.getProperty("IOTDB_WAL_DIR", "/home/liurui/data/wal");
				Collections.addAll(config.IOTDB_WAL_DIR, walDir.split(","));
				String systemDir = properties.getProperty("IOTDB_SYSTEM_DIR", "/home/liurui/data/system");
				Collections.addAll(config.IOTDB_SYSTEM_DIR, systemDir.split(","));
				for (String data_ : config.IOTDB_DATA_DIR) {
					config.SEQUENCE_DIR.add(data_ + "/sequence");
					config.UNSEQUENCE_DIR.add(data_ + "/unsequence");
				}
				config.ENCODING = properties.getProperty("ENCODING", "PLAIN");
				config.TEST_DATA_PERSISTENCE = properties.getProperty("TEST_DATA_PERSISTENCE", "None");
				config.NUMBER_OF_DECIMAL_DIGIT = Integer.parseInt(properties.getProperty("NUMBER_OF_DECIMAL_DIGIT", config.NUMBER_OF_DECIMAL_DIGIT+""));
				config.LOG_PRINT_INTERVAL = Integer.parseInt(properties.getProperty("LOG_PRINT_INTERVAL", config.LOG_PRINT_INTERVAL+""));
				config.MUL_DEV_BATCH = Boolean.parseBoolean(properties.getProperty("MUL_DEV_BATCH", config.MUL_DEV_BATCH+""));
				config.IS_QUIET_MODE = Boolean.parseBoolean(properties.getProperty("IS_QUIET_MODE", config.IS_QUIET_MODE+""));
				config.NET_DEVICE = properties.getProperty("NET_DEVICE", "e");
				config.WORKLOAD_BUFFER_SIZE = Integer.parseInt(properties.getProperty("WORKLOAD_BUFFER_SIZE", config.WORKLOAD_BUFFER_SIZE+""));
				config.STORAGE_GROUP_NAME = properties.getProperty("STORAGE_GROUP_NAME", config.STORAGE_GROUP_NAME);
				config.TIMESERIES_NAME = properties.getProperty("TIMESERIES_NAME", config.TIMESERIES_NAME);
				config.TIMESERIES_TYPE = properties.getProperty("TIMESERIES_TYPE", config.TIMESERIES_TYPE);
				config.TIMESERIES_VALUE_SCOPE = properties.getProperty("TIMESERIES_VALUE_SCOPE", config.TIMESERIES_VALUE_SCOPE);
				config.IS_OVERFLOW = Boolean.parseBoolean(properties.getProperty("IS_OVERFLOW", config.IS_OVERFLOW+""));
				config.OVERFLOW_RATIO = Double.parseDouble(properties.getProperty("OVERFLOW_RATIO", config.OVERFLOW_RATIO+""));

				config.BENCHMARK_WORK_MODE = properties.getProperty("BENCHMARK_WORK_MODE", "");
				config.IMPORT_DATA_FILE_PATH = properties.getProperty("IMPORT_DATA_FILE_PATH", "");
				config.METADATA_FILE_PATH= properties.getProperty("METADATA_FILE_PATH", "");
				config.BATCH_EXECUTE_COUNT = Integer.parseInt(properties.getProperty("BATCH_EXECUTE_COUNT", config.BATCH_EXECUTE_COUNT+""));
				config.OVERFLOW_MODE = Integer.parseInt(properties.getProperty("OVERFLOW_MODE", config.OVERFLOW_MODE+""));
				config.MAX_K = Integer.parseInt(properties.getProperty("MAX_K", config.MAX_K+""));
				config.LAMBDA = Double.parseDouble(properties.getProperty("LAMBDA", config.LAMBDA+""));
				config.IS_RANDOM_TIMESTAMP_INTERVAL = Boolean.parseBoolean(properties.getProperty("IS_RANDOM_TIMESTAMP_INTERVAL", config.IS_RANDOM_TIMESTAMP_INTERVAL+""));
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
				config.START_TIME = properties.getProperty("START_TIME", config.START_TIME);
				config.INIT_WAIT_TIME = Long.parseLong(properties.getProperty("INIT_WAIT_TIME", config.INIT_WAIT_TIME+""));
				config.DATA_SEED = Long.parseLong(properties.getProperty("DATA_SEED", config.DATA_SEED+""));
				config.LIMIT_CLAUSE_MODE = Integer.parseInt(properties.getProperty("LIMIT_CLAUSE_MODE", config.LIMIT_CLAUSE_MODE + ""));
				config.STEP_SIZE = Integer.parseInt(properties.getProperty("STEP_SIZE", config.STEP_SIZE+""));
				config.IS_CLIENT_BIND = Boolean.parseBoolean(properties.getProperty("IS_CLIENT_BIND", config.IS_CLIENT_BIND+""));
				config.IS_DELETE_DATA = Boolean.parseBoolean(properties.getProperty("IS_DELETE_DATA", config.IS_DELETE_DATA+""));
				config.REAL_QUERY_START_TIME = Long.parseLong(properties.getProperty("REAL_QUERY_START_TIME", config.REAL_QUERY_START_TIME+""));
				config.REAL_QUERY_STOP_TIME = Long.parseLong(properties.getProperty("REAL_QUERY_STOP_TIME", config.REAL_QUERY_STOP_TIME+""));
				config.USE_CLUSTER=Boolean.parseBoolean(properties.getProperty("USE_CLUSTER",config.USE_CLUSTER + ""));
				if (config.USE_CLUSTER){
					config.FIRST_INDEX = Integer.parseInt(properties.getProperty("FIRST_INDEX",config.FIRST_INDEX + ""));
					config.FIRST_DEVICE_INDEX = config.FIRST_INDEX * config.DEVICE_NUMBER;
				}
				else {
					config.FIRST_DEVICE_INDEX = 0;
				}
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

}
