package cn.edu.tsinghua.iotdb.benchmark.conf;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionXml;

public class Config {
	private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
	private String deviceCode;

	public Config(){

	}

	public String host="127.0.0.1";
	public String port="6667";

	/** 设备数量 */
	public int DEVICE_NUMBER = 2;
	/** 测试客户端线程数量 */
	public int CLIENT_NUMBER = 2;
	/** 每个设备的传感器数量 */
	public int SENSOR_NUMBER = 5;
	/** 数据采集步长 */
	public long POINT_STEP = 7000;
	/** 数据发送缓存条数 */
	public int CACHE_NUM = 10;
	/** 存储组数量 */
	public int GROUP_NUMBER = 1;
	/** 数据编码方式 */
	public String ENCODING = "PLAIN";
	/**是否为多设备批插入模式*/
	public boolean MUL_DEV_BATCH = false;
	/**是否为批插入乱序模式*/
	public boolean IS_OVERFLOW = false;
	/**乱序模式*/
	public int OVERFLOW_MODE = 0;
	/**批插入乱序比例*/
	public double OVERFLOW_RATIO = 1.0;

	public double LAMBDA = 3;

	public int MAX_K = 10;

	public boolean IS_RANDOM_TIMESTAMP_INTERVAL = false ;

	public int START_TIMESTAMP_INDEX = 20;

	public boolean USE_OPS = false;

	public double CLIENT_MAX_WRT_RATE = 10000000.0;

	/**系统性能检测时间间隔-2秒*/
 	public int INTERVAL = 0;
 	/**系统性能检测网卡设备名*/
 	public String NET_DEVICE = "e";
 	/**存储系统性能信息的文件路径*/
 	public String SERVER_MODE_INFO_FILE = "";

	/**一个样例数据的存储组名称*/
 	public String STORAGE_GROUP_NAME ;
	/**一个样例数据的时序名称*/
 	public String TIMESERIES_NAME ;
	/**一个时序的数据类型*/
 	public String TIMESERIES_TYPE ;
	/**时序数据取值范围*/
	public String TIMESERIES_VALUE_SCOPE ;
	/**样例数据生成路径及文件名*/
	public String GEN_DATA_FILE_PATH = "/home/liurui/sampleData";
	/**上一次结果的日志路径*/
	public String LAST_RESULT_PATH = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/logs";

	/**存放SQL语句文件的完整路径*/
	public String SQL_FILE = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/SQLFile";

	/** 文件的名字 */
	public String FILE_PATH ;
	/** 是否从文件读取数据*/
	public boolean READ_FROM_FILE = false;
	/** 一次插入到数据库的条数 */
	public int BATCH_OP_NUM = 100;

	public boolean TAG_PATH = true;

	public String LOG_STOP_FLAG_PATH;

	public int STORE_MODE = 1;

	public long LOOP = 10000;

	/** 数据采集丢失率 */
	public double POINT_LOSE_RATIO = 0.01;
	// ============各函数比例start============//FIXME 传参数时加上这几个参数
	/** 线性 默认 9个 0.054 */
	public double LINE_RATIO = 0.054;
	/** 傅里叶函数 6个 0.036 */
	// public static double SIN_RATIO=0.386;//0.036
	public double SIN_RATIO = 0.036;// 0.036
	/** 方波 9个 0.054 */
	public double SQUARE_RATIO = 0.054;
	/** 随机数 默认 86个 0.512 */
	public double RANDOM_RATIO = 0.512;
	/** 常数 默认 58个 0.352 */
	// public static double CONSTANT_RATIO= 0.002;//0.352
	public double CONSTANT_RATIO = 0.352;// 0.352

	// ============各函数比例end============

	/** 内置函数参数 */
	public List<FunctionParam> LINE_LIST = new ArrayList<FunctionParam>();
	public List<FunctionParam> SIN_LIST = new ArrayList<FunctionParam>();
	public List<FunctionParam> SQUARE_LIST = new ArrayList<FunctionParam>();
	public List<FunctionParam> RANDOM_LIST = new ArrayList<FunctionParam>();
	public List<FunctionParam> CONSTANT_LIST = new ArrayList<FunctionParam>();
	/** 设备编号 */
	public List<String> DEVICE_CODES = new ArrayList<String>();
	/** 传感器编号 */
	public List<String> SENSOR_CODES = new ArrayList<String>();
	/** 设备_传感器 时间偏移量 */
	public Map<String, Long> SHIFT_TIME_MAP = new HashMap<String, Long>();
	/** 传感器对应的函数 */
	public Map<String, FunctionParam> SENSOR_FUNCTION = new HashMap<String, FunctionParam>();

	/** 历史数据开始时间 */
	public long HISTORY_START_TIME;
	/** 历史数据结束时间 */
	public long HISTORY_END_TIME;

	// 负载生成器参数 start
	/** LoadBatchId 批次id */
	public Long PERFORM_BATCH_ID;

	// 负载测试完是否删除数据
	public boolean IS_DELETE_DATA = false;
	public double WRITE_RATIO = 0.2;
	public double SIMPLE_QUERY_RATIO = 0.2;
	public double MAX_QUERY_RATIO = 0.2;
	public double MIN_QUERY_RATIO = 0.2;
	public double AVG_QUERY_RATIO = 0.2;
	public double COUNT_QUERY_RATIO = 0.2;
	public double SUM_QUERY_RATIO = 0.2;
	public double RANDOM_INSERT_RATIO = 0.2;
	public double UPDATE_RATIO = 0.2;

	//iotDB查询测试相关参数
	public int QUERY_SENSOR_NUM = 1;
	public int QUERY_DEVICE_NUM = 1;
	public int QUERY_CHOICE = 1;
	public String QUERY_AGGREGATE_FUN = "";
	public long QUERY_INTERVAL = DEVICE_NUMBER * POINT_STEP;
	public double QUERY_LOWER_LIMIT = 0;
	public boolean IS_EMPTY_PRECISE_POINT_QUERY = false;
	public long TIME_UNIT = QUERY_INTERVAL / 2;
	public long QUERY_SEED = 1516580959202L;
	public int QUERY_LIMIT_N = 1;
	public int QUERY_LIMIT_OFFSET = 0;
	public int QUERY_SLIMIT_N = 1;
	public int QUERY_SLIMIT_OFFSET = 0;
	public boolean CREATE_SCHEMA = true;

	//mysql相关参数
	// mysql服务器URL以及用户名密码
	public String MYSQL_URL = "jdbc:mysql://166.111.141.168:3306/benchmark?"
			+ "user=root&password=Ise_Nel_2017&useUnicode=true&characterEncoding=UTF8&useSSL=false";
	//是否将结果写入mysql
	public boolean IS_USE_MYSQL = false;
	public boolean IS_SAVE_DATAMODEL = false;

	public String REMARK = "";
	public String VERSION = "";

	// InfluxDB参数
	// Influx服务器URL
	public String INFLUX_URL = "http://localhost:8086";
	// 使用的数据库名
	public String INFLUX_DB_NAME = "test";
	
	//Opentsdb参数
	public String OPENTSDB_URL="http://localhost:4242";
	
	// 使用的数据库
	public String DB_SWITCH = "IoTDB";

	//benchmark 运行模式
	public String BENCHMARK_WORK_MODE="";
	//the file path of import data
	public String IMPORT_DATA_FILE_PATH = "";
	//import csv数据文件时的BATCH
	public int BATCH_EXECUTE_COUNT = 5000;
	//mataData文件路径
	public String METADATA_FILE_PATH = "";

	public void updateLoadTypeRatio(double wr, double rir, double mqr, double sqr, double ur) {
		WRITE_RATIO = wr;
		RANDOM_INSERT_RATIO = rir;
		MAX_QUERY_RATIO = mqr;
		SIMPLE_QUERY_RATIO = sqr;
		UPDATE_RATIO = ur;
		if (WRITE_RATIO < 0 || RANDOM_INSERT_RATIO < 0 || MAX_QUERY_RATIO < 0 || SIMPLE_QUERY_RATIO < 0
				|| UPDATE_RATIO < 0) {
			LOGGER.error("some of load rate cannot less than 0");
			System.exit(0);
		}
		if (WRITE_RATIO == 0 && RANDOM_INSERT_RATIO == 0 && MAX_QUERY_RATIO == 0 && SIMPLE_QUERY_RATIO == 0
				&& UPDATE_RATIO == 0) {
			WRITE_RATIO = 0.2;
			SIMPLE_QUERY_RATIO = 0.2;
			MAX_QUERY_RATIO = 0.2;
			MIN_QUERY_RATIO = 0.2;
			AVG_QUERY_RATIO = 0.2;
			COUNT_QUERY_RATIO = 0.2;
			SUM_QUERY_RATIO = 0.2;
			RANDOM_INSERT_RATIO = 0.2;
			UPDATE_RATIO = 0.2;
		}
	}

	public void initInnerFucntion() {
		FunctionXml xml = null;
		try {
			InputStream input = Function.class.getResourceAsStream("function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class, FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			xml = (FunctionXml) unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for (FunctionParam param : xmlFuctions) {
			if (param.getFunctionType().indexOf("_mono_k") != -1) {
				LINE_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_mono") != -1) {
				// 如果min==max则为常数，系统没有非常数的
				if (param.getMin() == param.getMax()) {
					CONSTANT_LIST.add(param);
				}
			} else if (param.getFunctionType().indexOf("_sin") != -1) {
				SIN_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_square") != -1) {
				SQUARE_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_random") != -1) {
				RANDOM_LIST.add(param);
			}
		}
	}

	/**
	 * 初始化传感器函数 Constants.SENSOR_FUNCTION
	 */
	public void initSensorFunction() {
		// 根据传进来的各个函数比例进行配置
		double sumRatio = CONSTANT_RATIO + LINE_RATIO + RANDOM_RATIO + SIN_RATIO + SQUARE_RATIO;
		if (sumRatio != 0 && CONSTANT_RATIO >= 0 && LINE_RATIO >= 0 && RANDOM_RATIO >= 0 && SIN_RATIO >= 0
				&& SQUARE_RATIO >= 0) {
			double constantArea = CONSTANT_RATIO / sumRatio;
			double lineArea = constantArea + LINE_RATIO / sumRatio;
			double randomArea = lineArea + RANDOM_RATIO / sumRatio;
			double sinArea = randomArea + SIN_RATIO / sumRatio;
			double squareArea = sinArea + SQUARE_RATIO / sumRatio;
			Random r = new Random();
			for (int i = 0; i < SENSOR_NUMBER; i++) {
				double property = r.nextDouble();
				FunctionParam param = null;
				Random fr = new Random();
				double middle = fr.nextDouble();
				if (property >= 0 && property < constantArea) {// constant
					int index = (int) (middle * CONSTANT_LIST.size());
					param = CONSTANT_LIST.get(index);
				}
				if (property >= constantArea && property < lineArea) {// line
					int index = (int) (middle * LINE_LIST.size());
					param = CONSTANT_LIST.get(index);
				}
				if (property >= lineArea && property < randomArea) {// random
					int index = (int) (middle * RANDOM_LIST.size());
					param = RANDOM_LIST.get(index);
				}
				if (property >= randomArea && property < sinArea) {// sin
					int index = (int) (middle * SIN_LIST.size());
					param = SIN_LIST.get(index);
				}
				if (property >= sinArea && property < squareArea) {// square
					int index = (int) (middle * SQUARE_LIST.size());
					param = SQUARE_LIST.get(index);
				}
				if (param == null) {
					System.err.println(" initSensorFunction() 初始化函数比例有问题！");
					System.exit(0);
				}
				SENSOR_FUNCTION.put(SENSOR_CODES.get(i), param);
			}
		} else {
			System.err.println("function ration must >=0 and sum>0");
			System.exit(0);
		}
	}

	/**
	 * 根据传感器数，初始化传感器编号
	 *
	 * @param sensorSum
	 * @return
	 */
	public List<String> initSensorCodes() {
		for (int i = 0; i < SENSOR_NUMBER; i++) {
			String sensorCode = "s_" + i;
			SENSOR_CODES.add(sensorCode);
		}
		return SENSOR_CODES;
	}

	/**
	 * 根据设备数，初始化设备编号
	 *
	 * @param deviceSum
	 * @return
	 */
	public List<String> initDeviceCodes() {
		for (int i = 0; i < DEVICE_NUMBER; i++) {
			String deviceCode = "d_" + i;
			DEVICE_CODES.add(deviceCode);
		}
		return DEVICE_CODES;
	}

	public String getSensorCodeByRandom() {
		List<String> sensors = SENSOR_CODES;
		int size = sensors.size();
		Random r = new Random();
		return sensors.get(r.nextInt(size));
	}

	public String getDeviceCodeByRandom() {
		List<String> devices = DEVICE_CODES;
		int size = devices.size();
		Random r = new Random();
		return devices.get(r.nextInt(size));
	}

	public static void main(String[] args) {
		// Config config = Config.newInstance();

	}
}
