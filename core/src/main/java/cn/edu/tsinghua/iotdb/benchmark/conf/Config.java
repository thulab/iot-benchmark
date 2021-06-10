package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionXml;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.DataSet;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

public class Config {

	public String getHOST() {
		return HOST;
	}

	public void setHOST(String HOST) {
		this.HOST = HOST;
	}

	public String getPORT() {
		return PORT;
	}

	public void setPORT(String PORT) {
		this.PORT = PORT;
	}

	public boolean isENABLE_DOUBLE_INSERT() {
		return ENABLE_DOUBLE_INSERT;
	}

	public void setENABLE_DOUBLE_INSERT(boolean ENABLE_DOUBLE_INSERT) {
		this.ENABLE_DOUBLE_INSERT = ENABLE_DOUBLE_INSERT;
	}

	public String getANOTHER_HOST() {
		return ANOTHER_HOST;
	}

	public void setANOTHER_HOST(String ANOTHER_HOST) {
		this.ANOTHER_HOST = ANOTHER_HOST;
	}

	public String getANOTHER_PORT() {
		return ANOTHER_PORT;
	}

	public void setANOTHER_PORT(String ANOTHER_PORT) {
		this.ANOTHER_PORT = ANOTHER_PORT;
	}

	public String getKAFKA_LOCATION() {
		return KAFKA_LOCATION;
	}

	public void setKAFKA_LOCATION(String KAFKA_LOCATION) {
		this.KAFKA_LOCATION = KAFKA_LOCATION;
	}

	public String getZOOKEEPER_LOCATION() {
		return ZOOKEEPER_LOCATION;
	}

	public void setZOOKEEPER_LOCATION(String ZOOKEEPER_LOCATION) {
		this.ZOOKEEPER_LOCATION = ZOOKEEPER_LOCATION;
	}

	public String getTOPIC_NAME() {
		return TOPIC_NAME;
	}

	public void setTOPIC_NAME(String TOPIC_NAME) {
		this.TOPIC_NAME = TOPIC_NAME;
	}

	public int getDEVICE_NUMBER() {
		return DEVICE_NUMBER;
	}

	public void setDEVICE_NUMBER(int DEVICE_NUMBER) {
		this.DEVICE_NUMBER = DEVICE_NUMBER;
	}

	public boolean isIS_CLIENT_BIND() {
		return IS_CLIENT_BIND;
	}

	public void setIS_CLIENT_BIND(boolean IS_CLIENT_BIND) {
		this.IS_CLIENT_BIND = IS_CLIENT_BIND;
	}
	
	public void setIS_SENSOR_TS_ALIGNMENT(boolean IS_SENSOR_TS_ALIGNMENT) {
		this.IS_SENSOR_TS_ALIGNMENT = IS_SENSOR_TS_ALIGNMENT;
	}
	
	public boolean isIS_SENSOR_TS_ALIGNMENT() {
		return IS_SENSOR_TS_ALIGNMENT ;
	}

	public boolean isENABLE_THRIFT_COMPRESSION() {
		return ENABLE_THRIFT_COMPRESSION;
	}

	public void setENABLE_THRIFT_COMPRESSION(boolean ENABLE_THRIFT_COMPRESSION) {
		this.ENABLE_THRIFT_COMPRESSION = ENABLE_THRIFT_COMPRESSION;
	}

	public int getCLIENT_NUMBER() {
		return CLIENT_NUMBER;
	}

	public void setCLIENT_NUMBER(int CLIENT_NUMBER) {
		this.CLIENT_NUMBER = CLIENT_NUMBER;
	}

	public int getSENSOR_NUMBER() {
		return SENSOR_NUMBER;
	}

	public void setSENSOR_NUMBER(int SENSOR_NUMBER) {
		this.SENSOR_NUMBER = SENSOR_NUMBER;
	}

	public long getPOINT_STEP() {
		return POINT_STEP;
	}

	public void setPOINT_STEP(long POINT_STEP) {
		this.POINT_STEP = POINT_STEP;
	}

	public String getTIMESTAMP_PRECISION() {
		return TIMESTAMP_PRECISION;
	}

	public void setTIMESTAMP_PRECISION(String TIMESTAMP_PRECISION) {
		this.TIMESTAMP_PRECISION = TIMESTAMP_PRECISION;
	}

	public int getSTEP_SIZE() {
		return STEP_SIZE;
	}

	public void setSTEP_SIZE(int STEP_SIZE) {
		this.STEP_SIZE = STEP_SIZE;
	}

	public int getOP_INTERVAL() {
		return OP_INTERVAL;
	}

	public void setOP_INTERVAL(int OP_INTERVAL) {
		this.OP_INTERVAL = OP_INTERVAL;
	}

	public String getSG_STRATEGY() {
		return SG_STRATEGY;
	}

	public void setSG_STRATEGY(String SG_STRATEGY) {
		this.SG_STRATEGY = SG_STRATEGY;
	}

	public int getBATCH_SIZE() {
		return BATCH_SIZE;
	}

	public void setBATCH_SIZE(int BATCH_SIZE) {
		this.BATCH_SIZE = BATCH_SIZE;
	}

	public int getGROUP_NUMBER() {
		return GROUP_NUMBER;
	}

	public void setGROUP_NUMBER(int GROUP_NUMBER) {
		this.GROUP_NUMBER = GROUP_NUMBER;
	}

	public String getENCODING() {
		return ENCODING;
	}

	public void setENCODING(String ENCODING) {
		this.ENCODING = ENCODING;
	}

	public int getNUMBER_OF_DECIMAL_DIGIT() {
		return NUMBER_OF_DECIMAL_DIGIT;
	}

	public void setNUMBER_OF_DECIMAL_DIGIT(int NUMBER_OF_DECIMAL_DIGIT) {
		this.NUMBER_OF_DECIMAL_DIGIT = NUMBER_OF_DECIMAL_DIGIT;
	}

	public String getCOMPRESSOR() {
		return COMPRESSOR;
	}

	public void setCOMPRESSOR(String COMPRESSOR) {
		this.COMPRESSOR = COMPRESSOR;
	}

	public boolean isMUL_DEV_BATCH() {
		return MUL_DEV_BATCH;
	}

	public void setMUL_DEV_BATCH(boolean MUL_DEV_BATCH) {
		this.MUL_DEV_BATCH = MUL_DEV_BATCH;
	}

	public long getINIT_WAIT_TIME() {
		return INIT_WAIT_TIME;
	}

	public void setINIT_WAIT_TIME(long INIT_WAIT_TIME) {
		this.INIT_WAIT_TIME = INIT_WAIT_TIME;
	}

	public boolean isIS_OVERFLOW() {
		return IS_OVERFLOW;
	}

	public void setIS_OVERFLOW(boolean IS_OVERFLOW) {
		this.IS_OVERFLOW = IS_OVERFLOW;
	}

	public int getOVERFLOW_MODE() {
		return OVERFLOW_MODE;
	}

	public void setOVERFLOW_MODE(int OVERFLOW_MODE) {
		this.OVERFLOW_MODE = OVERFLOW_MODE;
	}

	public double getOVERFLOW_RATIO() {
		return OVERFLOW_RATIO;
	}

	public void setOVERFLOW_RATIO(double OVERFLOW_RATIO) {
		this.OVERFLOW_RATIO = OVERFLOW_RATIO;
	}

	public double getREAL_INSERT_RATE() {
		return REAL_INSERT_RATE;
	}

	public void setREAL_INSERT_RATE(double REAL_INSERT_RATE) {
		this.REAL_INSERT_RATE = REAL_INSERT_RATE;
	}

	public boolean isUSE_CLUSTER() {
		return USE_CLUSTER;
	}

	public void setUSE_CLUSTER(boolean USE_CLUSTER) {
		this.USE_CLUSTER = USE_CLUSTER;
	}

	public int getFIRST_INDEX() {
		return FIRST_INDEX;
	}

	public void setFIRST_INDEX(int FIRST_INDEX) {
		this.FIRST_INDEX = FIRST_INDEX;
	}

	public boolean isIS_QUIET_MODE() {
		return IS_QUIET_MODE;
	}

	public void setIS_QUIET_MODE(boolean IS_QUIET_MODE) {
		this.IS_QUIET_MODE = IS_QUIET_MODE;
	}

	public int getLOG_PRINT_INTERVAL() {
		return LOG_PRINT_INTERVAL;
	}

	public void setLOG_PRINT_INTERVAL(int LOG_PRINT_INTERVAL) {
		this.LOG_PRINT_INTERVAL = LOG_PRINT_INTERVAL;
	}

	public int getWORKLOAD_BUFFER_SIZE() {
		return WORKLOAD_BUFFER_SIZE;
	}

	public void setWORKLOAD_BUFFER_SIZE(int WORKLOAD_BUFFER_SIZE) {
		this.WORKLOAD_BUFFER_SIZE = WORKLOAD_BUFFER_SIZE;
	}

	public double getLAMBDA() {
		return LAMBDA;
	}

	public void setLAMBDA(double LAMBDA) {
		this.LAMBDA = LAMBDA;
	}

	public int getMAX_K() {
		return MAX_K;
	}

	public void setMAX_K(int MAX_K) {
		this.MAX_K = MAX_K;
	}

	public boolean isIS_RANDOM_TIMESTAMP_INTERVAL() {
		return IS_RANDOM_TIMESTAMP_INTERVAL;
	}

	public void setIS_RANDOM_TIMESTAMP_INTERVAL(boolean IS_RANDOM_TIMESTAMP_INTERVAL) {
		this.IS_RANDOM_TIMESTAMP_INTERVAL = IS_RANDOM_TIMESTAMP_INTERVAL;
	}

	public double getCLIENT_MAX_WRT_RATE() {
		return CLIENT_MAX_WRT_RATE;
	}

	public void setCLIENT_MAX_WRT_RATE(double CLIENT_MAX_WRT_RATE) {
		this.CLIENT_MAX_WRT_RATE = CLIENT_MAX_WRT_RATE;
	}

	public int getLIMIT_CLAUSE_MODE() {
		return LIMIT_CLAUSE_MODE;
	}

	public void setLIMIT_CLAUSE_MODE(int LIMIT_CLAUSE_MODE) {
		this.LIMIT_CLAUSE_MODE = LIMIT_CLAUSE_MODE;
	}

	public String getOPERATION_PROPORTION() {
		return OPERATION_PROPORTION;
	}

	public void setOPERATION_PROPORTION(String OPERATION_PROPORTION) {
		this.OPERATION_PROPORTION = OPERATION_PROPORTION;
	}

	public String getINSERT_DATATYPE_PROPORTION() {
		return INSERT_DATATYPE_PROPORTION;
	}

	public void setINSERT_DATATYPE_PROPORTION(String INSERT_DATATYPE_PROPORTION) {
		this.INSERT_DATATYPE_PROPORTION = INSERT_DATATYPE_PROPORTION;
	}

	public String getENCODING_BOOLEAN() {
		return ENCODING_BOOLEAN;
	}

	public void setENCODING_BOOLEAN(String ENCODING_BOOLEAN) {
		this.ENCODING_BOOLEAN = ENCODING_BOOLEAN;
	}

	public String getENCODING_INT32() {
		return ENCODING_INT32;
	}

	public void setENCODING_INT32(String ENCODING_INT32) {
		this.ENCODING_INT32 = ENCODING_INT32;
	}

	public String getENCODING_INT64() {
		return ENCODING_INT64;
	}

	public void setENCODING_INT64(String ENCODING_INT64) {
		this.ENCODING_INT64 = ENCODING_INT64;
	}

	public String getENCODING_FLOAT() {
		return ENCODING_FLOAT;
	}

	public void setENCODING_FLOAT(String ENCODING_FLOAT) {
		this.ENCODING_FLOAT = ENCODING_FLOAT;
	}

	public String getENCODING_DOUBLE() {
		return ENCODING_DOUBLE;
	}

	public void setENCODING_DOUBLE(String ENCODING_DOUBLE) {
		this.ENCODING_DOUBLE = ENCODING_DOUBLE;
	}

	public String getENCODING_TEXT() {
		return ENCODING_TEXT;
	}

	public void setENCODING_TEXT(String ENCODING_TEXT) {
		this.ENCODING_TEXT = ENCODING_TEXT;
	}

	public String getSTART_TIME() {
		return START_TIME;
	}

	public void setSTART_TIME(String START_TIME) {
		this.START_TIME = START_TIME;
	}

	public int getINTERVAL() {
		return INTERVAL;
	}

	public void setINTERVAL(int INTERVAL) {
		this.INTERVAL = INTERVAL;
	}

	public String getNET_DEVICE() {
		return NET_DEVICE;
	}

	public void setNET_DEVICE(String NET_DEVICE) {
		this.NET_DEVICE = NET_DEVICE;
	}

	public String getSTORAGE_GROUP_NAME() {
		return STORAGE_GROUP_NAME;
	}

	public void setSTORAGE_GROUP_NAME(String STORAGE_GROUP_NAME) {
		this.STORAGE_GROUP_NAME = STORAGE_GROUP_NAME;
	}

	public String getTIMESERIES_NAME() {
		return TIMESERIES_NAME;
	}

	public void setTIMESERIES_NAME(String TIMESERIES_NAME) {
		this.TIMESERIES_NAME = TIMESERIES_NAME;
	}

	public String getTIMESERIES_TYPE() {
		return TIMESERIES_TYPE;
	}

	public void setTIMESERIES_TYPE(String TIMESERIES_TYPE) {
		this.TIMESERIES_TYPE = TIMESERIES_TYPE;
	}

	public String getTIMESERIES_VALUE_SCOPE() {
		return TIMESERIES_VALUE_SCOPE;
	}

	public void setTIMESERIES_VALUE_SCOPE(String TIMESERIES_VALUE_SCOPE) {
		this.TIMESERIES_VALUE_SCOPE = TIMESERIES_VALUE_SCOPE;
	}

	public String getFILE_PATH() {
		return FILE_PATH;
	}

	public void setFILE_PATH(String FILE_PATH) {
		this.FILE_PATH = FILE_PATH;
	}

	public DataSet getDATA_SET() {
		return DATA_SET;
	}

	public void setDATA_SET(DataSet DATA_SET) {
		this.DATA_SET = DATA_SET;
	}

	public List<String> getFIELDS() {
		return FIELDS;
	}

	public void setFIELDS(List<String> FIELDS) {
		this.FIELDS = FIELDS;
	}

	public int[] getPRECISION() {
		return PRECISION;
	}

	public void setPRECISION(int[] PRECISION) {
		this.PRECISION = PRECISION;
	}

	public String getDB_DATA_PATH() {
		return DB_DATA_PATH;
	}

	public void setDB_DATA_PATH(String DB_DATA_PATH) {
		this.DB_DATA_PATH = DB_DATA_PATH;
	}

	public List<String> getIOTDB_DATA_DIR() {
		return IOTDB_DATA_DIR;
	}

	public void setIOTDB_DATA_DIR(List<String> IOTDB_DATA_DIR) {
		this.IOTDB_DATA_DIR = IOTDB_DATA_DIR;
	}

	public List<String> getIOTDB_WAL_DIR() {
		return IOTDB_WAL_DIR;
	}

	public void setIOTDB_WAL_DIR(List<String> IOTDB_WAL_DIR) {
		this.IOTDB_WAL_DIR = IOTDB_WAL_DIR;
	}

	public List<String> getIOTDB_SYSTEM_DIR() {
		return IOTDB_SYSTEM_DIR;
	}

	public void setIOTDB_SYSTEM_DIR(List<String> IOTDB_SYSTEM_DIR) {
		this.IOTDB_SYSTEM_DIR = IOTDB_SYSTEM_DIR;
	}

	public List<String> getSEQUENCE_DIR() {
		return SEQUENCE_DIR;
	}

	public void setSEQUENCE_DIR(List<String> SEQUENCE_DIR) {
		this.SEQUENCE_DIR = SEQUENCE_DIR;
	}

	public List<String> getUNSEQUENCE_DIR() {
		return UNSEQUENCE_DIR;
	}

	public void setUNSEQUENCE_DIR(List<String> UNSEQUENCE_DIR) {
		this.UNSEQUENCE_DIR = UNSEQUENCE_DIR;
	}

	public int getFIRST_DEVICE_INDEX() {
		return FIRST_DEVICE_INDEX;
	}

	public void setFIRST_DEVICE_INDEX(int FIRST_DEVICE_INDEX) {
		this.FIRST_DEVICE_INDEX = FIRST_DEVICE_INDEX;
	}

	public long getLOOP() {
		return LOOP;
	}

	public void setLOOP(long LOOP) {
		this.LOOP = LOOP;
	}

	public double getLINE_RATIO() {
		return LINE_RATIO;
	}

	public void setLINE_RATIO(double LINE_RATIO) {
		this.LINE_RATIO = LINE_RATIO;
	}

	public double getSIN_RATIO() {
		return SIN_RATIO;
	}

	public void setSIN_RATIO(double SIN_RATIO) {
		this.SIN_RATIO = SIN_RATIO;
	}

	public double getSQUARE_RATIO() {
		return SQUARE_RATIO;
	}

	public void setSQUARE_RATIO(double SQUARE_RATIO) {
		this.SQUARE_RATIO = SQUARE_RATIO;
	}

	public double getRANDOM_RATIO() {
		return RANDOM_RATIO;
	}

	public void setRANDOM_RATIO(double RANDOM_RATIO) {
		this.RANDOM_RATIO = RANDOM_RATIO;
	}

	public double getCONSTANT_RATIO() {
		return CONSTANT_RATIO;
	}

	public void setCONSTANT_RATIO(double CONSTANT_RATIO) {
		this.CONSTANT_RATIO = CONSTANT_RATIO;
	}

	public long getDATA_SEED() {
		return DATA_SEED;
	}

	public void setDATA_SEED(long DATA_SEED) {
		this.DATA_SEED = DATA_SEED;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	private String HOST ="127.0.0.1";
	private String PORT ="6667";

	private boolean ENABLE_DOUBLE_INSERT = false;
	private String ANOTHER_HOST ="127.0.0.1";
	private String ANOTHER_PORT ="6668";

	private String KAFKA_LOCATION = "127.0.0.1:9092";
	private String ZOOKEEPER_LOCATION = "127.0.0.1:2181";
	private String TOPIC_NAME = "NULL";

	private int poolSize = 50;

	/** 设备数量 */
	private int DEVICE_NUMBER = 2;

	/** 设备和客户端是否绑定 */
	private boolean IS_CLIENT_BIND = true;
	
	/** 传感器时间戳是否对齐*/
	private boolean IS_SENSOR_TS_ALIGNMENT = true;

	/** if enable the thrift compression */
	private boolean ENABLE_THRIFT_COMPRESSION = false;

	/** 测试客户端线程数量 */
	private int CLIENT_NUMBER = 2;

	/** 每个设备的传感器数量 */
	private int SENSOR_NUMBER = 5;

	/** 数据采集步长 */
	private long POINT_STEP = 7000;

	/** 时间戳精度 */
	private String TIMESTAMP_PRECISION = "ms";

	/** 查询时间戳变化增加步长 */
	private int STEP_SIZE = 1;

  /**
   * 操作执行间隔
   */
	private int OP_INTERVAL = 0;

	/** 存储组分配策略*/
	private String SG_STRATEGY="hash";

	/** 数据发送缓存条数 */
	private int BATCH_SIZE = 1000;

	/** 存储组数量 */
	private int GROUP_NUMBER = 1;

	/** 数据编码方式 */
	private String ENCODING = "PLAIN";

	/** 生成数据的小数保留位数 */
	private int NUMBER_OF_DECIMAL_DIGIT = 2;

	/** 数据压缩方式 */
	private String COMPRESSOR = "UNCOMPRESSED";

	/**是否为多设备批插入模式*/
	private boolean MUL_DEV_BATCH = false;

	/**数据库初始化等待时间ms*/
	private long INIT_WAIT_TIME=5000;

	/**是否为批插入乱序模式*/
	private boolean IS_OVERFLOW = false;

	/**乱序模式*/
	private int OVERFLOW_MODE = 0;

	/**批插入乱序比例*/
	private double OVERFLOW_RATIO = 1.0;

  /**
   * 实际写入设备数占的比例
   */
	private double REAL_INSERT_RATE = 1.0;

	/**使用集群模式**/
	private boolean USE_CLUSTER = false;

	/**集群模式下device的FIRST_INDEX**/
	private int FIRST_INDEX = 0;

	private boolean IS_QUIET_MODE = true;

	private int LOG_PRINT_INTERVAL = 5;

	private int WORKLOAD_BUFFER_SIZE = 100;

	private double LAMBDA = 3;

	private int MAX_K = 10;

	private boolean IS_RANDOM_TIMESTAMP_INTERVAL = false;

	private double CLIENT_MAX_WRT_RATE = 10000000.0;

	private int LIMIT_CLAUSE_MODE = 0;

	private String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0";

	private String INSERT_DATATYPE_PROPORTION = "1:1:1:1:1:1";

	private String ENCODING_BOOLEAN = "PLAIN";

	private String ENCODING_INT32 = "PLAIN";

	private String ENCODING_INT64 = "PLAIN";

	private String ENCODING_FLOAT = "PLAIN";

	private String ENCODING_DOUBLE = "PLAIN";

	private String ENCODING_TEXT = "PLAIN";

	private String START_TIME = "2018-8-30T00:00:00+08:00";

	/**系统性能检测时间间隔-2秒*/
 	private int INTERVAL = 0;

 	/**系统性能检测网卡设备名*/
 	private String NET_DEVICE = "e";

 	/**一个样例数据的存储组名称*/
 	private String STORAGE_GROUP_NAME ;

 	/**一个样例数据的时序名称*/
 	private String TIMESERIES_NAME ;

 	/**一个时序的数据类型*/
 	private String TIMESERIES_TYPE ;

 	/**时序数据取值范围*/
	private String TIMESERIES_VALUE_SCOPE ;

	/** 文件的名字 */
	private String FILE_PATH;

	/** 数据集的名字 */
	private DataSet DATA_SET;

	/** 数据集的传感器 */
	private List<String> FIELDS;

	/** 数据集的传感器的精度 */
	private int[] PRECISION;

	private String DB_DATA_PATH;

	private List<String> IOTDB_DATA_DIR = new ArrayList<>();

	private List<String> IOTDB_WAL_DIR = new ArrayList<>();

	private List<String> IOTDB_SYSTEM_DIR = new ArrayList<>();

	private List<String> SEQUENCE_DIR = new ArrayList<>();

	private List<String> UNSEQUENCE_DIR = new ArrayList<>();

	private int FIRST_DEVICE_INDEX = 0;

	private long LOOP = 10000;

	/** 线性 默认 9个 0.054 */
	private double LINE_RATIO = 0.054;

	/** 傅里叶函数 6个 0.036 */
	private double SIN_RATIO = 0.036;

	/** 方波 9个 0.054 */
	private double SQUARE_RATIO = 0.054;

	/** 随机数 默认 86个 0.512 */
	private double RANDOM_RATIO = 0.512;

	/** 常数 默认 58个 0.352 */
	private double CONSTANT_RATIO = 0.352;

	// ============各函数比例end============
	private long DATA_SEED = 666L;

	/** 内置函数参数 */
	private final List<FunctionParam> LINE_LIST = new ArrayList<>();
	private final List<FunctionParam> SIN_LIST = new ArrayList<>();
	private final List<FunctionParam> SQUARE_LIST = new ArrayList<>();
	private final List<FunctionParam> RANDOM_LIST = new ArrayList<>();
	private final List<FunctionParam> CONSTANT_LIST = new ArrayList<>();

	/** 设备编号 */
	private List<Integer> DEVICE_CODES = new ArrayList<>();

	/** 传感器编号 */
	public List<String> SENSOR_CODES = new ArrayList<>();

	/** 传感器对应的函数 */
	public Map<String, FunctionParam> SENSOR_FUNCTION = new HashMap<>();

	// 负载测试完是否删除数据
	public boolean IS_DELETE_DATA = false;

	public List<Integer> getDEVICE_CODES() {
		return DEVICE_CODES;
	}

	public void setDEVICE_CODES(List<Integer> DEVICE_CODES) {
		this.DEVICE_CODES = DEVICE_CODES;
	}

	public List<String> getSENSOR_CODES() {
		return SENSOR_CODES;
	}

	public void setSENSOR_CODES(List<String> SENSOR_CODES) {
		this.SENSOR_CODES = SENSOR_CODES;
	}

	public Map<String, FunctionParam> getSENSOR_FUNCTION() {
		return SENSOR_FUNCTION;
	}

	public void setSENSOR_FUNCTION(Map<String, FunctionParam> SENSOR_FUNCTION) {
		this.SENSOR_FUNCTION = SENSOR_FUNCTION;
	}

	public boolean isIS_DELETE_DATA() {
		return IS_DELETE_DATA;
	}

	public void setIS_DELETE_DATA(boolean IS_DELETE_DATA) {
		this.IS_DELETE_DATA = IS_DELETE_DATA;
	}

	public int getQUERY_SENSOR_NUM() {
		return QUERY_SENSOR_NUM;
	}

	public void setQUERY_SENSOR_NUM(int QUERY_SENSOR_NUM) {
		this.QUERY_SENSOR_NUM = QUERY_SENSOR_NUM;
	}

	public int getQUERY_DEVICE_NUM() {
		return QUERY_DEVICE_NUM;
	}

	public void setQUERY_DEVICE_NUM(int QUERY_DEVICE_NUM) {
		this.QUERY_DEVICE_NUM = QUERY_DEVICE_NUM;
	}

	public String getQUERY_AGGREGATE_FUN() {
		return QUERY_AGGREGATE_FUN;
	}

	public void setQUERY_AGGREGATE_FUN(String QUERY_AGGREGATE_FUN) {
		this.QUERY_AGGREGATE_FUN = QUERY_AGGREGATE_FUN;
	}

	public long getQUERY_INTERVAL() {
		return QUERY_INTERVAL;
	}

	public void setQUERY_INTERVAL(long QUERY_INTERVAL) {
		this.QUERY_INTERVAL = QUERY_INTERVAL;
	}

	public double getQUERY_LOWER_LIMIT() {
		return QUERY_LOWER_LIMIT;
	}

	public void setQUERY_LOWER_LIMIT(double QUERY_LOWER_LIMIT) {
		this.QUERY_LOWER_LIMIT = QUERY_LOWER_LIMIT;
	}

	public boolean isIS_EMPTY_PRECISE_POINT_QUERY() {
		return IS_EMPTY_PRECISE_POINT_QUERY;
	}

	public void setIS_EMPTY_PRECISE_POINT_QUERY(boolean IS_EMPTY_PRECISE_POINT_QUERY) {
		this.IS_EMPTY_PRECISE_POINT_QUERY = IS_EMPTY_PRECISE_POINT_QUERY;
	}

	public long getTIME_UNIT() {
		return TIME_UNIT;
	}

	public void setTIME_UNIT(long TIME_UNIT) {
		this.TIME_UNIT = TIME_UNIT;
	}

	public long getQUERY_SEED() {
		return QUERY_SEED;
	}

	public void setQUERY_SEED(long QUERY_SEED) {
		this.QUERY_SEED = QUERY_SEED;
	}

	public int getQUERY_LIMIT_N() {
		return QUERY_LIMIT_N;
	}

	public void setQUERY_LIMIT_N(int QUERY_LIMIT_N) {
		this.QUERY_LIMIT_N = QUERY_LIMIT_N;
	}

	public int getQUERY_LIMIT_OFFSET() {
		return QUERY_LIMIT_OFFSET;
	}

	public void setQUERY_LIMIT_OFFSET(int QUERY_LIMIT_OFFSET) {
		this.QUERY_LIMIT_OFFSET = QUERY_LIMIT_OFFSET;
	}

	public int getQUERY_SLIMIT_N() {
		return QUERY_SLIMIT_N;
	}

	public void setQUERY_SLIMIT_N(int QUERY_SLIMIT_N) {
		this.QUERY_SLIMIT_N = QUERY_SLIMIT_N;
	}

	public int getQUERY_SLIMIT_OFFSET() {
		return QUERY_SLIMIT_OFFSET;
	}

	public void setQUERY_SLIMIT_OFFSET(int QUERY_SLIMIT_OFFSET) {
		this.QUERY_SLIMIT_OFFSET = QUERY_SLIMIT_OFFSET;
	}

	public boolean isCREATE_SCHEMA() {
		return CREATE_SCHEMA;
	}

	public void setCREATE_SCHEMA(boolean CREATE_SCHEMA) {
		this.CREATE_SCHEMA = CREATE_SCHEMA;
	}

	public long getREAL_QUERY_START_TIME() {
		return REAL_QUERY_START_TIME;
	}

	public void setREAL_QUERY_START_TIME(long REAL_QUERY_START_TIME) {
		this.REAL_QUERY_START_TIME = REAL_QUERY_START_TIME;
	}

	public long getREAL_QUERY_STOP_TIME() {
		return REAL_QUERY_STOP_TIME;
	}

	public void setREAL_QUERY_STOP_TIME(long REAL_QUERY_STOP_TIME) {
		this.REAL_QUERY_STOP_TIME = REAL_QUERY_STOP_TIME;
	}

	public String getTEST_DATA_PERSISTENCE() {
		return TEST_DATA_PERSISTENCE;
	}

	public void setTEST_DATA_PERSISTENCE(String TEST_DATA_PERSISTENCE) {
		this.TEST_DATA_PERSISTENCE = TEST_DATA_PERSISTENCE;
	}

	public boolean isCSV_OUTPUT() {
		return CSV_OUTPUT;
	}

	public void setCSV_OUTPUT(boolean CSV_OUTPUT) {
		this.CSV_OUTPUT = CSV_OUTPUT;
	}

	public String getREMARK() {
		return REMARK;
	}

	public void setREMARK(String REMARK) {
		this.REMARK = REMARK;
	}

	public String getTEST_DATA_STORE_IP() {
		return TEST_DATA_STORE_IP;
	}

	public void setTEST_DATA_STORE_IP(String TEST_DATA_STORE_IP) {
		this.TEST_DATA_STORE_IP = TEST_DATA_STORE_IP;
	}

	public String getTEST_DATA_STORE_PORT() {
		return TEST_DATA_STORE_PORT;
	}

	public void setTEST_DATA_STORE_PORT(String TEST_DATA_STORE_PORT) {
		this.TEST_DATA_STORE_PORT = TEST_DATA_STORE_PORT;
	}

	public String getTEST_DATA_STORE_DB() {
		return TEST_DATA_STORE_DB;
	}

	public void setTEST_DATA_STORE_DB(String TEST_DATA_STORE_DB) {
		this.TEST_DATA_STORE_DB = TEST_DATA_STORE_DB;
	}

	public String getTEST_DATA_STORE_USER() {
		return TEST_DATA_STORE_USER;
	}

	public void setTEST_DATA_STORE_USER(String TEST_DATA_STORE_USER) {
		this.TEST_DATA_STORE_USER = TEST_DATA_STORE_USER;
	}

	public String getTEST_DATA_STORE_PW() {
		return TEST_DATA_STORE_PW;
	}

	public void setTEST_DATA_STORE_PW(String TEST_DATA_STORE_PW) {
		this.TEST_DATA_STORE_PW = TEST_DATA_STORE_PW;
	}

	public void setMYSQL_REAL_INSERT_RATE(double MYSQL_REAL_INSERT_RATE) {
		this.MYSQL_REAL_INSERT_RATE = MYSQL_REAL_INSERT_RATE;
	}

	public double getMYSQL_REAL_INSERT_RATE() {
		return MYSQL_REAL_INSERT_RATE;
	}

	private double MYSQL_REAL_INSERT_RATE = 1.0;

	//iotDB查询测试相关参数
	private int QUERY_SENSOR_NUM = 1;
	private int QUERY_DEVICE_NUM = 1;
	private String QUERY_AGGREGATE_FUN = "";
	private long QUERY_INTERVAL = DEVICE_NUMBER * POINT_STEP;
	private double QUERY_LOWER_LIMIT = 0;
	private boolean IS_EMPTY_PRECISE_POINT_QUERY = false;
	private long TIME_UNIT = QUERY_INTERVAL / 2;
	private long QUERY_SEED = 1516580959202L;
	private int QUERY_LIMIT_N = 1;
	private int QUERY_LIMIT_OFFSET = 0;
	private int QUERY_SLIMIT_N = 1;
	private int QUERY_SLIMIT_OFFSET = 0;
	private boolean CREATE_SCHEMA = true;
	private long REAL_QUERY_START_TIME = 0;
	private long REAL_QUERY_STOP_TIME = Long.MAX_VALUE;

	//mysql相关参数
	// mysql服务器URL以及用户名密码
	private String TEST_DATA_PERSISTENCE = "None";
	private boolean CSV_OUTPUT = true;

	private String REMARK = "";
	private String TEST_DATA_STORE_IP = "";
	private String TEST_DATA_STORE_PORT = "";
	private String TEST_DATA_STORE_DB = "";
	private String TEST_DATA_STORE_USER = "";
	private String TEST_DATA_STORE_PW = "";
	private String VERSION = "";

	private AtomicLong CURRENT_CSV_LINE = new AtomicLong();

	private long MAX_CSV_LINE = 10000000;

	private boolean CSV_FILE_SPLIT = true;

	public boolean isCSV_FILE_SPLIT() {
		return CSV_FILE_SPLIT;
	}

	public void setCSV_FILE_SPLIT(boolean CSV_FILE_SPLIT) {
		this.CSV_FILE_SPLIT = CSV_FILE_SPLIT;
	}

	public void setMAX_CSV_LINE(long MAX_CSV_LINE) {
		this.MAX_CSV_LINE = MAX_CSV_LINE;
	}

	public long IncrementAndGetCURRENT_CSV_LINE() {
		return CURRENT_CSV_LINE.incrementAndGet();
	}

	public long getCURRENT_CSV_LINE() {
		return CURRENT_CSV_LINE.get();
	}

	public void resetCURRENT_CSV_LINE() {
		CURRENT_CSV_LINE.set(0);
	}

	public void setCURRENT_CSV_LINE(AtomicLong CURRENT_CSV_LINE) {
		this.CURRENT_CSV_LINE = CURRENT_CSV_LINE;
	}


	public long getMAX_CSV_LINE() {
		return MAX_CSV_LINE;
	}

	public String getVERSION() {
		return VERSION;
	}

	public void setVERSION(String VERSION) {
		this.VERSION = VERSION;
	}

	public String getDB_URL() {
		return DB_URL;
	}

	public void setDB_URL(String DB_URL) {
		this.DB_URL = DB_URL;
	}

	public String getDB_NAME() {
		return DB_NAME;
	}

	public void setDB_NAME(String DB_NAME) {
		this.DB_NAME = DB_NAME;
	}

	public String getDB_SWITCH() {
		return DB_SWITCH;
	}

	public void setDB_SWITCH(String DB_SWITCH) {
		this.DB_SWITCH = DB_SWITCH;
	}

	public String getBENCHMARK_WORK_MODE() {
		return BENCHMARK_WORK_MODE;
	}

	public void setBENCHMARK_WORK_MODE(String BENCHMARK_WORK_MODE) {
		this.BENCHMARK_WORK_MODE = BENCHMARK_WORK_MODE;
	}

	public String getINSERT_MODE() {
		return INSERT_MODE;
	}

	public void setINSERT_MODE(String INSERT_MODE) {
		this.INSERT_MODE = INSERT_MODE;
	}

	public String getIMPORT_DATA_FILE_PATH() {
		return IMPORT_DATA_FILE_PATH;
	}

	public void setIMPORT_DATA_FILE_PATH(String IMPORT_DATA_FILE_PATH) {
		this.IMPORT_DATA_FILE_PATH = IMPORT_DATA_FILE_PATH;
	}

	public int getBATCH_EXECUTE_COUNT() {
		return BATCH_EXECUTE_COUNT;
	}

	public void setBATCH_EXECUTE_COUNT(int BATCH_EXECUTE_COUNT) {
		this.BATCH_EXECUTE_COUNT = BATCH_EXECUTE_COUNT;
	}

	public String getMETADATA_FILE_PATH() {
		return METADATA_FILE_PATH;
	}

	public void setMETADATA_FILE_PATH(String METADATA_FILE_PATH) {
		this.METADATA_FILE_PATH = METADATA_FILE_PATH;
	}

	// DB参数
	// 服务器URL
	private String DB_URL = "http://localhost:8086";
	// 使用的数据库名
	private String DB_NAME = "test";

	// 使用的数据库
	private String DB_SWITCH = "IoTDB";

	//benchmark 运行模式
	private String BENCHMARK_WORK_MODE="";
	//插入数据模式:
	//IoTDB: jdbc,session
	private String INSERT_MODE = "jdbc";
	//the file path of import data
	private String IMPORT_DATA_FILE_PATH = "";
	//import csv数据文件时的BATCH
	private int BATCH_EXECUTE_COUNT = 5000;
	//mataData文件路径
	private String METADATA_FILE_PATH = "";

	public void initInnerFunction() {
		FunctionXml xml = null;
		try {
			InputStream input = Function.class.getResourceAsStream("/function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class, FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			xml = (FunctionXml) unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for (FunctionParam param : xmlFuctions) {
			if (param.getFunctionType().contains("_mono_k")) {
				LINE_LIST.add(param);
			} else if (param.getFunctionType().contains("_mono")) {
				// 如果min==max则为常数，系统没有非常数的
				if (param.getMin() == param.getMax()) {
					CONSTANT_LIST.add(param);
				}
			} else if (param.getFunctionType().contains("_sin")) {
				SIN_LIST.add(param);
			} else if (param.getFunctionType().contains("_square")) {
				SQUARE_LIST.add(param);
			} else if (param.getFunctionType().contains("_random")) {
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
			Random r = new Random(DATA_SEED);
			for (int i = 0; i < SENSOR_NUMBER; i++) {
				double property = r.nextDouble();
				FunctionParam param = null;
				Random fr = new Random(DATA_SEED + 1 + i);
				double middle = fr.nextDouble();
				if (property >= 0 && property < constantArea) {// constant
					int index = (int) (middle * CONSTANT_LIST.size());
					param = CONSTANT_LIST.get(index);
				}
				if (property >= constantArea && property < lineArea) {// line
					int index = (int) (middle * LINE_LIST.size());
					param = LINE_LIST.get(index);
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
	 */
	void initSensorCodes() {
		for (int i = 0; i < SENSOR_NUMBER; i++) {
			String sensorCode = "s_" + i;
			SENSOR_CODES.add(sensorCode);
		}
	}

	/**
	 * 根据设备数，初始化设备编号
	 */
	public void initDeviceCodes() {
		for (int i = FIRST_DEVICE_INDEX; i < DEVICE_NUMBER + FIRST_DEVICE_INDEX; i++) {
			DEVICE_CODES.add(i);
		}
	}


  void initRealDataSetSchema() {
    switch (DATA_SET) {
      case TDRIVE:
        FIELDS = Arrays.asList("longitude", "latitude");
        PRECISION = new int[]{5, 5};
        break;
      case REDD:
        FIELDS = Collections.singletonList("v");
        PRECISION = new int[]{2};
        break;
      case GEOLIFE:
        FIELDS = Arrays.asList("Latitude", "Longitude", "Zero", "Altitude");
        PRECISION = new int[]{6, 6, 0, 12};
        break;
      case NOAA:
        FIELDS = Arrays
            .asList("TEMP", "DEWP", "SLP", "STP", "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN",
                "PRCP", "SNDP", "FRSHTT");
        PRECISION = new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 0};
        break;
      default:
        throw new RuntimeException(DATA_SET + " is not support");
    }
  }

}
