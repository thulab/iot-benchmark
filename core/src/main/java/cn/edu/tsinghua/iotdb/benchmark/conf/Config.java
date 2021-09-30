/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionXml;
import cn.edu.tsinghua.iotdb.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.enums.DBSwitch;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Config {
  // 初始化
  // 初始化：清理数据
  /** Whether to clear old data before test */
  private boolean IS_DELETE_DATA = false;
  /**
   * The time The time waiting for the init of database under test (unit: ms) it depends on whether
   * delete of database is asynchronous currently needed by KairosDB, InfluxDb, OpenTSDB,
   * TimescaleDB
   */
  private long INIT_WAIT_TIME = 5000;

  /** System performance detection network card device name eg. eth0 */
  private String NET_DEVICE = "e";

  // 初始化：工作状态
  /** Total number of operations that each client process */
  private long LOOP = 10000;

  /**
   * The running mode of benchmark 1. testWithDefaultPath: Conventional test mode, supporting mixed
   * loads of multiple read and write operations 2. writeWithRealDataSet: Write the real data set
   * mode, you need to configure FILE_PATH and DATA_SET, currently supported 3.
   * queryWithRealDataSet: To query the real data set mode, you need to configure
   * REAL_QUERY_START_TIME, REAL_QUERY_STOP_TIME, DATA_SET and testWithDefaultPath mode to query
   * related parameters currently supported 4. serverMODE: Server resource usage monitoring mode
   * (run in this mode is started by the ser-benchmark.sh script, no need to manually configure this
   * parameter)
   */
  private BenchmarkMode BENCHMARK_WORK_MODE = BenchmarkMode.TEST_WITH_DEFAULT_PATH;

  /** Precision of result, unit: % */
  private double RESULT_PRECISION = 0.1;

  /** Whether use benchmark in cluster * */
  private boolean BENCHMARK_CLUSTER = false;
  /** In cluster mode of benchmark, the index of benchmark which will influence index of devices */
  private int BENCHMARK_INDEX = 0;
  /** Calculated in this way: FIRST_DEVICE_INDEX = BENCHMARK_INDEX * DEVICE_NUMBER */
  private int FIRST_DEVICE_INDEX = 0;
  /** 是否都可见，如果可见就可以向其他node发送 Whether access all nodes, rather than just one coordinator */
  private boolean IS_ALL_NODES_VISIBLE = false;

  // 初始化：被测数据库配置
  private DBConfig dbConfig = new DBConfig();

  // 初始化：被测数据库IoTDB相关参数 监控模式(Server Mode)
  /** The data dir of IoTDB (Split by comma) */
  private List<String> IOTDB_DATA_DIR = new ArrayList<>();
  /** The WAL(Write-ahead-log) dir of IoTDB (Split by comma) */
  private List<String> IOTDB_WAL_DIR = new ArrayList<>();
  /** The system dirs of IoTDB */
  private List<String> IOTDB_SYSTEM_DIR = new ArrayList<>();
  /** The sequence dirs of IoTDB */
  private List<String> SEQUENCE_DIR = new ArrayList<>();
  /** The unsequence dirs of IoTDB */
  private List<String> UNSEQUENCE_DIR = new ArrayList<>();

  // 初始化：双写模式
  /** whether to operate another database */
  private boolean IS_DOUBLE_WRITE = false;
  /** Another configuration of db */
  private DBConfig ANOTHER_DBConfig = new DBConfig();
  /** Whether run verification when double write */
  private boolean IS_COMPARISON = false;
  /** Whether to do point compare */
  private boolean IS_POINT_COMPARISON = false;

  // 初始化：Kafka
  /** Location of Kafka */
  private String KAFKA_LOCATION = "127.0.0.1:9092";
  /** Location of Zookeeper */
  private String ZOOKEEPER_LOCATION = "127.0.0.1:2181";
  /** The name of topic in Kafka */
  private String TOPIC_NAME = "NULL";

  // 时间戳
  /** The interval of timestamp(not real rate) */
  private long POINT_STEP = 7000L;
  /** The precision of timestamp, currently support ms and us */
  private String TIMESTAMP_PRECISION = "ms";

  // 数据

  // 数据：格式与编码
  /** The length of string */
  private int STRING_LENGTH = 2;
  /**
   * 插入数据的比例 Data Type, D1:D2:D3:D4:D5:D6 D1: BOOLEAN D2: INT32 D3: INT64 D4: FLOAT D5: DOUBLE D6:
   * TEXT
   */
  private String INSERT_DATATYPE_PROPORTION = "1:1:1:1:1:1";

  // 测试数据相关参数

  // 测试数据：外部测试数据
  /** The path of file */
  private String FILE_PATH;
  /** The size of Big Batch */
  private int BIG_BATCH_SIZE = 10;

  // 设备、传感器、客户端相关参数
  /** The number of devices of database */
  private int DEVICE_NUMBER = 2;
  /** The ratio of actual write devices. (0,1] */
  private double REAL_INSERT_RATE = 1.0;
  /**
   * The number of sensors of each device The number of timeseries = DEVICE_NUMBER * SENSOR_NUMBER
   */
  private int SENSOR_NUMBER = 5;

  /** Whether the sensor timestamp is aligned */
  private boolean IS_SENSOR_TS_ALIGNMENT = true;
  /**
   * whether the device is bind to client if true: number of clients <= devices if false: number of
   * clients can larger than devices
   */
  private boolean IS_CLIENT_BIND = true;
  /**
   * The number of client if IS_CLIENT_BIND = true: this number must be less than or equal to the
   * number of devices.
   */
  private int CLIENT_NUMBER = 2;

  // 设备、传感器、客户端：生成数据的规律
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
  /** Seed of data */
  private long DATA_SEED = 666L;

  // 被测系统IoTDB的参数
  /** if enable the thrift compression */
  private boolean ENABLE_THRIFT_COMPRESSION = false;
  /** Storage Group Allocation Strategy, currently supported hash/mode/div */
  private String SG_STRATEGY = "hash";
  /** The number of storage group, must less than or equal to number of devices */
  private int GROUP_NUMBER = 1;
  /** The size of IoTDB core session pool */
  private int IOTDB_SESSION_POOL_SIZE = 50;

  // 被测系统是MS SQL Server时的参数
  private String COMPRESSION = "NONE";

  // Operation 相关参数
  /**
   * The operation execution interval if operation time > OP_INTERVAL, then execute next operations
   * right now. else wait (OP_INTERVAL - operation time) unit: ms
   */
  private long OP_INTERVAL = 0;
  /** The max time for writing in ms */
  private int WRITE_OPERATION_TIMEOUT_MS = 120000;
  /** The max time for reading in ms */
  private int READ_OPERATION_TIMEOUT_MS = 300000;

  // Operation：写入相关参数
  /**
   * The number of data rows written in batch each row is the data of all sensors of a certain
   * device at a certain time stamp the number of data points written in each batch = SENSOR_NUMBER
   * * BATCH_SIZE
   */
  private int BATCH_SIZE_PER_WRITE = 1;
  /** Whether create schema before writing */
  private boolean CREATE_SCHEMA = true;

  /** Start time of writing data */
  private String START_TIME = "2018-8-30T00:00:00+08:00";

  // Operation：乱序写入部分
  /** Whether insert out of order */
  private boolean IS_OUT_OF_ORDER = false;
  /**
   * The mode of out-of-order insertion 0: Out-of-order mode of Poisson distribution 1: Out-of-order
   * mode of batch
   */
  private int OUT_OF_ORDER_MODE = 0;
  /** The out of order ratio of batch inserting */
  private double OUT_OF_ORDER_RATIO = 1.0;
  /** Whether use random time interval in inorder data need IS_OUT_OF_ORDER = false */
  private boolean IS_REGULAR_FREQUENCY = false;

  /** The expectation and variance of Poisson Distribution based on basic model */
  private double LAMBDA = 3;
  /** The max K of Poisson random variable based on basic model */
  private int MAX_K = 10;

  // Operation：查询相关参数
  /** The change step size of the time starting point of the time filter condition */
  private int STEP_SIZE = 1;
  /**
   * The ratio of each operation, INGESTION:Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10 INGESTION Q1: Precise
   * point query, Eg. select v1... from data where time = ? and device in ? Q2: Time range query,
   * Eg. select v1... from data where time > ? and time < ? and device in ? Q3: Time Range query
   * with value filtering, Eg. select v1... from data where time > ? and time < ? and v1 > ? and
   * device in ? Q4: Aggregate query with time filter, Eg. select func(v1)... from data where device
   * in ? and time > ? and time < ? Q5: Aggregate query with value filtering, Eg. select func(v1)...
   * from data where device in ? and value > ? Q6: Aggregate query with value filtering and time
   * filtering, Eg. select func(v1)... from data where device in ? and value > ? and time > ? and
   * time < ? Q7: Grouped aggregate query, For the time being, only sentences with one time interval
   * can be generated Q8: Last point query, Eg. select time, v1... where device = ? and time =
   * max(time) Q9: Reverse order range query (only limited start and end time), Eg. select v1...
   * from data where time > ? and time < ? and device in ? order by time desc Q10: Range query with
   * value filtering in reverse order, Eg. select v1... from data where time > ? and time < ? and v1
   * > ? and device in ? order by time desc
   */
  private String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0:0:0";
  /** The number of sensors involved in each query */
  private int QUERY_SENSOR_NUM = 1;
  /** The number of devices involved in each query */
  private int QUERY_DEVICE_NUM = 1;
  /** Set aggregate function when aggregate query, Eg. count */
  private String QUERY_AGGREGATE_FUN = "";
  /**
   * The time interval between the start time and the end time in the query with start and end time
   * the time interval in groupBy (the unit is determined by the accuracy)
   */
  private long QUERY_INTERVAL = 10000;
  /** Conditional query parameters "where xxx > QUERY_LOWER_VALUE" */
  private double QUERY_LOWER_VALUE = 0;
  /** The size of group in group by query(ms), Eg. 20000 */
  private long GROUP_BY_TIME_UNIT = QUERY_INTERVAL / 2;
  /** Query random seed */
  private long QUERY_SEED = 1516580959202L;
  // TODO check properties
  /** Maximum number of output items in conditional query with limit TODO check */
  private int QUERY_LIMIT_N = 1;
  /** The offset in conditional query with limit */
  private int QUERY_LIMIT_OFFSET = 0;
  /** Maximum number of output sequences */
  private int QUERY_SLIMIT_N = 1;
  /** Offset of output sequences */
  private int QUERY_SLIMIT_OFFSET = 0;

  // workload 相关部分
  /** The size of workload buffer size */
  private int WORKLOAD_BUFFER_SIZE = 100;

  // 输出
  /** Use what to store test data, currently support None, IoTDB, MySQL, CSV */
  private String TEST_DATA_PERSISTENCE = "None";

  // 输出：系统性能 Server mode
  /** System performance information recording interval is INTERVAL+2 seconds */
  private int MONITOR_INTERVAL = 0;

  // 输出：日志
  /** Whether use quiet mode. Quiet mode will mute some log output and computations */
  private boolean IS_QUIET_MODE = true;
  /** Print test progress log interval in second */
  private int LOG_PRINT_INTERVAL = 5;

  // 输出：数据库配置，当前支持IoTDB和MySQL
  /** The Ip of database */
  private String TEST_DATA_STORE_IP = "";
  /** The Port of database */
  private String TEST_DATA_STORE_PORT = "";
  /** Which database to use */
  private String TEST_DATA_STORE_DB = "";
  /** Which user to authenticate */
  private String TEST_DATA_STORE_USER = "";
  /** The password of user */
  private String TEST_DATA_STORE_PW = "";
  /** The write time out of database */
  private long TEST_DATA_WRITE_TIME_OUT = 300000;
  /** The max connection of database */
  private int TEST_DATA_MAX_CONNECTION = 1;
  /**
   * The remark of experiment which will be stored into mysql as part of table name (Notice that no
   * .) rename to TEST_DATA_STORE_REMARK
   */
  private String REMARK = "";

  // 输出：MySQL
  /** ratio of real writes into mysql */
  private double MYSQL_REAL_INSERT_RATE = 1.0;

  // 输出：CSV
  /** Whether output the result to an csv file located in data folder */
  private boolean CSV_OUTPUT = true;
  /** Current csv file write line */
  private AtomicLong CURRENT_CSV_LINE = new AtomicLong();
  /** Max line of csv line */
  private long CSV_MAX_LINE = 10000000;
  /** Whether split result into different csv file */
  private boolean CSV_FILE_SPLIT = true;

  /** Sensor number */
  private List<String> SENSOR_CODES = new ArrayList<>();
  /** Built-in function parameters */
  private final List<FunctionParam> LINE_LIST = new ArrayList<>();

  private final List<FunctionParam> SIN_LIST = new ArrayList<>();
  private final List<FunctionParam> SQUARE_LIST = new ArrayList<>();
  private final List<FunctionParam> RANDOM_LIST = new ArrayList<>();
  private final List<FunctionParam> CONSTANT_LIST = new ArrayList<>();
  /** Sensor function */
  private Map<String, FunctionParam> SENSOR_FUNCTION = new HashMap<>();

  /** init inner functions */
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
        // if min equals to max, then it is constant.
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

  /** init sensor functions -> Constants.SENSOR_FUNCTION */
  public void initSensorFunction() {
    // Configure according to the ratio of each function passed in
    double sumRatio = CONSTANT_RATIO + LINE_RATIO + RANDOM_RATIO + SIN_RATIO + SQUARE_RATIO;
    if (sumRatio != 0
        && CONSTANT_RATIO >= 0
        && LINE_RATIO >= 0
        && RANDOM_RATIO >= 0
        && SIN_RATIO >= 0
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
        // constant
        if (property >= 0 && property < constantArea) {
          int index = (int) (middle * CONSTANT_LIST.size());
          param = CONSTANT_LIST.get(index);
        }
        // line
        if (property >= constantArea && property < lineArea) {
          int index = (int) (middle * LINE_LIST.size());
          param = LINE_LIST.get(index);
        }
        // random
        if (property >= lineArea && property < randomArea) {
          int index = (int) (middle * RANDOM_LIST.size());
          param = RANDOM_LIST.get(index);
        }
        // sin
        if (property >= randomArea && property < sinArea) {
          int index = (int) (middle * SIN_LIST.size());
          param = SIN_LIST.get(index);
        }
        // square
        if (property >= sinArea && property < squareArea) {
          int index = (int) (middle * SQUARE_LIST.size());
          param = SQUARE_LIST.get(index);
        }
        if (param == null) {
          System.err.println(
              "There is a problem with the initialization function scale "
                  + "in initSensorFunction()!");
          System.exit(0);
        }
        SENSOR_FUNCTION.put(SENSOR_CODES.get(i), param);
      }
    } else {
      System.err.println("function ration must >=0 and sum>0");
      System.exit(0);
    }
  }

  // TODO remove following

  /** According to the number of sensors, initialize the sensor number */
  void initSensorCodes() {
    for (int i = 0; i < SENSOR_NUMBER; i++) {
      String sensorCode = "s_" + i;
      SENSOR_CODES.add(sensorCode);
    }
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

  /** Getter and Setter */
  public boolean isIS_DELETE_DATA() {
    return IS_DELETE_DATA;
  }

  public void setIS_DELETE_DATA(boolean IS_DELETE_DATA) {
    this.IS_DELETE_DATA = IS_DELETE_DATA;
  }

  public long getINIT_WAIT_TIME() {
    return INIT_WAIT_TIME;
  }

  public void setINIT_WAIT_TIME(long INIT_WAIT_TIME) {
    this.INIT_WAIT_TIME = INIT_WAIT_TIME;
  }

  public String getNET_DEVICE() {
    return NET_DEVICE;
  }

  public void setNET_DEVICE(String NET_DEVICE) {
    this.NET_DEVICE = NET_DEVICE;
  }

  public long getLOOP() {
    return LOOP;
  }

  public void setLOOP(long LOOP) {
    this.LOOP = LOOP;
  }

  public BenchmarkMode getBENCHMARK_WORK_MODE() {
    return BENCHMARK_WORK_MODE;
  }

  public void setBENCHMARK_WORK_MODE(BenchmarkMode BENCHMARK_WORK_MODE) {
    this.BENCHMARK_WORK_MODE = BENCHMARK_WORK_MODE;
  }

  public double getRESULT_PRECISION() {
    return RESULT_PRECISION;
  }

  public void setRESULT_PRECISION(double RESULT_PRECISION) {
    this.RESULT_PRECISION = RESULT_PRECISION;
  }

  public boolean isBENCHMARK_CLUSTER() {
    return BENCHMARK_CLUSTER;
  }

  public void setBENCHMARK_CLUSTER(boolean BENCHMARK_CLUSTER) {
    this.BENCHMARK_CLUSTER = BENCHMARK_CLUSTER;
  }

  public int getBENCHMARK_INDEX() {
    return BENCHMARK_INDEX;
  }

  public void setBENCHMARK_INDEX(int BENCHMARK_INDEX) {
    this.BENCHMARK_INDEX = BENCHMARK_INDEX;
  }

  public int getFIRST_DEVICE_INDEX() {
    return FIRST_DEVICE_INDEX;
  }

  public void setFIRST_DEVICE_INDEX(int FIRST_DEVICE_INDEX) {
    this.FIRST_DEVICE_INDEX = FIRST_DEVICE_INDEX;
  }

  public boolean isIS_ALL_NODES_VISIBLE() {
    return IS_ALL_NODES_VISIBLE;
  }

  public void setIS_ALL_NODES_VISIBLE(boolean IS_ALL_NODES_VISIBLE) {
    this.IS_ALL_NODES_VISIBLE = IS_ALL_NODES_VISIBLE;
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

  public int getSTRING_LENGTH() {
    return STRING_LENGTH;
  }

  public void setSTRING_LENGTH(int STRING_LENGTH) {
    this.STRING_LENGTH = STRING_LENGTH;
  }

  public String getINSERT_DATATYPE_PROPORTION() {
    return INSERT_DATATYPE_PROPORTION;
  }

  public void setINSERT_DATATYPE_PROPORTION(String INSERT_DATATYPE_PROPORTION) {
    this.INSERT_DATATYPE_PROPORTION = INSERT_DATATYPE_PROPORTION;
  }

  public String getFILE_PATH() {
    return FILE_PATH;
  }

  public void setFILE_PATH(String FILE_PATH) {
    this.FILE_PATH = FILE_PATH;
  }

  public int getDEVICE_NUMBER() {
    return DEVICE_NUMBER;
  }

  public void setDEVICE_NUMBER(int DEVICE_NUMBER) {
    this.DEVICE_NUMBER = DEVICE_NUMBER;
  }

  public double getREAL_INSERT_RATE() {
    return REAL_INSERT_RATE;
  }

  public void setREAL_INSERT_RATE(double REAL_INSERT_RATE) {
    this.REAL_INSERT_RATE = REAL_INSERT_RATE;
  }

  public int getSENSOR_NUMBER() {
    return SENSOR_NUMBER;
  }

  public void setSENSOR_NUMBER(int SENSOR_NUMBER) {
    this.SENSOR_NUMBER = SENSOR_NUMBER;
  }

  public boolean isIS_SENSOR_TS_ALIGNMENT() {
    return IS_SENSOR_TS_ALIGNMENT;
  }

  public void setIS_SENSOR_TS_ALIGNMENT(boolean IS_SENSOR_TS_ALIGNMENT) {
    this.IS_SENSOR_TS_ALIGNMENT = IS_SENSOR_TS_ALIGNMENT;
  }

  public boolean isIS_CLIENT_BIND() {
    return IS_CLIENT_BIND;
  }

  public void setIS_CLIENT_BIND(boolean IS_CLIENT_BIND) {
    this.IS_CLIENT_BIND = IS_CLIENT_BIND;
  }

  public int getCLIENT_NUMBER() {
    return CLIENT_NUMBER;
  }

  public void setCLIENT_NUMBER(int CLIENT_NUMBER) {
    this.CLIENT_NUMBER = CLIENT_NUMBER;
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

  public boolean isENABLE_THRIFT_COMPRESSION() {
    return ENABLE_THRIFT_COMPRESSION;
  }

  public void setENABLE_THRIFT_COMPRESSION(boolean ENABLE_THRIFT_COMPRESSION) {
    this.ENABLE_THRIFT_COMPRESSION = ENABLE_THRIFT_COMPRESSION;
  }

  public String getSG_STRATEGY() {
    return SG_STRATEGY;
  }

  public void setSG_STRATEGY(String SG_STRATEGY) {
    this.SG_STRATEGY = SG_STRATEGY;
  }

  public int getGROUP_NUMBER() {
    return GROUP_NUMBER;
  }

  public void setGROUP_NUMBER(int GROUP_NUMBER) {
    this.GROUP_NUMBER = GROUP_NUMBER;
  }

  public int getIOTDB_SESSION_POOL_SIZE() {
    return IOTDB_SESSION_POOL_SIZE;
  }

  public void setIOTDB_SESSION_POOL_SIZE(int IOTDB_SESSION_POOL_SIZE) {
    this.IOTDB_SESSION_POOL_SIZE = IOTDB_SESSION_POOL_SIZE;
  }

  public long getOP_INTERVAL() {
    return OP_INTERVAL;
  }

  public void setOP_INTERVAL(long OP_INTERVAL) {
    this.OP_INTERVAL = OP_INTERVAL;
  }

  public int getWRITE_OPERATION_TIMEOUT_MS() {
    return WRITE_OPERATION_TIMEOUT_MS;
  }

  public void setWRITE_OPERATION_TIMEOUT_MS(int WRITE_OPERATION_TIMEOUT_MS) {
    this.WRITE_OPERATION_TIMEOUT_MS = WRITE_OPERATION_TIMEOUT_MS;
  }

  public int getREAD_OPERATION_TIMEOUT_MS() {
    return READ_OPERATION_TIMEOUT_MS;
  }

  public void setREAD_OPERATION_TIMEOUT_MS(int READ_OPERATION_TIMEOUT_MS) {
    this.READ_OPERATION_TIMEOUT_MS = READ_OPERATION_TIMEOUT_MS;
  }

  public int getBATCH_SIZE_PER_WRITE() {
    return BATCH_SIZE_PER_WRITE;
  }

  public void setBATCH_SIZE_PER_WRITE(int BATCH_SIZE_PER_WRITE) {
    this.BATCH_SIZE_PER_WRITE = BATCH_SIZE_PER_WRITE;
  }

  public boolean isCREATE_SCHEMA() {
    return CREATE_SCHEMA;
  }

  public void setCREATE_SCHEMA(boolean CREATE_SCHEMA) {
    this.CREATE_SCHEMA = CREATE_SCHEMA;
  }

  public String getSTART_TIME() {
    return START_TIME;
  }

  public void setSTART_TIME(String START_TIME) {
    this.START_TIME = START_TIME;
  }

  public boolean isIS_OUT_OF_ORDER() {
    return IS_OUT_OF_ORDER;
  }

  public void setIS_OUT_OF_ORDER(boolean IS_OUT_OF_ORDER) {
    this.IS_OUT_OF_ORDER = IS_OUT_OF_ORDER;
  }

  public int getOUT_OF_ORDER_MODE() {
    return OUT_OF_ORDER_MODE;
  }

  public void setOUT_OF_ORDER_MODE(int OUT_OF_ORDER_MODE) {
    this.OUT_OF_ORDER_MODE = OUT_OF_ORDER_MODE;
  }

  public double getOUT_OF_ORDER_RATIO() {
    return OUT_OF_ORDER_RATIO;
  }

  public void setOUT_OF_ORDER_RATIO(double OUT_OF_ORDER_RATIO) {
    this.OUT_OF_ORDER_RATIO = OUT_OF_ORDER_RATIO;
  }

  public boolean isIS_REGULAR_FREQUENCY() {
    return IS_REGULAR_FREQUENCY;
  }

  public void setIS_REGULAR_FREQUENCY(boolean IS_REGULAR_FREQUENCY) {
    this.IS_REGULAR_FREQUENCY = IS_REGULAR_FREQUENCY;
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

  public int getSTEP_SIZE() {
    return STEP_SIZE;
  }

  public void setSTEP_SIZE(int STEP_SIZE) {
    this.STEP_SIZE = STEP_SIZE;
  }

  public String getOPERATION_PROPORTION() {
    return OPERATION_PROPORTION;
  }

  public void setOPERATION_PROPORTION(String OPERATION_PROPORTION) {
    this.OPERATION_PROPORTION = OPERATION_PROPORTION;
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

  public double getQUERY_LOWER_VALUE() {
    return QUERY_LOWER_VALUE;
  }

  public void setQUERY_LOWER_VALUE(double QUERY_LOWER_VALUE) {
    this.QUERY_LOWER_VALUE = QUERY_LOWER_VALUE;
  }

  public long getGROUP_BY_TIME_UNIT() {
    return GROUP_BY_TIME_UNIT;
  }

  public void setGROUP_BY_TIME_UNIT(long GROUP_BY_TIME_UNIT) {
    this.GROUP_BY_TIME_UNIT = GROUP_BY_TIME_UNIT;
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

  public int getWORKLOAD_BUFFER_SIZE() {
    return WORKLOAD_BUFFER_SIZE;
  }

  public void setWORKLOAD_BUFFER_SIZE(int WORKLOAD_BUFFER_SIZE) {
    this.WORKLOAD_BUFFER_SIZE = WORKLOAD_BUFFER_SIZE;
  }

  public String getTEST_DATA_PERSISTENCE() {
    return TEST_DATA_PERSISTENCE;
  }

  public void setTEST_DATA_PERSISTENCE(String TEST_DATA_PERSISTENCE) {
    this.TEST_DATA_PERSISTENCE = TEST_DATA_PERSISTENCE;
  }

  public int getMONITOR_INTERVAL() {
    return MONITOR_INTERVAL;
  }

  public void setMONITOR_INTERVAL(int MONITOR_INTERVAL) {
    this.MONITOR_INTERVAL = MONITOR_INTERVAL;
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

  public String getREMARK() {
    return REMARK;
  }

  public void setREMARK(String REMARK) {
    this.REMARK = REMARK;
  }

  public double getMYSQL_REAL_INSERT_RATE() {
    return MYSQL_REAL_INSERT_RATE;
  }

  public void setMYSQL_REAL_INSERT_RATE(double MYSQL_REAL_INSERT_RATE) {
    this.MYSQL_REAL_INSERT_RATE = MYSQL_REAL_INSERT_RATE;
  }

  public boolean isCSV_OUTPUT() {
    return CSV_OUTPUT;
  }

  public void setCSV_OUTPUT(boolean CSV_OUTPUT) {
    this.CSV_OUTPUT = CSV_OUTPUT;
  }

  public long getCSV_MAX_LINE() {
    return CSV_MAX_LINE;
  }

  public void setCSV_MAX_LINE(long CSV_MAX_LINE) {
    this.CSV_MAX_LINE = CSV_MAX_LINE;
  }

  public boolean isCSV_FILE_SPLIT() {
    return CSV_FILE_SPLIT;
  }

  public void setCSV_FILE_SPLIT(boolean CSV_FILE_SPLIT) {
    this.CSV_FILE_SPLIT = CSV_FILE_SPLIT;
  }

  public List<String> getSENSOR_CODES() {
    return SENSOR_CODES;
  }

  public void setSENSOR_CODES(List<String> SENSOR_CODES) {
    this.SENSOR_CODES = SENSOR_CODES;
  }

  public List<FunctionParam> getLINE_LIST() {
    return LINE_LIST;
  }

  public List<FunctionParam> getSIN_LIST() {
    return SIN_LIST;
  }

  public List<FunctionParam> getSQUARE_LIST() {
    return SQUARE_LIST;
  }

  public List<FunctionParam> getRANDOM_LIST() {
    return RANDOM_LIST;
  }

  public List<FunctionParam> getCONSTANT_LIST() {
    return CONSTANT_LIST;
  }

  public Map<String, FunctionParam> getSENSOR_FUNCTION() {
    return SENSOR_FUNCTION;
  }

  public void setSENSOR_FUNCTION(Map<String, FunctionParam> SENSOR_FUNCTION) {
    this.SENSOR_FUNCTION = SENSOR_FUNCTION;
  }

  public long getTEST_DATA_WRITE_TIME_OUT() {
    return TEST_DATA_WRITE_TIME_OUT;
  }

  public void setTEST_DATA_WRITE_TIME_OUT(long TEST_DATA_WRITE_TIME_OUT) {
    this.TEST_DATA_WRITE_TIME_OUT = TEST_DATA_WRITE_TIME_OUT;
  }

  public int getTEST_DATA_MAX_CONNECTION() {
    return TEST_DATA_MAX_CONNECTION;
  }

  public void setTEST_DATA_MAX_CONNECTION(int TEST_DATA_MAX_CONNECTION) {
    this.TEST_DATA_MAX_CONNECTION = TEST_DATA_MAX_CONNECTION;
  }

  public String getCOMPRESSION() {
    return COMPRESSION;
  }

  public void setCOMPRESSION(String COMPRESSION) {
    this.COMPRESSION = COMPRESSION;
  }

  public void setIS_DOUBLE_WRITE(boolean IS_DOUBLE_WRITE) {
    this.IS_DOUBLE_WRITE = IS_DOUBLE_WRITE;
  }

  public DBConfig getDbConfig() {
    return dbConfig;
  }

  public void setDbConfig(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public DBConfig getANOTHER_DBConfig() {
    return ANOTHER_DBConfig;
  }

  public void setANOTHER_DBConfig(DBConfig ANOTHER_DBConfig) {
    this.ANOTHER_DBConfig = ANOTHER_DBConfig;
  }

  /** Wrapper method */
  public void setDB_SWITCH(DBSwitch DB_SWITCH) {
    this.dbConfig.setDB_SWITCH(DB_SWITCH);
  }

  public void setHOST(List<String> HOST) {
    this.dbConfig.setHOST(HOST);
  }

  public void setPORT(List<String> PORT) {
    this.dbConfig.setPORT(PORT);
  }

  public void setDB_NAME(String DB_NAME) {
    this.dbConfig.setDB_NAME(DB_NAME);
  }

  public void setUSERNAME(String USERNAME) {
    this.dbConfig.setUSERNAME(USERNAME);
  }

  public void setPASSWORD(String PASSWORD) {
    this.dbConfig.setPASSWORD(PASSWORD);
  }

  public void setTOKEN(String TOKEN) {
    this.dbConfig.setTOKEN(TOKEN);
  }

  public void setANOTHER_DB_SWITCH(DBSwitch ANOTHER_DBConfig_SWITCH) {
    this.ANOTHER_DBConfig.setDB_SWITCH(ANOTHER_DBConfig_SWITCH);
  }

  public void setANOTHER_HOST(List<String> ANOTHER_HOST) {
    this.ANOTHER_DBConfig.setHOST(ANOTHER_HOST);
  }

  public void setANOTHER_PORT(List<String> ANOTHER_PORT) {
    this.ANOTHER_DBConfig.setPORT(ANOTHER_PORT);
  }

  public void setANOTHER_USERNAME(String ANOTHER_USERNAME) {
    this.ANOTHER_DBConfig.setUSERNAME(ANOTHER_USERNAME);
  }

  public void setANOTHER_PASSWORD(String ANOTHER_PASSWORD) {
    this.ANOTHER_DBConfig.setPASSWORD(ANOTHER_PASSWORD);
  }

  public void setANOTHER_DB_NAME(String ANOTHER_DBConfig_NAME) {
    this.ANOTHER_DBConfig.setDB_NAME(ANOTHER_DBConfig_NAME);
  }

  public void setANOTHER_TOKEN(String ANOTHER_TOKEN) {
    this.ANOTHER_DBConfig.setTOKEN(ANOTHER_TOKEN);
  }

  public boolean isIS_DOUBLE_WRITE() {
    return IS_DOUBLE_WRITE;
  }

  public boolean isIS_COMPARISON() {
    return IS_COMPARISON;
  }

  public void setIS_COMPARISON(boolean IS_COMPARISON) {
    this.IS_COMPARISON = IS_COMPARISON;
  }

  public int getBIG_BATCH_SIZE() {
    return BIG_BATCH_SIZE;
  }

  public void setBIG_BATCH_SIZE(int BIG_BATCH_SIZE) {
    this.BIG_BATCH_SIZE = BIG_BATCH_SIZE;
  }

  public boolean isIS_POINT_COMPARISON() {
    return IS_POINT_COMPARISON;
  }

  public void setIS_POINT_COMPARISON(boolean IS_POINT_COMPARISON) {
    this.IS_POINT_COMPARISON = IS_POINT_COMPARISON;
  }

  /**
   * write dataset config to info
   *
   * @return
   */
  public String toInfoText() {
    return "LOOP"
        + LOOP
        + "\nBIG_BATCH_SIZE"
        + BIG_BATCH_SIZE
        + "\nFIRST_DEVICE_INDEX"
        + FIRST_DEVICE_INDEX
        + "\nPOINT_STEP"
        + POINT_STEP
        + "\nTIMESTAMP_PRECISION='"
        + TIMESTAMP_PRECISION
        + '\''
        + "\nSTRING_LENGTH"
        + STRING_LENGTH
        + "\nINSERT_DATATYPE_PROPORTION='"
        + INSERT_DATATYPE_PROPORTION
        + '\''
        + "\nDEVICE_NUMBER"
        + DEVICE_NUMBER
        + "\nREAL_INSERT_RATE"
        + REAL_INSERT_RATE
        + "\nSENSOR_NUMBER"
        + SENSOR_NUMBER
        + "\nIS_SENSOR_TS_ALIGNMENT"
        + IS_SENSOR_TS_ALIGNMENT
        + "\nDATA_SEED"
        + DATA_SEED
        + "\nSG_STRATEGY='"
        + SG_STRATEGY
        + '\''
        + "\nGROUP_NUMBER"
        + GROUP_NUMBER
        + "\nBATCH_SIZE_PER_WRITE"
        + BATCH_SIZE_PER_WRITE
        + "\nSTART_TIME='"
        + START_TIME
        + '\''
        + "\nIS_OUT_OF_ORDER"
        + IS_OUT_OF_ORDER
        + "\nOUT_OF_ORDER_MODE"
        + OUT_OF_ORDER_MODE
        + "\nOUT_OF_ORDER_RATIO"
        + OUT_OF_ORDER_RATIO
        + "\nIS_REGULAR_FREQUENCY"
        + IS_REGULAR_FREQUENCY
        + "\nLAMBDA"
        + LAMBDA
        + "\nMAX_K"
        + MAX_K
        + "\nSTEP_SIZE"
        + STEP_SIZE
        + "\nQUERY_SENSOR_NUM"
        + QUERY_SENSOR_NUM
        + "\nQUERY_DEVICE_NUM"
        + QUERY_DEVICE_NUM
        + "\nQUERY_AGGREGATE_FUN='"
        + QUERY_AGGREGATE_FUN
        + '\''
        + "\nQUERY_INTERVAL"
        + QUERY_INTERVAL
        + "\nQUERY_LOWER_VALUE"
        + QUERY_LOWER_VALUE
        + "\nGROUP_BY_TIME_UNIT"
        + GROUP_BY_TIME_UNIT
        + "\nQUERY_SEED"
        + QUERY_SEED
        + "\nQUERY_LIMIT_N"
        + QUERY_LIMIT_N
        + "\nQUERY_LIMIT_OFFSET"
        + QUERY_LIMIT_OFFSET
        + "\nQUERY_SLIMIT_N"
        + QUERY_SLIMIT_N
        + "\nQUERY_SLIMIT_OFFSET"
        + QUERY_SLIMIT_OFFSET
        + "\nWORKLOAD_BUFFER_SIZE"
        + WORKLOAD_BUFFER_SIZE
        + "\nSENSOR_CODES"
        + SENSOR_CODES;
  }

  /**
   * get properties from config, one property in one line.
   *
   * @return
   */
  public Map<String, Object> getShowProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("BENCHMARK_WORK_MODE", this.BENCHMARK_WORK_MODE);

    properties.put("RESULT_PRECISION", this.RESULT_PRECISION + "%");
    properties.put("DBConfig", this.dbConfig);
    properties.put("DOUBLE_WRITE", this.IS_DOUBLE_WRITE);
    if (this.isIS_DOUBLE_WRITE()) {
      properties.put("ANOTHER DBConfig", this.ANOTHER_DBConfig);
      properties.put("IS_COMPASSION", this.IS_COMPARISON);
      properties.put("IS_POINT_COMPARISON", this.IS_POINT_COMPARISON);
    }
    properties.put("BENCHMARK_CLUSTER", this.BENCHMARK_CLUSTER);
    if (this.BENCHMARK_CLUSTER) {
      properties.put("BENCHMARK_INDEX", this.BENCHMARK_INDEX);
      properties.put("FIRST_DEVICE_INDEX", this.FIRST_DEVICE_INDEX);
      properties.put("IS_ALL_NODES_VISIBLE", this.IS_ALL_NODES_VISIBLE);
    }
    properties.put("OPERATION_PROPORTION", this.OPERATION_PROPORTION);
    properties.put("INSERT_DATATYPE_PROPORTION", this.INSERT_DATATYPE_PROPORTION);
    properties.put("IS_DELETE_DATA", this.IS_DELETE_DATA);
    properties.put("CREATE_SCHEMA", this.CREATE_SCHEMA);
    properties.put("IS_CLIENT_BIND", this.IS_CLIENT_BIND);
    properties.put("CLIENT_NUMBER", this.CLIENT_NUMBER);
    properties.put("GROUP_NUMBER", this.GROUP_NUMBER);
    properties.put("SG_STRATEGY", this.SG_STRATEGY);
    properties.put("DEVICE_NUMBER", this.DEVICE_NUMBER);
    properties.put("REAL_INSERT_RATE", this.REAL_INSERT_RATE);
    properties.put("SENSOR_NUMBER", this.SENSOR_NUMBER);
    properties.put("IS_SENSOR_TS_ALIGNMENT", this.IS_SENSOR_TS_ALIGNMENT);
    properties.put("BATCH_SIZE_PER_WRITE", this.BATCH_SIZE_PER_WRITE);
    properties.put("LOOP", this.LOOP);
    properties.put("POINT_STEP", this.POINT_STEP);
    properties.put("OP_INTERVAL", this.OP_INTERVAL);
    properties.put("QUERY_INTERVAL", this.QUERY_INTERVAL);
    properties.put("IS_OUT_OF_ORDER", this.IS_OUT_OF_ORDER);
    properties.put("OUT_OF_ORDER_MODE", this.OUT_OF_ORDER_MODE);
    properties.put("OUT_OF_ORDER_RATIO", this.OUT_OF_ORDER_RATIO);
    properties.put("IS_REGULAR_FREQUENCY", this.IS_REGULAR_FREQUENCY);
    properties.put("START_TIME", this.START_TIME);
    return properties;
  }

  /**
   * get all properties from config, one property in one line.
   *
   * @return
   */
  public Map<String, Object> getAllProperties() {
    Map<String, Object> properties = getShowProperties();
    properties.put("TIMESTAMP_PRECISION", this.TIMESTAMP_PRECISION);
    properties.put("STRING_LENGTH", this.STRING_LENGTH);
    properties.put("ENABLE_THRIFT_COMPRESSION", this.ENABLE_THRIFT_COMPRESSION);
    properties.put("WRITE_OPERATION_TIMEOUT_MS", this.WRITE_OPERATION_TIMEOUT_MS);
    properties.put("READ_OPERATION_TIMEOUT_MS", this.READ_OPERATION_TIMEOUT_MS);
    if (this.IS_OUT_OF_ORDER) {
      properties.put("LAMBDA", this.LAMBDA);
      properties.put("MAX_K", this.MAX_K);
    }
    properties.put("STEP_SIZE", this.STEP_SIZE);
    properties.put("QUERY_SENSOR_NUM", this.QUERY_SENSOR_NUM);
    properties.put("QUERY_DEVICE_NUM", this.QUERY_DEVICE_NUM);
    properties.put("QUERY_AGGREGATE_FUN", this.QUERY_AGGREGATE_FUN);
    properties.put("QUERY_LOWER_VALUE", this.QUERY_LOWER_VALUE);
    properties.put("QUERY_SEED", this.QUERY_SEED);
    properties.put("QUERY_LIMIT_N", this.QUERY_LIMIT_N);
    properties.put("QUERY_LIMIT_OFFSET", this.QUERY_LIMIT_OFFSET);
    properties.put("QUERY_SLIMIT_N", this.QUERY_SLIMIT_N);
    properties.put("QUERY_SLIMIT_OFFSET", this.QUERY_SLIMIT_OFFSET);
    properties.put("WORKLOAD_BUFFER_SIZE", this.WORKLOAD_BUFFER_SIZE);
    return properties;
  }
}
