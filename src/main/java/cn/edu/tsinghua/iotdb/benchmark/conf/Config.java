package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.workload.reader.DataSet;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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

  public Config() {
  }

  public String host = "127.0.0.1";
  public String port = "6667";

  /**
   * 设备数量
   */
  public int DEVICE_NUMBER = 2;
  /**
   * 设备和客户端是否绑定
   */
  public boolean IS_CLIENT_BIND = true;
  /**
   * 测试客户端线程数量
   */
  public int CLIENT_NUMBER = 2;
  /**
   * 每个设备的传感器数量
   */
  public int SENSOR_NUMBER = 5;
  /**
   * 数据采集步长
   */
  public long POINT_STEP = 7000;
  /**
   * 查询时间戳变化增加步长
   */
  public int STEP_SIZE = 1;
  /**
   * 数据发送缓存条数
   */
  public int BATCH_SIZE = 10;
  /**
   * 存储组数量
   */
  public int GROUP_NUMBER = 1;
  /**
   * 数据类型
   */
  public String DATA_TYPE = "FLOAT";
  /**
   * 数据编码方式
   */
  public String ENCODING = "PLAIN";
  /**
   * 数据压缩方式
   */
  public String COMPRESSOR = "UNCOMPRESSED";
  /**
   * 是否为多设备批插入模式
   */
  public boolean MUL_DEV_BATCH = false;
  /**
   * 数据库初始化等待时间ms
   */
  public long INIT_WAIT_TIME = 5000;
  /**
   * 是否为批插入乱序模式
   */
  public boolean IS_OVERFLOW = false;
  /**
   * 乱序模式
   */
  public int OVERFLOW_MODE = 0;
  /**
   * 批插入乱序比例
   */
  public double OVERFLOW_RATIO = 1.0;

  public double LAMBDA = 3;

  public int MAX_K = 10;

  public boolean IS_RANDOM_TIMESTAMP_INTERVAL = false;

  public int START_TIMESTAMP_INDEX = 20;

  public boolean USE_OPS = false;

  public double CLIENT_MAX_WRT_RATE = 10000000.0;

  public int LIMIT_CLAUSE_MODE = 0;

  public String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0";

  public String START_TIME = "2018-8-30T00:00:00+08:00";

  /**
   * 系统性能检测时间间隔-2秒
   */
  public int INTERVAL = 0;
  /**
   * 系统性能检测网卡设备名
   */
  public String NET_DEVICE = "e";
  /**
   * 存储系统性能信息的文件路径
   */
  public String SERVER_MODE_INFO_FILE = "";
  /**
   * 一个样例数据的存储组名称
   */
  public String STORAGE_GROUP_NAME;
  /**
   * 一个样例数据的时序名称
   */
  public String TIMESERIES_NAME;
  /**
   * 一个时序的数据类型
   */
  public String TIMESERIES_TYPE;
  /**
   * 时序数据取值范围
   */
  public String TIMESERIES_VALUE_SCOPE;
  /**
   * 样例数据生成路径及文件名
   */
  public String GEN_DATA_FILE_PATH = "/home/liurui/sampleData";
  /**
   * 上一次结果的日志路径
   */
  public String LAST_RESULT_PATH = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/logs";
  /**
   * 存放SQL语句文件的完整路径
   */
  public String SQL_FILE = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/SQLFile";
  /**
   * 文件的名字
   */
  public String FILE_PATH;
  /**
   * 数据集的名字
   */
  public DataSet DATA_SET;
  /**
   * 数据集的传感器
   */
  public List<String> FIELDS;
  /**
   * 数据集的传感器的精度
   */
  public int[] PRECISION;
  /**
   * 是否从文件读取数据
   */
  public boolean READ_FROM_FILE = false;
  /**
   * 一次插入到数据库的条数
   */
  public int BATCH_OP_NUM = 100;

  public boolean TAG_PATH = true;

  public String LOG_STOP_FLAG_PATH;

  public int FIRST_DEVICE_INDEX = 0;

  public int STORE_MODE = 1;

  public long LOOP = 10000;

  /**
   * 数据采集丢失率
   */
  public double POINT_LOSE_RATIO = 0.01;
  // ============各函数比例start============//FIXME 传参数时加上这几个参数
  /**
   * 线性 默认 9个 0.054
   */
  public double LINE_RATIO = 0.054;
  /**
   * 傅里叶函数 6个 0.036
   */
  // public static double SIN_RATIO=0.386;//0.036
  public double SIN_RATIO = 0.036;// 0.036
  /**
   * 方波 9个 0.054
   */
  public double SQUARE_RATIO = 0.054;
  /**
   * 随机数 默认 86个 0.512
   */
  public double RANDOM_RATIO = 0.512;
  /**
   * 常数 默认 58个 0.352
   */
  // public static double CONSTANT_RATIO= 0.002;//0.352
  public double CONSTANT_RATIO = 0.352;// 0.352

  // ============各函数比例end============
  public long DATA_SEED = 666L;
  /**
   * 内置函数参数
   */
  public List<FunctionParam> LINE_LIST = new ArrayList<FunctionParam>();
  public List<FunctionParam> SIN_LIST = new ArrayList<FunctionParam>();
  public List<FunctionParam> SQUARE_LIST = new ArrayList<FunctionParam>();
  public List<FunctionParam> RANDOM_LIST = new ArrayList<FunctionParam>();
  public List<FunctionParam> CONSTANT_LIST = new ArrayList<FunctionParam>();
  /**
   * 设备编号
   */
  public List<String> DEVICE_CODES = new ArrayList<String>();
  /**
   * 传感器编号
   */
  public List<String> SENSOR_CODES = new ArrayList<String>();
  /**
   * 设备_传感器 时间偏移量
   */
  public Map<String, Long> SHIFT_TIME_MAP = new HashMap<String, Long>();
  /**
   * 传感器对应的函数
   */
  public Map<String, FunctionParam> SENSOR_FUNCTION = new HashMap<String, FunctionParam>();

  /**
   * 历史数据开始时间
   */
  public long HISTORY_START_TIME;
  /**
   * 历史数据结束时间
   */
  public long HISTORY_END_TIME;

  // 负载生成器参数 start
  /**
   * LoadBatchId 批次id
   */
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
  public long REAL_QUERY_START_TIME = 0;
  public long REAL_QUERY_STOP_TIME = Long.MAX_VALUE;

  //mysql相关参数
  // mysql服务器URL以及用户名密码
  public String MYSQL_URL = "jdbc:mysql://166.111.141.168:3306/benchmark?"
      + "user=root&password=Ise_Nel_2017&useUnicode=true&characterEncoding=UTF8&useSSL=false";
  //是否将结果写入mysql
  public boolean IS_USE_MYSQL = false;
  public boolean IS_SAVE_DATAMODEL = false;

  public String REMARK = "";
  public String VERSION = "";

  // DB参数
  // 服务器URL
  public String DB_URL = "http://localhost:8086";
  // 使用的数据库名
  public String DB_NAME = "test";

  // 使用的数据库
  public String DB_SWITCH = "IoTDB";

  //benchmark 运行模式
  public String BENCHMARK_WORK_MODE = "";
  //the file path of import data
  public String IMPORT_DATA_FILE_PATH = "";
  //import csv数据文件时的BATCH
  public int BATCH_EXECUTE_COUNT = 5000;
  //mataData文件路径
  public String METADATA_FILE_PATH = "";
  //写入text类型指定数据
  public String TEXT_GEN = "000105dc130703081c1f00001b6300001b6400001b6500001b6600001b6700001b680dc0150001000100020002000700070c050401010a000000000000000063000000000000008800034803480348000148db00013e700048f8640011ac7e0025d76c00fc000c041a0004f2b7000114fb0001326700013f78000151eb196400002b190200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000fcffffffffffffffffffffffffffff3f00c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c003000c0000003000c00300001000200010002000100020001000200010002000100020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004b4b4b4b646464643c3c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000045044f0f00c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000090009000900090f3a163a9e3a803a740f16e0168c16a616b50f1c8e1c931c921c910f197c198d199b199c9f000025002500250025ffedffedffedffed00000000000000000000000000000000000000000000000000000000000000000000000000000009000900090009ff033afd3b1d3b283b10006b006c006d006c00330031003300340f0eea0efe0ee10ee80439043bffecfff2090b6f0af3f500000900090009000900000000000000000000000000000000000000000000000000000000000000000000440c1c1c13191e1affc0ffffff0f00000000f0ff41784178408840883ffc3ffc3f983f983f983f983f843f84ff0f000000000000000000000000000000000000000000000000ff0f000000000000000000000000000000000000000000000000ff0f1e1e1e1e19a019a01806180618d818d81ba81ba81e5a1e5aff0f1a681a681a901a90186a186a19501950198c198c19c819c80f418c418c409c409c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000989c9da09e9c98969596979600983f060606060606929448920400000000f0f0f0f0f0f0606060605060f0f0f0f0f0f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003af23afafdffffff555555555555555555555555555555cf0100000000000000000000000000000000000000000000000000010300010001000100010103002b002b002b002b0000000000040003000000000004000300000000000400030000000000040003030e030e030e030e01080108010801080108010801020102010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010100000300000301084b01084b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002019e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

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
    if (WRITE_RATIO == 0 && RANDOM_INSERT_RATIO == 0 && MAX_QUERY_RATIO == 0
        && SIMPLE_QUERY_RATIO == 0
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
    if (sumRatio != 0 && CONSTANT_RATIO >= 0 && LINE_RATIO >= 0 && RANDOM_RATIO >= 0
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
  public List<String> initSensorCodes() {
    for (int i = 0; i < SENSOR_NUMBER; i++) {
      String sensorCode = "s_" + i;
      SENSOR_CODES.add(sensorCode);
    }
    return SENSOR_CODES;
  }

  /**
   * 根据设备数，初始化设备编号
   */
  public List<String> initDeviceCodes() {
    for (int i = FIRST_DEVICE_INDEX; i < DEVICE_NUMBER + FIRST_DEVICE_INDEX; i++) {
      String deviceCode = "d_" + i;
      DEVICE_CODES.add(deviceCode);
    }
    return DEVICE_CODES;
  }

  public String getSensorCodeByRandom() {
    List<String> sensors = SENSOR_CODES;
    int size = sensors.size();
    Random r = new Random(QUERY_SEED);
    return sensors.get(r.nextInt(size));
  }


  public void initRealDataSetSchema() {
    switch (DATA_SET) {
      case TDRIVE:
        FIELDS = Arrays.asList("longitude", "latitude");
        PRECISION = new int[]{5, 5};
        break;
      case REDD:
        FIELDS = Arrays.asList("v");
        PRECISION = new int[]{2};
        break;
      case GEOLIFE:
        FIELDS = Arrays.asList("Latitude", "Longitude", "Zero", "Altitude");
        PRECISION = new int[]{6, 6, 0, 12};
        break;
      default:
        throw new RuntimeException(DATA_SET + " is not support");
    }
  }


  public String getDeviceCodeByRandom() {
    List<String> devices = DEVICE_CODES;
    int size = devices.size();
    Random r = new Random(QUERY_SEED);
    return devices.get(r.nextInt(size));
  }

  public static void main(String[] args) {
    // Config config = Config.newInstance();

  }
}
