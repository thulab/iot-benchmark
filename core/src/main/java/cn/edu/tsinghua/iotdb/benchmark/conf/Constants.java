package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;

/**
 * 系统运行常量值
 */
public class Constants {
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
    public static final String URL = "jdbc:iotdb://%s:%s/";
    public static final String USER = "root";
    public static final String PASSWD = "root";
    public static final String ROOT_SERIES_NAME = "root";
    public static final String CONSOLE_PREFIX = "IotDB-benchmark>";
    public static final String BENCHMARK_CONF = "benchmark-conf";
    public static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
    public static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";
    public static final String POSTGRESQL_USER = "postgres";
    public static final String POSTGRESQL_PASSWD = "postgres";
    //support DB names of DB_SWITCH
    public static final String DB_IOT = "IoTDB";
    public static final String DB_DOUBLE_IOT = "DoubleIoTDB";
    public static final String DB_INFLUX = "InfluxDB";
    public static final String DB_OPENTS = "OpenTSDB";
    public static final String DB_CTS = "CTSDB";
    public static final String DB_KAIROS = "KairosDB";
    public static final String DB_TIMESCALE = "TimescaleDB";
    public static final String DB_FAKE = "FakeDB";
    public static final String DB_TAOSDB = "TaosDB";
    //special DB_SWITCH
    public static final String BENCHMARK_IOTDB = "App";

    public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

    //different running mode
    public static final String MODE_IMPORT_DATA_FROM_CSV = "importDataFromCSV";
    public static final String MODE_WRITE_WITH_REAL_DATASET = "writeWithRealDataSet";
    public static final String MODE_QUERY_WITH_REAL_DATASET = "queryWithRealDataSet";
    public static final String MODE_TEST_WITH_DEFAULT_PATH = "testWithDefaultPath";
    public static final String MODE_SERVER_MODE = "serverMODE";
    public static final String MODE_CLIENT_SYSTEM_INFO = "clientSystemInfo";

    //different insert mode
    public static final String INSERT_USE_JDBC = "jdbc";
    public static final String INSERT_USE_SESSION = "session";

    // support test data persistence:
    public static final String TDP_NONE = "None";
    public static final String TDP_IOTDB = "IoTDB";
    public static final String TDP_MYSQL = "MySQL";
    public static final String TDP_CSV = "CSV";

    // device and storage group assignment
    public static final String MOD_SG_ASSIGN_MODE = "mod";
    public static final String HASH_SG_ASSIGN_MODE = "hash";
    public static final String DIV_SG_ASSIGN_MODE = "div";

    public static final String IOTDB011_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB";
    public static final String IOTDB011_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDBSession";
    public static final String IOTDB010_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDB";
    public static final String IOTDB010_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDBSession";
    public static final String IOTDB009_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDB";
    public static final String IOTDB009_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDBSession";
    public static final String INFLUXDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB";
}
