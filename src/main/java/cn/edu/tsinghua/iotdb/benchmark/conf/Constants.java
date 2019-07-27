package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;

/**
 * 系统运行常量值
 */
public class Constants {
    public static final String START_TIME = "2018-8-30T00:00:00+08:00";
    public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(START_TIME);
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
    public static final String DB_INFLUX = "InfluxDB";
    public static final String DB_OPENTS = "OpenTSDB";
    public static final String DB_CTS = "CTSDB";
    public static final String DB_KAIROS = "KairosDB";
    public static final String DB_TIMESCALE = "TimescaleDB";
    public static final String DB_FAKE = "FakeDB";
    //special DB_SWITCH
    public static final String BENCHMARK_IOTDB = "App";

    public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

    public static final String[] QUERY_CHOICE_NAME = {
            " ",
            "Precise",
            "Fuzzy",
            "Aggregation",
            "Range",
            "Criteria",
            "Nearest Point",
            "Group By",
            "SLimit",
            "Limit Criteria",
            "Aggregation Without Filter",
            "Aggregation With Value Filter"
    };

    public static final String SAMPLE_DATA_FILE_NAME = "sampleData.txt";
    //different running mode
    public static final String MODE_IMPORT_DATA_FROM_CSV = "importDataFromCSV";
    public static final String MODE_WRITE_WITH_REAL_DATASET = "writeWithRealDataSet";
    public static final String MODE_QUERY_WITH_REAL_DATASET = "queryWithRealDataSet";
    public static final String MODE_QUERY_TEST_WITH_DEFAULT_PATH = "queryTestWithDefaultPath";
    public static final String MODE_INSERT_TEST_WITH_DEFAULT_PATH = "insertTestWithDefaultPath";
    public static final String MODE_TEST_WITH_DEFAULT_PATH = "testWithDefaultPath";
    public static final String MODE_SERVER_MODE = "serverMODE";
    public static final String MODE_CLIENT_SYSTEM_INFO = "clientSystemInfo";
    public static final String MODE_INSERT_TEST_WITH_USERDEFINED_PATH = "insertTestWithUserDefinedPath";
    public static final String MODE_EXECUTE_SQL_FROM_FILE = "executeSQLFromFile";

    public static final String EXE_SQL_FROM_FILE_MODE = "1";

}
