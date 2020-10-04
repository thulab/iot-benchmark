package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.Metric;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SingleTestMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalOperationResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(IotdbRecorder.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String CREATE_SERIES_SQL = "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
    private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
    private Connection connection;
    private static final long EXP_TIME = System.currentTimeMillis();
    private static final String PATH_PREFIX = Constants.ROOT_SERIES_NAME + "." + config.getTEST_DATA_STORE_DB();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
    private final String projectID = String.format("%s_%s_%s", config.getDB_SWITCH(), config.getREMARK(), sdf.format(new java.util.Date(EXP_TIME)));
    private Statement globalStatement;
    private static final String THREAD_PREFIX = "pool-1-thread-";
    private final String insertSqlPrefix = "insert into " + PATH_PREFIX;
    private final String operationResultPrefix = insertSqlPrefix + "." + projectID + ".";
    private long count = 0;
    private static final String ENCODING = "PLAIN";
    private static final String COMPRESS = "UNCOMPRESSED";
    private static final String DOUBLE_TYPE = "DOUBLE";
    private static final String ALREADY_KEYWORD = "already exist";
    private static final String CRETE_SCHEMA_ERROR_HINT = "create schema error";
    private static final int SEND_TO_IOTDB_BATCH_SIZE = 1000;
    private String localName;
    private static final String INSERT_SQL_STR1 = ") values(";
    private static final String INSERT_SQL_STR2 = "(timestamp";

    public IotdbRecorder() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            localName = localhost.getHostName();
        } catch (UnknownHostException e) {
            localName = "localName";
            LOGGER.error("Get localhost failed because: {}", e.getMessage(), e);
        }
        localName = localName.replace("-", "_");
        localName = localName.replace(".", "_");
        localName = "_" + localName;
        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            connection = DriverManager
                .getConnection(String.format(Constants.URL, config.getTEST_DATA_STORE_IP(), config.getTEST_DATA_STORE_PORT()),
                    config.getTEST_DATA_STORE_USER(), config.getTEST_DATA_STORE_PW());
            initSchema();
            globalStatement = connection.createStatement();
        } catch (Exception e) {
            LOGGER.error("Initialize IoTDB failed because ", e);
        }
    }

    private void initSchema() {
        try {
            // register storage group using TEST_DATA_STORE_DB
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format(SET_STORAGE_GROUP_SQL, PATH_PREFIX));
            }
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
        // create time series
        if(config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            initSingleTestMetrics();
            initResultMetrics();
        }
        if(config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_SERVER_MODE)) {
            initSystemMetrics();
        }
    }

    /**
     * System metrics include:
     * root.test.localName.
     */
    private void initSystemMetrics() {
        try (Statement statement = connection.createStatement()) {
            for(SystemMetrics systemMetric: SystemMetrics.values()){
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + "." + localName
                        + "." + systemMetric,
                    DOUBLE_TYPE, ENCODING, COMPRESS);
                statement.addBatch(createSeriesSql);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    private void initResultMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (Operation op : Operation.values()) {
                for (Metric metric : Metric.values()) {
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + metric.getName(),
                        DOUBLE_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
                for(TotalOperationResult totalOperationResult : TotalOperationResult.values()){
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + totalOperationResult.getName(),
                        DOUBLE_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
            }
            for(TotalResult totalResult : TotalResult.values()){
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + ".total"
                        + "." + totalResult.getName(),
                    DOUBLE_TYPE, ENCODING, COMPRESS);
                statement.addBatch(createSeriesSql);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    private void initSingleTestMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
                for (int i = 1; i <= config.getCLIENT_NUMBER(); i++) {
                    for(Operation op: Operation.values()){
                        String threadName = THREAD_PREFIX + i;
                        String createSeriesSql = String.format(CREATE_SERIES_SQL,
                            PATH_PREFIX
                                + "." + projectID
                                + "." + threadName
                                + "." + op
                                + "." + metrics.getName(),
                            metrics.getType(), ENCODING, COMPRESS);
                        statement.addBatch(createSeriesSql);
                    }
                }
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    private void addBatch(StringBuilder builder) {
        builder.append(")");
        try {
            globalStatement.addBatch(builder.toString());
            count ++;
            if(count % SEND_TO_IOTDB_BATCH_SIZE == 0) {
                globalStatement.executeBatch();
                globalStatement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("Add batch failed", e);
        }
    }

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        try (Statement statement = connection.createStatement()) {
            long currTime = System.currentTimeMillis();
            StringBuilder builder = new StringBuilder(insertSqlPrefix).append(".").append(localName).append(INSERT_SQL_STR2);
            StringBuilder valueBuilder = new StringBuilder(INSERT_SQL_STR1).append(currTime);
            for(Map.Entry entry: systemMetricsMap.entrySet()) {
                builder.append(",").append(entry.getKey());
                if (entry.getValue() == null) {
                    valueBuilder.append(",").append(0);
                } else {
                    valueBuilder.append(",").append(entry.getValue());
                }
            }
            builder.append(valueBuilder).append(")");
            statement.execute(builder.toString());
        } catch (SQLException e) {
            LOGGER.error("insert system metric data failed ", e);
        }
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        StringBuilder builder = new StringBuilder(operationResultPrefix).append(Thread.currentThread().getName());
        long currTime = System.currentTimeMillis();
        builder.append(".").append(operation)
            .append(INSERT_SQL_STR2);
        for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
            builder.append(",").append(metrics.getName());
        }
        builder.append(INSERT_SQL_STR1);
        builder.append(currTime);
        builder.append(",").append(okPoint);
        builder.append(",").append(failPoint);
        builder.append(",").append(latency);
        builder.append(",'").append(remark).append("'");
        addBatch(builder);
    }

    @Override
    public void saveResult(String operation, String k, String v) {
        StringBuilder builder = new StringBuilder(insertSqlPrefix);
        builder.append(".").append(operation).append(INSERT_SQL_STR2);
        builder.append(",").append(k);
        builder.append(INSERT_SQL_STR1);
        builder.append(EXP_TIME);
        builder.append(",").append(v);
        addBatch(builder);
    }

    @Override public void saveTestConfig() {
        // TO do
    }

    @Override public void close() {
        try {
            globalStatement.executeBatch();
            globalStatement.clearBatch();
            globalStatement.close();
            connection.close();
        } catch (SQLException e) {
            LOGGER.error("close failed", e);
        }
    }
}
