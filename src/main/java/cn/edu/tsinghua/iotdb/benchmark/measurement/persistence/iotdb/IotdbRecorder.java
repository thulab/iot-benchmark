package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Metric;
import cn.edu.tsinghua.iotdb.benchmark.measurement.TotalOperationResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.TotalResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.SingleTestMetrics;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(IotdbRecorder.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String CREATE_SERIES_SQL = "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
    private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
    private Connection connection;
    private static final long EXP_TIME = System.currentTimeMillis();
    private static final String PATH_PREFIX = Constants.ROOT_SERIES_NAME + "." + config.TEST_DATA_STORE_DB;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
    private String projectID = String.format("%s_%s_%s", config.DB_SWITCH, config.REMARK, sdf.format(new java.util.Date(EXP_TIME)));
    private Statement globalStatement;
    private static final String THREAD_PREFIX = "pool-1-thread-";
    private String resultPrefix = "insert into " + PATH_PREFIX;
    private String operationResultPrefix = resultPrefix + "." + projectID + ".";
    private long count = 0;
    private static final String ENCODING = "PLAIN";
    private static final String COMPRESS = "UNCOMPRESSED";
    private static final String TOTAL_RESULT_TYPE = "DOUBLE";

    public IotdbRecorder() {
        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            connection = DriverManager
                .getConnection(String.format(Constants.URL, config.TEST_DATA_STORE_IP, config.TEST_DATA_STORE_PORT),
                    config.TEST_DATA_STORE_USER, config.TEST_DATA_STORE_PW);
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
            // ignore if already has the storage group
        }
        // create time series
        initSingleTestMetrics();
        initResultMetrics();
    }

    private void initResultMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (Operation op : Operation.values()) {
                for (Metric metric : Metric.values()) {
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + metric.getName(),
                        TOTAL_RESULT_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
                for(TotalOperationResult totalOperationResult : TotalOperationResult.values()){
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + totalOperationResult.getName(),
                        TOTAL_RESULT_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
            }
            for(TotalResult totalResult : TotalResult.values()){
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + ".total"
                        + "." + totalResult.getName(),
                    TOTAL_RESULT_TYPE, ENCODING, COMPRESS);
                statement.addBatch(createSeriesSql);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains("already exist")) {
                LOGGER.error("create schema error :", e);
            }
        }
    }

    private void initSingleTestMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
                for (int i = 1; i <= config.CLIENT_NUMBER; i++) {
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
        }
    }

    @Override
    public void insertSystemMetrics(double cpu, double mem, double io, double networkReceive, double networkSend,
        double processMemSize, double dataSize, double systemSize, double sequenceSize, double overflowSize,
        double walSize, float tps, float ioRead, float ioWrite, List<Integer> openFileList) {

    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        StringBuilder builder = new StringBuilder(operationResultPrefix).append(Thread.currentThread().getName());
        long currTime = System.currentTimeMillis();
        builder.append(".").append(operation)
            .append("(timestamp");
        for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
            builder.append(",").append(metrics.getName());
        }
        builder.append(") values(");
        builder.append(currTime);
        builder.append(",").append(okPoint);
        builder.append(",").append(failPoint);
        builder.append(",").append(latency);
        builder.append(",'").append(remark).append("'");
        addBatch(builder);
    }

    @Override public void saveResult(String operation, String k, String v) {
        StringBuilder builder = new StringBuilder(resultPrefix);
        builder.append(".").append(operation).append("(timestamp");
        builder.append(",").append(k);
        builder.append(") values(");
        builder.append(EXP_TIME);
        builder.append(",").append(v);
        addBatch(builder);
    }

    private void addBatch(StringBuilder builder) {
        builder.append(")");
        try {
            globalStatement.addBatch(builder.toString());
            count ++;
            if(count % 5000 == 0) {
                globalStatement.executeBatch();
                globalStatement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("Add batch failed", e);
        }
    }

    @Override public void saveTestConfig() {

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
