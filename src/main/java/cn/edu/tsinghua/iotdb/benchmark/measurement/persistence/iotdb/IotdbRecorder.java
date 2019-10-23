package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
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
    private static final long SHIFT_NANO_TIME = EXP_TIME * 1000000 - System.nanoTime();
    private static final String PATH_PREFIX = Constants.ROOT_SERIES_NAME + "." + config.TEST_DATA_STORE_DB;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
    private String projectID = String.format("%s_%s_%s", config.DB_SWITCH, config.REMARK, sdf.format(new java.util.Date(EXP_TIME)));
    private Statement globalStatement;
    private long count = 0;

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
    }

    private void initSingleTestMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (SingleTestMetrics metrics: SingleTestMetrics.values()) {
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + "." + projectID
                        + "." + metrics.name,
                    metrics.type, "PLAIN", "UNCOMPRESSED");
                statement.addBatch(createSeriesSql);
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
        StringBuilder builder = new StringBuilder();
        //long currNanoTime = System.nanoTime() + SHIFT_NANO_TIME;
        long currNanoTime = System.currentTimeMillis();
        builder.append("insert into ")
            .append(PATH_PREFIX)
            .append(".").append(projectID)
            .append("(timestamp");
        for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
            builder.append(",").append(metrics.name);
        }
        builder.append(") values(");
        builder.append(currNanoTime);

        builder.append(",'").append(Thread.currentThread().getName()).append("'");
        builder.append(",'").append(operation).append("'");
        builder.append(",").append(okPoint);
        builder.append(",").append(failPoint);
        builder.append(",").append(latency);
        builder.append(",'").append(remark).append("'");
        builder.append(")");

        try {
            globalStatement.addBatch(builder.toString());
            count ++;
            if(count % 5000 == 0) {
                globalStatement.executeBatch();
                globalStatement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("saveOperationResult add batch failed", e);
        }

        // String sql = builder.toString();
        // LOGGER.info("SQL: {}", sql);
    }

    @Override public void saveResult(String operation, String k, String v) {

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

    public enum SingleTestMetrics {
        CLIENT_NAME("clientName", "TEXT"),
        OPERATION("operation","TEXT"),
        OK_POINT("okPoint","INT32"),
        FAIL_POINT("failPoint","INT32"),
        LATENCY("latency","DOUBLE"),
        REMARK("remark","TEXT");

        SingleTestMetrics(String n, String t) {
            name = n;
            type = t;
        }

        String name;
        String type;
    }
}
