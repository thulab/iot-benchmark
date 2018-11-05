package cn.edu.tsinghua.iotdb.benchmark.db.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimescaleDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDB.class);
    private static final String convertToHypertable = "SELECT create_hypertable('%s', 'time');";
    private Connection connection;
    private static Config config;
    private List<Point> points;
    private Map<String, String> mp;
    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private ProbTool probTool;
    private final double unitTransfer = 1000000000.0;

    public TimescaleDB(long labID) throws ClassNotFoundException, SQLException {
        Class.forName(Constants.POSTGRESQL_JDBC_NAME);
        config = ConfigDescriptor.getInstance().getConfig();
        points = new ArrayList<>();
        mp = new HashMap<>();
        mySql = new MySqlLog();
        this.labID = labID;
        sensorRandom = new Random(1 + config.QUERY_SEED);
        timestampRandom = new Random(2 + config.QUERY_SEED);
        probTool = new ProbTool();
        connection = DriverManager.getConnection(
                String.format(Constants.POSTGRESQL_URL, config.host, config.port, config.DB_NAME),
                Constants.POSTGRESQL_USER,
                Constants.POSTGRESQL_PASSWD
        );
        mySql.initMysql(labID);
    }

    @Override
    public void init() throws SQLException {

    }

    /**
     * Map the data schema concepts as follow:
     * DB_NAME -> database
     * storage group name -> table name
     * device name -> a field in table
     * sensors -> fields in table
     *
     * Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables
     *
     * -- We start by creating a regular SQL table
     *
     * CREATE TABLE conditions (
     *   time        TIMESTAMPTZ       NOT NULL,
     *   location    TEXT              NOT NULL,
     *   temperature DOUBLE PRECISION  NULL,
     *   humidity    DOUBLE PRECISION  NULL
     * );
     *
     * -- This creates a hypertable that is partitioned by time
     * --   using the values in the `time` column.
     *
     * SELECT create_hypertable('conditions', 'time');
     *
     * @throws SQLException
     */
    @Override
    public void createSchema() {
        ArrayList<String> group = new ArrayList<>();
        for (int i = 0; i < config.GROUP_NUMBER; i++) {
            group.add("group_" + i);
        }
        for (String g : group) {
            createTable(g);
        }

    }

    /**
     * create hypertable
     * @param tableName table name (i.e. storage group name)
     */
    private void createTable(String tableName) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(getCreateTableSQL(tableName));
            statement.execute(String.format(convertToHypertable, tableName));
        } catch (SQLException e) {
            LOGGER.error("Can't create PG table because: {}", e.getMessage());
            e.printStackTrace();
        }
        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.warn("Can't close statement when setting storage group because: {}", e.getMessage());
        }
    }

    /**
     * -- Creating a regular SQL table
     * example:
     *
     * CREATE TABLE group_0 (
     *   time       TIMESTAMPTZ       NOT NULL,
     *   device     TEXT              NOT NULL,
     *   s_0        DOUBLE PRECISION  NULL,
     *   s_1        DOUBLE PRECISION  NULL
     * );
     *
     * @return create table SQL String
     */
    private String getCreateTableSQL(String group){
        StringBuilder SQLBuilder = new StringBuilder("CREATE TABLE ").append(group).append(" (");
        SQLBuilder.append("time TIMESTAMPTZ NOT NULL, device TEXT NOT NULL");
        for(String sensor: config.SENSOR_CODES){
            SQLBuilder.append(", ").append(sensor).append(" ").append(config.DATA_TYPE).append(" PRECISION NULL");
        }
        SQLBuilder.append(");");
        return SQLBuilder.toString();
    }

    @Override
    public long getLabID() {
        return 0;
    }

    private String getGroup(String device) {
        String[] spl = device.split("_");
        int deviceIndex = Integer.parseInt(spl[1]);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupIndex = deviceIndex / groupSize;
        return "group_" + groupIndex;
    }

    /**
     * example:
     *
     * INSERT INTO conditions(time, device, s_0, s_1)
     *   VALUES (1535558400000, 'd_0', 70.0, 50.0);
     *
     * @param batch offset of loop
     * @param index offset of batch
     * @param device the device this sql are writing data into.
     * @return Insert SQL string
     */
    private String createSQLStatment(int batch, int index, String device) {
        String group = getGroup(device);
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(group).append("(time, device");
        for (String sensor : config.SENSOR_CODES) {
            builder.append(", ").append(sensor);
        }
        builder.append(") ").append("VALUES (");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
        if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
            currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
        }
        builder.append(currentTime);
        builder.append(", '").append(device).append("'");
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(");");
        LOGGER.debug("createSQLStatment:  {}", builder.toString());
        return builder.toString();
    }

    @Override
    public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        Statement statement;
        int[] result;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            if (!config.IS_OVERFLOW) {
                for (int i = 0; i < config.CACHE_NUM; i++) {
                    String sql = createSQLStatment(loopIndex, i, device);
                    statement.addBatch(sql);
                }
            } else {
                int shuffleSize = (int) (config.OVERFLOW_RATIO * config.CACHE_NUM);
                int[] shuffleSequence = new int[shuffleSize];
                for (int i = 0; i < shuffleSize; i++) {
                    shuffleSequence[i] = i;
                }

                int tmp = shuffleSequence[shuffleSize - 1];
                shuffleSequence[shuffleSize - 1] = shuffleSequence[0];
                shuffleSequence[0] = tmp;

                for (int i = 0; i < shuffleSize; i++) {
                    String sql = createSQLStatment(loopIndex, shuffleSequence[i], device);
                    statement.addBatch(sql);
                }
                for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
                    String sql = createSQLStatment(loopIndex, i, device);
                    statement.addBatch(sql);
                }
            }

            long startTime = System.nanoTime();
            try {
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;
            latencies.add(costTime);
            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        return 0;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {

    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public long count(String group, String device, String sensor) {
        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        return 0;
    }
}
