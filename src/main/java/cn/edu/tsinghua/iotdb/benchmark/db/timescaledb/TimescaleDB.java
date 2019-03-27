package cn.edu.tsinghua.iotdb.benchmark.db.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
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
    private static final String convertToHypertable = "SELECT create_hypertable('%s', 'time', chunk_time_interval => 86400000);";
    private static final String dropTable = "DROP TABLE %s;";
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
    public void init() {
        //delete old data
        Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {

            e.printStackTrace();
        }
        String table = config.DB_NAME;
        try {
            assert statement != null;
            statement.execute(String.format(dropTable, table));
            // wait for deletion complete
            try {
                LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
                Thread.sleep(config.INIT_WAIT_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            LOGGER.warn("delete old data table {} failed, because: {}", table, e.getMessage());
            if(!e.getMessage().contains("does not exist")) {
                e.printStackTrace();
            }
        } finally {
            try {
                assert statement != null;
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Map the data schema concepts as follow:
     * DB_NAME -> database
     * storage group name -> table name
     * device name -> a field in table
     * sensors -> fields in table
     * <p>
     * Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables
     * <p>
     * -- We start by creating a regular SQL table
     * <p>
     * CREATE TABLE conditions (
     * time        TIMESTAMPTZ       NOT NULL,
     * location    TEXT              NOT NULL,
     * temperature DOUBLE PRECISION  NULL,
     * humidity    DOUBLE PRECISION  NULL
     * );
     * <p>
     * -- This creates a hypertable that is partitioned by time
     * --   using the values in the `time` column.
     * <p>
     * SELECT create_hypertable('conditions', 'time');
     *
     * @throws SQLException
     */
    @Override
    public void createSchema() {
        createTable(config.DB_NAME);
    }

    /**
     * create hypertable
     *
     * @param tableName table name (i.e. storage group name)
     */
    private void createTable(String tableName) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(getCreateTableSQL(tableName));
            LOGGER.debug("CreateTableSQL Statement:  {}", getCreateTableSQL(tableName));
            statement.execute(String.format(convertToHypertable, tableName));
            LOGGER.debug("convertToHypertable Statement:  {}", String.format(convertToHypertable, tableName));
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
     * <p>
     * CREATE TABLE group_0 (
     * time       BIGINT              NOT NULL,
     * device     TEXT                NOT NULL,
     * s_0        DOUBLE PRECISION    NULL,
     * s_1        DOUBLE PRECISION    NULL
     * );
     *
     * @return create table SQL String
     */
    private String getCreateTableSQL(String tableName) {
        StringBuilder SQLBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        SQLBuilder.append("time BIGINT NOT NULL, sGroup TEXT NOT NULL, device TEXT NOT NULL");
        for (String sensor : config.SENSOR_CODES) {
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
     * <p>
     * INSERT INTO conditions(time, group, device, s_0, s_1)
     * VALUES (1535558400000, 'group_0', 'd_0', 70.0, 50.0);
     *
     * @param batch  offset of loop
     * @param index  offset of batch
     * @param device the device this sql are writing data into.
     * @return Insert SQL string
     */
    private String createSQLStatment(int batch, int index, String device) {
        String group = getGroup(device);
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(config.DB_NAME).append("(time, sGroup, device");
        for (String sensor : config.SENSOR_CODES) {
            builder.append(", ").append(sensor);
        }
        builder.append(") ").append("VALUES (");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.BATCH_SIZE
            + index);
        if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
            currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
        }
        builder.append(currentTime);
        builder.append(", '").append(group).append("'");
        builder.append(", '").append(device).append("'");
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(", ").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(");");
        LOGGER.debug("createSQLStatement:  {}", builder.toString());
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
                for (int i = 0; i < config.BATCH_SIZE; i++) {
                    String sql = createSQLStatment(loopIndex, i, device);
                    statement.addBatch(sql);
                }
            }

            long startTime = System.nanoTime();
            errorNum = getErrorNum(statement);
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;
            latencies.add(costTime);
            if (errorNum > 0) {
                LOGGER.info("Batch insert failed point number is {}! ", errorNum);
            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.BATCH_SIZE * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
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
        if (connection != null) {
            connection.close();
        }
        if (mySql != null) {
            mySql.closeMysql();
        }
    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        return 0;
    }

    /**
     * 创建查询语句--(查询设备下的num个传感器数值)
     * notice: select time so result contains timestamp like other TSDBs for fairness
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        if (num > config.SENSOR_NUMBER) {
            throw new SQLException("config.SENSOR_NUMBER is " + config.SENSOR_NUMBER
                    + " shouldn't less than the number of fields in querySql");
        }
        List<String> list = new ArrayList<>(config.SENSOR_CODES);
        Collections.shuffle(list, sensorRandom);
        builder.append("time, device");
        for (int i = 0; i < num; i++) {
            builder.append(", ").append(list.get(i));
        }
        builder.append(" FROM ").append(config.DB_NAME);
        builder.append(" WHERE ");
        builder.append(getDeviceCondition(devices));
        return builder.toString();
    }

    private String getDeviceCondition(List<Integer> devices){
        StringBuilder builder = new StringBuilder("(");
        for(int i: devices){
            builder.append("device='d_").append(i).append("'").append(" OR ");
        }
        String raw = builder.toString();
        return raw.substring(0, raw.length() - 4) + ")";
    }

    /**
     * 创建查询语句--(精确点查询)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList) throws SQLException {
        //String strTime = sdf.format(new Date(time));
        String strTime = String.valueOf(time);
        return createQuerySQLStatment(devices, num) + " AND time = " + strTime + " ORDER BY time DESC;";
    }

    /*
    Example of different query types:
    1.Exact point query:
    SELECT s_57 FROM root.performf.group_4.d_49 WHERE time = 2010-01-01 12:00:00
    SELECT group_4.s_57, group_4.s_53, group_5.s_57, group_5.s_53 FROM group_4, group_5 WHERE group_4.device time = 2010-01-01 12:00:00
    2.Aggregation function query:
    SELECT max_value(s_76) FROM root.performf.group_3.d_31 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00
    3.Range query:
    SELECT s_30 FROM root.performf.group_4.d_43 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00
    4.Criteria query:
    SELECT s_39 FROM root.performf.group_2.d_29 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00 AND root.performf.group_2.d_29.s_39 > 0.0
    5.Latest point query:
    SELECT max_time(s_76) FROM root.performf.group_3.d_31
    6.Group-by query:
    SELECT max_value(s_81) FROM root.performf.group_9.d_92 WHERE root.performf.group_9.d_92.s_81 >= 0.0  GROUP BY(600000ms, 1262275200000,[2010-01-01 12:00:00,2010-01-01 13:00:00])
    */

    /**
     * @param devices
     * @param index
     * @param startTime
     * @param client
     * @param errorCount
     * @param latencies
     */
    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
        Statement statement = null;
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0, latency = 0;
        try {
            statement = connection.createStatement();
            List<String> sensorList = new ArrayList<String>();

            switch (config.QUERY_CHOICE) {
                case 1:// 精确点查询
                    //以下语句是为了假若使用 startTimeInterval = database.getTotalTimeInterval() / config.LOOP; 可保证能查出点来
                    long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
                            + Constants.START_TIMESTAMP;
                    if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
                        timeStamp += config.POINT_STEP / 2;
                    }
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, timeStamp, sensorList);
                    break;
                case 2:// 模糊点查询（暂未实现）

                case 3:// 聚合函数查询
                    sql = createQuerySQLStatement(devices, config.QUERY_AGGREGATE_FUN, config.QUERY_SENSOR_NUM, startTime,
                            startTime + config.QUERY_INTERVAL);
                    break;
                case 4:// 范围查询
                    sql = createQuerySQLStatement(devices, config.QUERY_SENSOR_NUM, startTime,
                            startTime + config.QUERY_INTERVAL);
                    break;
                case 5:// 条件查询
                    //sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime, startTime + config.QUERY_INTERVAL, config.QUERY_LOWER_LIMIT, sensorList);
                    break;
                case 6:// 最近点查询
                    //sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, "max_time", sensorList);
                    break;
                case 7:// groupBy查询（暂时只有一个时间段）
                    sql = createQuerySQLStatement(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, startTime,
                            startTime + config.QUERY_INTERVAL);
                    break;
                case 8:// query with limit and series limit and their offsets
                    //int device_id = index % devices.size();
                    //sql = createQuerySQLStatment(device_id, config.QUERY_LIMIT_N, config.QUERY_LIMIT_OFFSET, config.QUERY_SLIMIT_N, config.QUERY_SLIMIT_OFFSET);
                    break;
                case 9:// criteria query with limit
//                    sql = createQuerySQLStatment(
//                            devices,
//                            config.QUERY_SENSOR_NUM,
//                            startTime,
//                            startTime + config.QUERY_INTERVAL,
//                            config.QUERY_LOWER_LIMIT,
//                            sensorList,
//                            config.QUERY_LIMIT_N,
//                            config.QUERY_LIMIT_OFFSET);
                    break;
                case 10:// aggregation function query without any filter
                    sql = createQuerySQLStatement(devices, config.QUERY_AGGREGATE_FUN, config.QUERY_SENSOR_NUM);
                    break;
                case 11:// aggregation function query with value filter
//                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT,
//                            sensorList);
                    break;

            }
            int line = 0;
            LOGGER.info("{} execute {} loop,提交执行的sql：{}", Thread.currentThread().getName(), index, sql);

            startTimeStamp = System.nanoTime();
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                line++;
//				int sensorNum = sensorList.size();
//				builder.append(" \ntimestamp = ").append(resultSet.getString(0)).append("; ");
//				for (int i = 1; i <= sensorNum; i++) {
//					builder.append(resultSet.getString(i)).append("; ");
//				}
            }
            statement.close();
            endTimeStamp = System.nanoTime();
            latency = endTimeStamp - startTimeStamp;
            latencies.add(latency);
            client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM);
            client.setTotalTime(client.getTotalTime() + latency);

            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, (latency / 1000.0) / 1000000.0,
                    line * config.QUERY_SENSOR_NUM,
                    line * config.QUERY_SENSOR_NUM * 1000.0 / (latency / 1000000.0),
                    (client.getTotalTime() / 1000.0) / 1000000.0, client.getTotalPoint(),
                    client.getTotalPoint() * 1000.0f / (client.getTotalTime() / 1000000.0));
            mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM,
                    (latency / 1000.0f) / 1000000.0, config.REMARK);
        } catch (SQLException e) {
            errorCount.set(errorCount.get() + 1);
            LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
            LOGGER.error("执行失败的查询语句：{}", sql);
            mySql.saveQueryProcess(index, 0, -1, "query fail!" + sql);
            e.printStackTrace();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void addFunSensor(String method, int num, StringBuilder builder, List<String> list) {
        if (method.length() > 1) {
            builder.append(method).append("(").append(list.get(0)).append(")");
            for (int i = 1; i < num; i++) {
                builder.append(" , ").append(method).append("(").append(list.get(i)).append(")");
            }
        } else {
            builder.append(list.get(0));
            for (int i = 1; i < num; i++) {
                builder.append(" , ").append(list.get(i));
            }
        }
    }

    /**
     * 创建查询语句--(带有时间约束条件的查询)
     *
     * @throws SQLException
     */
    public String createQuerySQLStatement(List<Integer> devices, int num, long startTime, long endTime) throws SQLException {
//        String strstartTime = sdf.format(new Date(startTime));
//        String strendTime = sdf.format(new Date(endTime));
        StringBuilder builder = new StringBuilder(createQuerySQLStatment(devices, num));
        builder.append(" AND time >= ").append(startTime).append(" AND time <= ").append(endTime);
        return builder.toString();
    }

    /**
     * -- Information about each 15-min period for each location
     * -- over the past 3 hours, ordered by time and temperature
     * SELECT time_bucket('15 minutes', time) AS fifteen_min,
     *   location, COUNT(*),
     *   MAX(temperature) AS max_temp,
     *   MAX(humidity) AS max_hum
     *   FROM conditions
     *   WHERE time > NOW() - interval '3 hours'
     *   GROUP BY fifteen_min, location
     *   ORDER BY fifteen_min DESC, max_temp DESC;
     *
     * @param devices
     * @param querySensorNum
     * @param aggFunction
     * @param startTime
     * @param endTime
     * @return
     */
    private String createQuerySQLStatement(List<Integer> devices, int querySensorNum, String aggFunction, long startTime, long endTime) {
        StringBuilder builder = new StringBuilder(createQuerySQLStatement(devices, querySensorNum, aggFunction));
//        String strstartTime = sdf.format(new Date(startTime));
//        String strendTime = sdf.format(new Date(endTime));
        builder.append(" AND time >= ");
        builder.append(startTime).append(" AND time <= ").append(endTime);
        if(aggFunction.length() > 1){
            builder.append(" GROUP BY sampleTime, device ORDER BY sampleTime DESC;");
        }
        return builder.toString();
    }

    private String createQuerySQLStatement(List<Integer> devices, String method, int num, long startTime, long endTime) {
        StringBuilder builder = new StringBuilder(createQuerySQLStatement(devices, method, num));
        builder.append(" AND time >= ").append(startTime);
        builder.append(" AND time <= ").append(endTime);
        builder.append(" GROUP BY device;");
        return builder.toString();
    }

    /**
     * 创建查询语句--(带有聚合函数的查询)
     * SELECT avg(s_0),
     *      avg(s_1)
     *   FROM test
     *   WHERE device='d_0'
     *
     */
    private String createQuerySQLStatement(List<Integer> devices, String method, int num) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT device, ");
        List<String> list = new ArrayList<>(config.SENSOR_CODES);
        Collections.shuffle(list, sensorRandom);
        addFunSensor(method, num, builder, list);
        builder.append(" FROM ").append(config.DB_NAME);
        builder.append(" WHERE ");
        builder.append(getDeviceCondition(devices));
        if(config.QUERY_CHOICE==10 && method.length() > 1){
            builder.append(" GROUP BY device;");
        }
        return builder.toString();
    }

    /**
     * 创建查询语句--(带有聚合函数的查询)
     * SELECT time_bucket('5 minutes', time)
     *     AS five_min, avg(cpu)
     *   FROM metrics
     *   GROUP BY five_min
     *   ORDER BY five_min DESC LIMIT 10;
     */
    private String createQuerySQLStatement(List<Integer> devices, int num, String method) {
        StringBuilder builder = new StringBuilder();

        if(method.length() > 1) {
            builder.append("SELECT time_bucket(").append(config.TIME_UNIT).append(", time) AS sampleTime, device, ");
        } else {
            builder.append("SELECT time, device, ");
        }

        List<String> list = new ArrayList<>(config.SENSOR_CODES);
        Collections.shuffle(list, sensorRandom);
        addFunSensor(method, num, builder, list);

        builder.append(" FROM ").append(config.DB_NAME);
        builder.append(" WHERE ");
        builder.append(getDeviceCondition(devices));
        return builder.toString();
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
        Statement statement;
        long errorNum = 0;
        int timestampIndex;
        PossionDistribution possionDistribution = new PossionDistribution(random);
        int nextDelta;

        try {
            statement = connection.createStatement();
            String sql;
            for (int i = 0; i < config.BATCH_SIZE; i++) {
                if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, random)) {
                    nextDelta = possionDistribution.getNextPossionDelta();
                    timestampIndex = maxTimestampIndex - nextDelta;
                } else {
                    maxTimestampIndex++;
                    timestampIndex = maxTimestampIndex;
                }
                sql = createSQLStatment(device, timestampIndex);
                statement.addBatch(sql);
            }
            long startTime = System.nanoTime();
            errorNum = getErrorNum(statement);
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
                        (config.BATCH_SIZE * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return maxTimestampIndex;
    }

    private long getErrorNum(Statement statement) throws SQLException {
        long errorNum = 0;
        try {
            statement.executeBatch();
        } catch (BatchUpdateException e) {
            LOGGER.error("Batch insert failed because: {}", e.getMessage());
            errorNum = config.BATCH_SIZE * config.SENSOR_NUMBER;
            e.printStackTrace();
        }
        return errorNum;
    }

    private String createSQLStatment(String device, int timestampIndex) {
        String group = getGroup(device);
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(config.DB_NAME).append("(time, sGroup, device");
        for (String sensor : config.SENSOR_CODES) {
            builder.append(", ").append(sensor);
        }
        builder.append(") ").append("VALUES (");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
        if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
            currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
        }
        builder.append(currentTime);
        builder.append(", '").append(group).append("'");
        builder.append(", '").append(device).append("'");
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(", ").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(");");
        LOGGER.debug("createSQLStatement:  {}", builder.toString());
        return builder.toString();
    }
}
