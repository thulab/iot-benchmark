package cn.edu.tsinghua.iotdb.benchmark.mssqlserver;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @Author stormbroken
 * Create by 2021/08/04
 * @Version 1.0
 **/

public class MsSQLServerDB implements IDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(MsSQLServerDB.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final String DBDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String DBURL = "jdbc:sqlserver://" +
            config.getHOST().get(0) + ":" + config.getPORT().get(0) +
            ";DataBaseName=" + config.getDB_NAME();
    // TODO remove after fix/core merge
    private static final String DBUSER = "test";
    private static final String DBPASSWORD = "12345678";
    private static final SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");


    //数据库连接
    public static Connection connection = null;

    private static final String CREATE_TABLE = "CREATE TABLE [" + config.getDB_NAME() + "]\n" +
            "([pk_fk_Id] [bigint] NOT NULL,\n" +
            "[pk_TimeStamp] [datetime2](7) NOT NULL,\n" +
            "[Value] [float] NULL,\n" +
            "CONSTRAINT PK_test PRIMARY KEY CLUSTERED\n" +
            "([pk_fk_Id] ASC,\n" +
            "[pk_TimeStamp] ASC\n" +
            ") WITH (IGNORE_DUP_KEY = ON) ON [PRIMARY]\n" +
            ")ON [PRIMARY]";

    private static final String DELETE_TABLE = "drop table " + config.getDB_NAME();
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
     * client thread.
     */
    @Override
    public void init() throws TsdbException {
        try {
            Class.forName(DBDRIVER);
            connection = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("Connect Error!");
            throw new TsdbException("Connect Error!", e);
        }
    }

    /**
     * Cleanup any state for this DB, including the old data deletion. Called once before each test if
     * IS_DELETE_DATA=true.
     */
    @Override
    public void cleanup() throws TsdbException {
        try{
            Statement statement = connection.createStatement();
            statement.execute(DELETE_TABLE);
            statement.close();
        }catch (SQLException sqlException){
            LOGGER.warn("No need to clean!");
            throw new TsdbException("No need to clean!", sqlException);
        }
    }

    /**
     * Close the DB instance connections. Called once per DB instance.
     */
    @Override
    public void close() throws TsdbException {
        if(connection != null){
            try{
                connection.close();
            }catch (SQLException sqlException){
                throw new TsdbException("Failed to close", sqlException);
            }
        }
    }

    /**
     * Called once before each test if CREATE_SCHEMA=true.
     *
     * @param schemaList schema of devices to register
     */
    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        try{
            Statement statement = connection.createStatement();
            statement.execute(CREATE_TABLE);
            statement.close();
        }catch (SQLException sqlException){
            LOGGER.warn("Failed to register", sqlException);
        }
    }

    /**
     * Insert one batch into the database, the DB implementation needs to resolve the data in batch
     * which contains device schema and Map[Long, List[String]] records. The key of records is a
     * timestamp and the value is a list of sensor value data.
     *
     * @param batch universal insertion data structure
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status insertOneBatch(Batch batch) throws DBConnectException {
        DeviceSchema deviceSchema = batch.getDeviceSchema();
        List<String> sensors = deviceSchema.getSensors();
        long groupNow = Long.parseLong(deviceSchema.getGroup().replace(config.getDB_NAME(), ""));
        long deviceNow = Long.parseLong(deviceSchema.getDevice().split("_")[1]);
        long idPredix =
                config.getSENSOR_NUMBER() * config.getDEVICE_NUMBER() *
                        (deviceNow + config.getGROUP_NUMBER() * groupNow);
        try{
            Statement statement = connection.createStatement();
            for(Record record: batch.getRecords()){
                String time = format.format(record.getTimestamp());
                List<Object> values = record.getRecordDataValue();
                for(int i = 0 ; i < values.size(); i++){
                    long sensorNow = i + idPredix;
                    StringBuffer sql = new StringBuffer("INSERT INTO ").append(config.getDB_NAME()).append(" values (");
                    sql.append(sensorNow).append(",");
                    sql.append("'").append(time).append("',");
                    sql.append(values.get(i)).append(")");
                    statement.addBatch(sql.toString());
                }
            }
            statement.executeBatch();
            statement.close();
            return new Status(true);
        }catch (SQLException e){
            e.printStackTrace();
            LOGGER.error("Write batch failed");
            return new Status(false, 0, e, e.getMessage());
        }
    }

    /**
     * Insert single-sensor one batch into the database, the DB implementation needs to resolve the
     * data in batch which contains device schema and Map[Long, List[String]] records. The key of
     * records is a timestamp and the value is one sensor value data.
     *
     * @param batch universal insertion data structure
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
        return null;
    }

    /**
     * Query data of one or multiple sensors at a precise timestamp. e.g. select v1... from data where
     * time = ? and device in ?
     *
     * @param preciseQuery universal precise query condition parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        return null;
    }

    /**
     * Query data of one or multiple sensors in a time range. e.g. select v1... from data where time
     * >= ? and time <= ? and device in ?
     *
     * @param rangeQuery universal range query condition parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        return null;
    }

    /**
     * Query data of one or multiple sensors in a time range with a value filter. e.g. select v1...
     * from data where time >= ? and time <= ? and v1 > ? and device in ?
     *
     * @param valueRangeQuery contains universal range query with value filter parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        return null;
    }

    /**
     * Query aggregated data of one or multiple sensors in a time range using aggregation function.
     * e.g. select func(v1)... from data where device in ? and time >= ? and time <= ?
     *
     * @param aggRangeQuery contains universal aggregation query with time filter parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        return null;
    }

    /**
     * Query aggregated data of one or multiple sensors in the whole time range. e.g. select
     * func(v1)... from data where device in ? and value > ?
     *
     * @param aggValueQuery contains universal aggregation query with value filter parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        return null;
    }

    /**
     * Query aggregated data of one or multiple sensors with both time and value filters. e.g. select
     * func(v1)... from data where device in ? and time >= ? and time <= ? and value > ?
     *
     * @param aggRangeValueQuery contains universal aggregation query with time and value filters
     *                           parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        return null;
    }

    /**
     * Query aggregated group-by-time data of one or multiple sensors within a time range. e.g. SELECT
     * max(s_0), max(s_1) FROM group_0, group_1 WHERE ( device = ’d_3’ OR device = ’d_8’) AND time >=
     * 2010-01-01 12:00:00 AND time <= 2010-01-01 12:10:00 GROUP BY time(60000ms)
     *
     * @param groupByQuery contains universal group by query condition parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        return null;
    }

    /**
     * Query the latest(max-timestamp) data of one or multiple sensors. e.g. select time, v1... where
     * device = ? and time = max(time)
     *
     * @param latestPointQuery contains universal latest point query condition parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        return null;
    }

    /**
     * similar to rangeQuery, but order by time desc.
     *
     * @param rangeQuery
     */
    @Override
    public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
        return null;
    }

    /**
     * similar to rangeQuery, but order by time desc.
     *
     * @param valueRangeQuery
     */
    @Override
    public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
        return null;
    }
}
