package cn.edu.tsinghua.iotdb.benchmark.questdb;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

/**
 * @Author stormbroken
 * Create by 2021/07/26
 * @Version 1.0
 **/

public class QuestDB implements IDatabase {
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
     * client thread.
     */
    @Override
    public void init() throws TsdbException {

    }

    /**
     * Cleanup any state for this DB, including the old data deletion. Called once before each test if
     * IS_DELETE_DATA=true.
     */
    @Override
    public void cleanup() throws TsdbException {

    }

    /**
     * Close the DB instance connections. Called once per DB instance.
     */
    @Override
    public void close() throws TsdbException {

    }

    /**
     * Called once before each test if CREATE_SCHEMA=true.
     *
     * @param schemaList schema of devices to register
     */
    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {

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
        return null;
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

    /**
     * map the given type string name to the name in the target DB
     *
     * @param iotdbType : "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"
     * @return
     */
    @Override
    public String typeMap(String iotdbType) {
        return null;
    }
}
