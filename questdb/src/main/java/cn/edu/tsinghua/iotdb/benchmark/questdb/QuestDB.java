/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.questdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class QuestDB implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuestDB.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String URL_QUEST = "jdbc:postgresql://%s:%s/qdb";

    private static final String USERNAME = "admin";
    private static final String PWD = "quest";
    private static final String SSLMODE = "disable";

    private static final String CREATE_TABLE = "create table " + config.getDB_NAME();
    private static final String SELECT_SQL = "select * from " + config.getDB_NAME();
    private static final String DROP_TABLE = "DROP TABLE ";

    private Connection connection;


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
     * client thread.
     */
    @Override
    public void init() throws TsdbException {
        try {
            Properties properties = new Properties();
            properties.setProperty("user", USERNAME);
            properties.setProperty("password", PWD);
            properties.setProperty("sslmode", SSLMODE);
            connection = DriverManager.getConnection(
                    String.format(URL_QUEST, config.getHOST().get(0), config.getPORT().get(0)),
                    properties);
            connection.setAutoCommit(false);
            LOGGER.info("init success.");
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.error("Failed to init database");
            throw new TsdbException("Failed to init database, maybe there is too much connections", e);
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
            ResultSet resultSet = statement.executeQuery("SHOW TABLES");
            while(resultSet.next()){
                System.out.println(resultSet.getString(0));
            }
            statement.close();
        }catch (SQLException e){
            LOGGER.error("Failed to cleanup!");
            throw new TsdbException("Failed to cleanup!", e);
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
            }catch (SQLException e){
                LOGGER.warn("Failed to close connection");
                throw new TsdbException("Failed to close", e);
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
        if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
            // TODO check the maximum of sensor_number
            StringBuffer create = new StringBuffer(CREATE_TABLE);
            create.append("( ts TIMESTAMP, ");
            // contain
            create.append(") timestamp(ts);");

            try(Statement statement = connection.createStatement()){

            } catch (SQLException e) {
                // ignore if already has the time series
                LOGGER.error("Register TaosDB schema failed because ", e);
                throw new TsdbException(e);
            }
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
        switch (iotdbType) {
            case "BOOLEAN":
                return "BOOL";
            case "INT32":
                return "INT";
            case "INT64":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "TEXT":
                return "BINARY";
            default:
                LOGGER.error("Unsupported data type {}, use default data type: BINARY.", iotdbType);
                return "BINARY";
        }
    }
}
