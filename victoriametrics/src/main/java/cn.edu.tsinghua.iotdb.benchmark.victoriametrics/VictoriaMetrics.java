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

package cn.edu.tsinghua.iotdb.benchmark.victoriametrics;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VictoriaMetrics implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(VictoriaMetrics.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final String URL = config.getHOST().get(0) + ":" + config.getPORT().get(0);
    private static final String CREATE_URL = URL + "/write?db=" + config.getDB_NAME();
    private static final String DELETE_URL = URL + "/api/v1/admin/tsdb/delete_series?match={db=\"" + config.getDB_NAME() + "\"}";
    private static final Random sensorRandom = new Random(1 + config.getDATA_SEED());
    private static final String QUERY_URL = URL + "/api/v1/query_range?query=test.*_value&";


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
     * client thread.
     */
    @Override
    public void init() throws TsdbException {
        // no need to init
    }

    /**
     * Cleanup any state for this DB, including the old data deletion. Called once before each test if
     * IS_DELETE_DATA=true.
     */
    @Override
    public void cleanup() throws TsdbException {
        try{
            HttpRequestUtil.sendPost(DELETE_URL, null);
        }catch (Exception e){
            LOGGER.warn("Failed to cleanup!");
            throw new TsdbException("Failed to cleanup!", e);
        }
    }

    /**
     * Close the DB instance connections. Called once per DB instance.
     */
    @Override
    public void close() throws TsdbException {
        // no need to close
    }

    /**
     * Called once before each test if CREATE_SCHEMA=true.
     *
     * @param schemaList schema of devices to register
     */
    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        // no need to register
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
        try{
            LinkedList<VictoriaMetricsModel> models = createDataModelByBatch(batch);
            StringBuffer body = new StringBuffer();
            for(VictoriaMetricsModel victoriaMetricsModel: models) {
                body.append(victoriaMetricsModel.toString() + "\n");
            }
            HttpRequestUtil.sendPost(CREATE_URL, body.toString());
            return new Status(true);
        }catch (Exception e){
            e.printStackTrace();
            return new Status(false, 0, e, e.toString());
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
        try{
            LinkedList<VictoriaMetricsModel> models = createDataModelByBatch(batch);
            StringBuffer body = new StringBuffer();
            for(VictoriaMetricsModel victoriaMetricsModel: models) {
                body.append(victoriaMetricsModel.toString() + "\n");
            }
            HttpRequestUtil.sendPost(CREATE_URL, body.toString());
            return new Status(true);
        }catch (Exception e){
            e.printStackTrace();
            return new Status(false, 0, e, e.toString());
        }
    }

    private LinkedList<VictoriaMetricsModel> createDataModelByBatch(Batch batch){
        DeviceSchema deviceSchema = batch.getDeviceSchema();
        String device = deviceSchema.getDevice();
        List<Record> records = batch.getRecords();
        List<String> sensors = deviceSchema.getSensors();
        int sensorNum = sensors.size();
        LinkedList<VictoriaMetricsModel> models = new LinkedList<>();

        for(Record record : records){
            if(batch.getColIndex() != -1){
                // insert one line
                VictoriaMetricsModel model = createModel(deviceSchema.getGroup(),
                        record.getTimestamp(), record.getRecordDataValue().get(0),
                        device, deviceSchema.getSensors().get(batch.getColIndex()));
                models.addLast(model);
            }else{
                // insert align data
                for(int j = 0; j < sensorNum; j++) {
                    VictoriaMetricsModel model = createModel(deviceSchema.getGroup(),
                            record.getTimestamp(), record.getRecordDataValue().get(j),
                            device, deviceSchema.getSensors().get(j));
                    models.addLast(model);
                }
            }
        }
        return models;
    }

    private VictoriaMetricsModel createModel(String metric, long timestamp, Object value, String device, String sensor){
        VictoriaMetricsModel model = new VictoriaMetricsModel();
        model.setMetric(metric);
        model.setTimestamp(timestamp);
        model.setValue(value);
        Map<String, String> tags = new HashMap<>();
        tags.put("device", device);
        tags.put("sensor", sensor);
        model.setTags(tags);
        return model;
    }

    // https://github.com/VictoriaMetrics/VictoriaMetrics#prometheus-querying-api-usage
    /**
     * Query data of one or multiple sensors at a precise timestamp. e.g. select v1... from data where
     * time = ? and device in ?
     *
     * @param preciseQuery universal precise query condition parameters
     * @return status which contains successfully executed flag, error message and so on.
     */
    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", preciseQuery.getTimestamp() - 1);
        queryMap.put("end", preciseQuery.getTimestamp() + 1);
        list = getSubQueries(preciseQuery.getDeviceSchema(), "none");
        queryMap.put("queries", list);
        String sql = JSON.toJSONString(queryMap);
        System.out.println(sql);
        return executeQueryAndGetStatus(sql, false);
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

    private Status executeQueryAndGetStatus(String sql, boolean isLatestPoint) {
        LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
        try {
            String response;
            response = HttpRequest.sendPost(QUERY_URL, sql);
            int pointNum = getOneQueryPointNum(response, isLatestPoint);
            LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), pointNum);
            return new Status(true, pointNum);
        } catch (Exception e) {
            e.printStackTrace();
            return new Status(false, 0, e, sql);
        }
    }

    private int getOneQueryPointNum(String str, boolean isLatestPoint) {
        int pointNum = 0;
        if (!isLatestPoint) {
            JSONArray jsonArray = JSON.parseArray(str);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject json = (JSONObject) jsonArray.get(i);
                pointNum += json.getJSONObject("dps").size();
            }
        } else {
            JSONArray jsonArray = JSON.parseArray(str);
            pointNum += jsonArray.size();
        }
        return pointNum;
    }

    private List<Map<String, Object>> getSubQueries(List<DeviceSchema> devices, String aggreFunc) {
        List<Map<String, Object>> list = new ArrayList<>();

        List<String> sensorList = new ArrayList<>();
        for (String sensor : devices.get(0).getSensors()) {
            sensorList.add(sensor);
        }
        Collections.shuffle(sensorList, sensorRandom);

        Map<String, List<String>> metric2devices = new HashMap<>();
        for (DeviceSchema d : devices) {
            String m = d.getGroup();
            metric2devices.putIfAbsent(m, new ArrayList());
            metric2devices.get(m).add(d.getDevice());
        }

        for (Map.Entry<String, List<String>> queryMetric : metric2devices.entrySet()) {
            Map<String, Object> subQuery = new HashMap<>();
            subQuery.put("aggregator", aggreFunc);
            subQuery.put("metric", queryMetric.getKey());

            Map<String, String> tags = new HashMap<>();
            String deviceStr = "";
            for (String d : queryMetric.getValue()) {
                deviceStr += "|" + d;
            }
            deviceStr = deviceStr.substring(1);

            String sensorStr = sensorList.get(0);
            for (int i = 1; i < config.getQUERY_SENSOR_NUM(); i++) {
                sensorStr += "|" + sensorList.get(i);
            }
            tags.put("sensor", sensorStr);
            tags.put("device", deviceStr);
            subQuery.put("tags", tags);
            list.add(subQuery);
        }
        return list;
    }
}
