package cn.edu.tsinghua.iotdb.benchmark.tsdb.opentsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlRecorder;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenTSDB implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDB.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private String openUrl;
    private String queryUrl;
    private String writeUrl;
    private String metric = "";
    private MySqlRecorder mySql;
    private Random sensorRandom;
    private Random timestampRandom;
    private Map<String, LinkedList<TSDBDataModel>> dataMap = new HashMap<>();
    private ProbTool probTool;
    private final String DELETE_METRIC_URL = "%s?start=%s&m=sum:1ms-sum:%s";
    private int backScanTime = 24;

    /**
     * constructor.
     */
    public OpenTSDB() {
        mySql = new MySqlRecorder();
        sensorRandom = new Random(1 + config.QUERY_SEED);
        timestampRandom = new Random(2 + config.QUERY_SEED);
        probTool = new ProbTool();
        openUrl = config.DB_URL;
        writeUrl = openUrl + "/api/put?summary ";
        queryUrl = openUrl + "/api/query";
    }

    @Override
    public void init() throws TsdbException {

    }

    @Override
    public void cleanup() throws TsdbException {
        //example URL:
        //http://host:4242/api/query?start=2016/02/16-00:00:00&end=2016/02/17-23:59:59&m=avg:1ms-avg:metricname
        for(int i = 0;i < config.GROUP_NUMBER;i++){
            String metricName = metric + "group_" + i;
            String deleteMetricURL = String.format(DELETE_METRIC_URL, queryUrl, Constants.START_TIMESTAMP, metricName);
            String response;
            try {
                response = HttpRequest.sendDelete(deleteMetricURL,"");
                LOGGER.info("Delete old data of {} ...", metricName);
                LOGGER.debug("Delete request response: {}", response);
            } catch (IOException e) {
                LOGGER.error("Delete old OpenTSDB metric {} failed. Error: {}", metricName, e.getMessage());
                throw new TsdbException(e);
            }
        }
        // wait for deletion complete
        try {
            LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
            Thread.sleep(config.INIT_WAIT_TIME);
        } catch (InterruptedException e) {
            LOGGER.error("Delete old OpenTSDB metrics failed. Error: {}", e.getMessage());
            throw new TsdbException(e);
        }
    }

    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        try {

        } catch (Exception e) {
            LOGGER.error("RegisterSchema OpenTSDB failed because ", e);
            throw new TsdbException(e);
        }
    }

    @Override
    public Status insertOneBatch(Batch batch) {
        // create dataModel
        LinkedList<TSDBDataModel> models = createDataModelByBatch(batch);
        String sql = JSON.toJSONString(models);
        return executeQueryAndGetStatus(sql);
    }

    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        /*
        Map<String, Object> queryMap = setQueryMap(startTime);
        String sql = JSON.toJSONString(queryMap);
        return executeQueryAndGetStatus(sql);*/
        return null;
    }

    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        return null;
    }

    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        return null;
    }


    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        return null;
    }


    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        return null;
    }


    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        return null;
    }


    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        return null;
    }


    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        return null;
    }

    @Override
    public void close() {

    }

    private LinkedList<TSDBDataModel> createDataModelByBatch(Batch batch) {
        DeviceSchema deviceSchema = batch.getDeviceSchema();
        String device = deviceSchema.getDevice();
        List<Record> records = batch.getRecords();
        List<String> sensors = deviceSchema.getSensors();
        int sensorNum = sensors.size();
        int recordNum = records.size();
        LinkedList<TSDBDataModel> models = new LinkedList<>();
        for(int i = 0; i < recordNum; i++){
            Record record = records.get(i);
            for(int j = 0; j < sensorNum; j++){
                TSDBDataModel model = new TSDBDataModel();
                model.setMetric(deviceSchema.getGroup());
                model.setTimestamp(record.getTimestamp());
                model.setValue(record.getRecordDataValue().get(j));
                Map<String, String> tags = new HashMap<>();
                tags.put("device", device);
                tags.put("sensor", deviceSchema.getSensors().get(j));
                model.setTags(tags);
                models.addLast(model);
            }
        }
        return models;
    }


    private Status executeQueryAndGetStatus(String sql) {
        int cnt = 0;
        try {
            long startTimeStamp = 0, endTimeStamp = 0, latency = 0;
            //LOGGER.info("{} execute {} loop,提交的JSON：{}", Thread.currentThread().getName(), index, sql);
            String str;
            startTimeStamp = System.nanoTime();
            str = HttpRequest.sendPost(queryUrl, sql);
            endTimeStamp = System.nanoTime();
            latency = endTimeStamp - startTimeStamp;
            LOGGER.debug("Response: " + str);
            cnt = getOneQueryPointNum(str);
            return new Status(true, latency, cnt);
        } catch (Exception e) {
            e.printStackTrace();
            return new Status(false, 0, cnt, e, sql);
        }
    }

    private int getOneQueryPointNum(String str) {
        /*
        int pointNum = 0;
        // 非最近点查询时选择方式1，否则选择方式2
        if (config.QUERY_CHOICE != 6) {
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
        */return 0;
    }

    private Map<String, Object> setQueryMap(long startTime) {
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("msResolution", true);
        queryMap.put("start", startTime - 1);
        queryMap.put("end", startTime + config.QUERY_INTERVAL + 1);
        return queryMap;
    }
}
