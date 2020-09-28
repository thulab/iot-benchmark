package cn.edu.tsinghua.iotdb.benchmark.tsdb.opentsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenTSDB implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDB.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private String openUrl;
    private String queryUrl;
    private String writeUrl;
    private String metric = "";
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

    // no need for opentsdb
    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    }

    @Override
    public Status insertOneBatch(Batch batch) {
        // create dataModel
        LinkedList<TSDBDataModel> models = createDataModelByBatch(batch);
        String sql = JSON.toJSONString(models);
        try {
            HttpRequest.sendPost(writeUrl, sql);
            return new Status(true);
        } catch (Exception e) {
            e.printStackTrace();
            return new Status(false, 0, e, e.toString());
        }
    }

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
        return executeQueryAndGetStatus(sql, false);
    }

    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", rangeQuery.getStartTimestamp() - 1);
        queryMap.put("end", rangeQuery.getEndTimestamp() + 1);
        list = getSubQueries(rangeQuery.getDeviceSchema(), "none");
        queryMap.put("queries", list);
        String sql = JSON.toJSONString(queryMap);
        return executeQueryAndGetStatus(sql, false);
    }

    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        Exception e = new TsdbException("OpenTSDB don't support this kind of query");
        return new Status(false, 0, e, e.getMessage());
    }


    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", aggRangeQuery.getStartTimestamp() - 1);
        queryMap.put("end", aggRangeQuery.getEndTimestamp() + 1);
        list = getSubQueries(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
        queryMap.put("queries", list);
        String sql = JSON.toJSONString(queryMap);
        return executeQueryAndGetStatus(sql, false);
    }


    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        Exception e = new TsdbException("OpenTSDB don't support this kind of query");
        return new Status(false, 0, e, e.getMessage());
    }


    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        Exception e = new TsdbException("OpenTSDB don't support this kind of query");
        return new Status(false, 0, e, e.getMessage());
    }


    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", groupByQuery.getStartTimestamp() - 1);
        queryMap.put("end", groupByQuery.getEndTimestamp() + 1);
        list = getSubQueries(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
        for (Map<String, Object> subQuery : list) {
            subQuery.put("downsample", groupByQuery.getGranularity() + "ms-" + groupByQuery.getAggFun());
        }
        queryMap.put("queries", list);
        String sql = JSON.toJSONString(queryMap);
        return executeQueryAndGetStatus(sql, false);
    }


    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", latestPointQuery.getStartTimestamp() - 1);
        queryMap.put("end", latestPointQuery.getEndTimestamp() + 1);
        list = getSubQueries(latestPointQuery.getDeviceSchema(), "none");
        for (Map<String, Object> subQuery : list) {
            subQuery.put("downsample", "0all-last");
        }
        queryMap.put("queries", list);
        String sql = JSON.toJSONString(queryMap);
        return executeQueryAndGetStatus(sql, true);
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


    private Status executeQueryAndGetStatus(String sql, boolean isLatestPoint) {
        LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
        try {
            String response;
            response = HttpRequest.sendPost(queryUrl, sql);
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
            for (int i = 1; i < config.QUERY_SENSOR_NUM; i++) {
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
