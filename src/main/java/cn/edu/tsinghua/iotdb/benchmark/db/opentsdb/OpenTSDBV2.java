package cn.edu.tsinghua.iotdb.benchmark.db.opentsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.TSDB;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class use the schema that regard sensor as metric and regard device and storage group as tag, which may
 * be not suitable for OpenTSDB according to the official document:
 * http://opentsdb.net/docs/build/html/user_guide/writing/index.html
 *
 * but for comparison this class is reserved.
 *
 * @author liurui
 */
public class OpenTSDBV2 extends TSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDBV2.class);
    private String openUrl;
    private String queryUrl;
    private String writeUrl;
    private String metric = "root.perform.";
    private Config config;
    private MySqlLog mySql;
    private long labID;
    private Map<String, LinkedList<TSDBDataModel>> dataMap = new HashMap<>();
    private Random sensorRandom = null;
    private Random timestampRandom;
    private ProbTool probTool;
    private int backScanTime = 24;

    public OpenTSDBV2(long labID) {
        mySql = new MySqlLog();
        this.labID = labID;
        config = ConfigDescriptor.getInstance().getConfig();
        sensorRandom = new Random(1 + config.QUERY_SEED);
        timestampRandom = new Random(2 + config.QUERY_SEED);
        probTool = new ProbTool();
        openUrl = config.DB_URL;
        writeUrl = openUrl + "/api/put?summary ";
        queryUrl = openUrl + "/api/query";
        mySql.initMysql(labID);
    }

    @Override
    public void init() {
        //OpenTSDB do not support delete old data very well
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
            throws SQLException {
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            String key = UUID.randomUUID().toString();
            dataMap.put(key, createDataModel(batchIndex, i, device));
            keys.add(key);
        }
        insertOneBatch(keys, batchIndex, totalTime, errorCount, latencies);
    }

    @Override
    public void insertOneBatch(LinkedList<String> keys, int batchIndex, ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        long startTime = 0, endTime = 0, latency = 0;
        String response = null;
        LinkedList<TSDBDataModel> models = new LinkedList<>();
        for (String key : keys) {
            models.addAll(dataMap.get(key));
            dataMap.remove(key);
        }
        String body = JSON.toJSONString(models);
        LOGGER.debug(body);
        try {
            startTime = System.nanoTime();
            response = HttpRequest.sendPost(writeUrl, body);
            endTime = System.nanoTime();
            latency = endTime - startTime;
            latencies.add(latency);
            LOGGER.debug("response: " + response);
            int errorNum = JSON.parseObject(response).getInteger("failed");
            errorCount.set(errorCount.get() + errorNum);
            LOGGER.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
                    Thread.currentThread().getName(), batchIndex, latency / 1000000000.0,
                    ((totalTime.get() + latency) / 1000000000.0),
                    ((models.size() - errorNum) / (double) latency) * 1000000000);
            totalTime.set(totalTime.get() + latency);
            mySql.saveInsertProcess(batchIndex, latency / 1000000000.0, totalTime.get() / 1000000000.0, errorNum,
                    config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + models.size());
            LOGGER.error("Batch insert failed, the failed num is ,{}, Error：{}", models.size(), e.getMessage());
            mySql.saveInsertProcess(batchIndex, latency / 1000000000.0, totalTime.get() / 1000000000.0, models.size(),
                    config.REMARK + e.getMessage());
            throw new SQLException(e.getMessage());
        }
    }

    private String getGroup(String device) {
        int deviceNum = getDeviceNum(device);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupNum = deviceNum / groupSize;
        return "group_" + groupNum;
    }

    private LinkedList<TSDBDataModel> createDataModel(int batchIndex, int dataIndex, String device) {
        LinkedList<TSDBDataModel> models = new LinkedList<TSDBDataModel>();
        String groupId = getGroup(device);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP
                    + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
            if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
                currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
            }
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            TSDBDataModel model = new TSDBDataModel();
            model.setMetric(sensor);
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String, String> tags = new HashMap<>();
            tags.put("device", device);
            tags.put("group", groupId);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }

    private LinkedList<TSDBDataModel> createDataModel(int timestampIndex, String device) {
        LinkedList<TSDBDataModel> models = new LinkedList<TSDBDataModel>();
        String groupId = getGroup(device);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
            if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
                currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
            }
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            TSDBDataModel model = new TSDBDataModel();
            model.setMetric(sensor);
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String, String> tags = new HashMap<>();
            tags.put("device", device);
            tags.put("group", groupId);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }

    private int getDeviceNum(String device) {
        String[] parts = device.split("_");
        try {
            return Integer.parseInt(parts[1]);
        } catch (Exception e) {
            LOGGER.error("{} {}", device, e.getMessage());
            throw e;
        }
    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        long endTime = 250000000 + Constants.START_TIMESTAMP;
        Map<String, Object> queryMap = new HashMap<>();

        Map<String, Object> subQuery = new HashMap<String, Object>();
        subQuery.put("metric", "s_0");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("device", "d_0");
        tags.put("group", "group_0");
        subQuery.put("tags", tags);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(subQuery);

        queryMap.put("queries", list);
        queryMap.put("backScan", backScanTime);

        LOGGER.debug(JSON.toJSONString(queryMap));
        try {
            String str = HttpRequest.sendPost(queryUrl + "/last", JSON.toJSONString(queryMap));
            LOGGER.debug(str);
            JSONArray jsonArray = new JSONArray(str);
            if (jsonArray.length() > 0) {
                endTime = ((JSONObject) jsonArray.get(0)).getLong("timestamp");
                LOGGER.debug("endTime = {}", endTime);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return endTime - Constants.START_TIMESTAMP;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client,
                                ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0, latency = 0;
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        queryMap.put("msResolution", true);
        queryMap.put("start", startTime);
        queryMap.put("end", startTime + config.QUERY_INTERVAL);

        try {
            switch (config.QUERY_CHOICE) {
                case 1:// 精确点查询
                    long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
                            + Constants.START_TIMESTAMP;
                    if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
                        timeStamp += config.POINT_STEP / 2;
                    }
                    queryMap.put("start", timeStamp);
                    queryMap.put("end", timeStamp);
                    list = getSubQueries(devices);
                    queryMap.put("queries", list);
                    break;
                case 2:// 模糊点查询（暂未实现）
                    break;
                case 3:// 聚合函数查询
                    list = getSubQueries(devices);
                    for (Map<String, Object> subQuery : list) {
                        subQuery.put("downsample", (config.QUERY_INTERVAL + 1) + "ms-" + config.QUERY_AGGREGATE_FUN);
                    }
                    queryMap.put("queries", list);
                    break;
                case 4:// 范围查询
                    list = getSubQueries(devices);
                    queryMap.put("queries", list);
                    break;
                case 5:// 条件查询

                    break;
                case 6:// 最近点查询
                    //FIXME
                    list = getSubQueries(devices);
                    for (Map<String, Object> subQuery : list) {
                        subQuery.remove("end");
                    }
                    for (Map<String, Object> subQuery : list) {
                        subQuery.remove("aggregator");
                    }
                    for (Map<String, Object> subQuery : list) {
                        subQuery.put("aggregator", "none");
                    }
                    for (Map<String, Object> subQuery : list) {
                        subQuery.put("downsample", "0all-last");
                    }
                    queryMap.put("queries", list);
                    break;
                case 7:// groupBy查询（暂时只有一个时间段）
                    list = getSubQueries(devices);
                    for (Map<String, Object> subQuery : list) {
                        subQuery.put("downsample", config.TIME_UNIT + "ms-" + config.QUERY_AGGREGATE_FUN);
                    }
                    queryMap.put("queries", list);
                    break;
                case 8:// query with limit and series limit and their offsets
                    //not support yet
                    break;
                case 9:// range query with limit

                    break;
                case 10:// aggregation function query without any filter
                    queryMap.put("start", Constants.START_TIMESTAMP);
                    queryMap.remove("end");
                    list = getSubQueries(devices);
                    for (Map<String, Object> subQuery : list) {
                        subQuery.put("downsample", config.TIME_UNIT + "ms-" + config.QUERY_AGGREGATE_FUN);
                    }
                    queryMap.put("queries", list);
                    break;
                case 11:// aggregation function query with value filter
                    //not support yet
                    break;
            }
            sql = JSON.toJSONString(queryMap);
            LOGGER.info("{} execute {} loop,提交的JSON：{}", Thread.currentThread().getName(), index, sql);
            String str ;
            startTimeStamp = System.nanoTime();
            str = HttpRequest.sendPost(queryUrl, sql);
            endTimeStamp = System.nanoTime();
            latency = endTimeStamp - startTimeStamp;
            latencies.add(latency);
//			else {
//				startTimeStamp = System.nanoTime();
////				str = HttpRequest.sendPost(queryUrl+"/last", sql);
//				str = HttpRequest.sendPost(queryUrl, sql);
//				endTimeStamp = System.nanoTime();
//			}
            LOGGER.debug("Response: " + str);

            int pointNum = getOneQueryPointNum(str);
            client.setTotalPoint(client.getTotalPoint() + pointNum);
            client.setTotalTime(client.getTotalTime() + latency);
            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, latency / 1000000000.0, pointNum,
                    pointNum * 1000000000.0 / latency, (client.getTotalTime()) / 1000000000.0,
                    client.getTotalPoint(), client.getTotalPoint() * 1000000000.0f / client.getTotalTime());
            mySql.saveQueryProcess(index, pointNum, latency / 1000000000.0f, config.REMARK);
        } catch (Exception e) {
            queryErrorProcess(index, errorCount, sql, startTimeStamp, endTimeStamp, e, LOGGER, mySql);
        }
    }

    private int getOneQueryPointNum(String str) {
        int pointNum = 0;
        if (config.QUERY_CHOICE != 6) {
            JSONArray jsonArray = new JSONArray(str);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject json = (JSONObject) jsonArray.get(i);
                pointNum += json.getJSONObject("dps").length();
            }
        } else {
            JSONArray jsonArray = new JSONArray(str);
            pointNum += jsonArray.length();
        }
        return pointNum;
    }

    private List<Map<String, Object>> getSubQueries(List<Integer> devices) {
        List<Map<String, Object>> list = new ArrayList<>();

        List<String> sensorList = new ArrayList<String>();
        for (String sensor : config.SENSOR_CODES) {
            sensorList.add(sensor);
        }
        Collections.shuffle(sensorList, sensorRandom);

        for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
            String metric = sensorList.get(i);
            Map<String, Object> subQuery = new HashMap<String, Object>();
            subQuery.put("aggregator", config.QUERY_AGGREGATE_FUN);// FIXME 值的意义需要再研究一下
            subQuery.put("metric", metric);

            List<String> deviceList = new ArrayList<>();
            List<String> groupList = new ArrayList<>();
            for (int d : devices) {
                deviceList.add("d_" + d);
            }
            for (String d : deviceList) {
                groupList.add(getGroup(d));
            }
            List<String> uniqueGroupList = new ArrayList<>(new TreeSet<>(groupList));

            Map<String, String> tags = new HashMap<String, String>();
            String deviceStr = "";
            for (String d : deviceList) {
                deviceStr += "|" + d;
            }
            deviceStr = deviceStr.substring(1);

            String groupStr = "";
            for (String g : uniqueGroupList) {
                groupStr += "|" + g;
            }
            groupStr = groupStr.substring(1);
            tags.put("group", groupStr);
            tags.put("device", deviceStr);
            subQuery.put("tags", tags);
            list.add(subQuery);
        }
        return list;
    }

    @Override
    public void createSchema() throws SQLException {
        // no need for opentsdb

    }

    @Override
    public long getLabID() {
        return this.labID;
    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        int timestampIndex;
        PossionDistribution possionDistribution = new PossionDistribution(random);
        int nextDelta;
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, random)) {
                nextDelta = possionDistribution.getNextPossionDelta();
                timestampIndex = maxTimestampIndex - nextDelta;
            } else {
                maxTimestampIndex++;
                timestampIndex = maxTimestampIndex;
            }
            String key = UUID.randomUUID().toString();
            dataMap.put(key, createDataModel(timestampIndex, device));
            keys.add(key);
        }
        insertOneBatch(keys, loopIndex, totalTime, errorCount, latencies);
        return maxTimestampIndex;
    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime,
                                        ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public long count(String group, String device, String sensor) {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
            throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

}
