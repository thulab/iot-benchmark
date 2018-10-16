package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.KairosDataModel;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KairosDB extends TSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CTSDB.class);
    private String Url;
    private String queryUrl;
    private String writeUrl;
    private String metricUrl;
    private String metric = Constants.ROOT_SERIES_NAME + ".";
    private String dataType = "double";
    private Config config;
    private MySqlLog mySql = new MySqlLog();
    private long labID;
    private float nano2million = 1000000;
    private Map<String, LinkedList<KairosDataModel>> dataMap = new HashMap<>();
    private Random sensorRandom = null;
    private static final String user = "root";
    private static final String pwd = "Root_1230!";
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);

    public KairosDB(long labID){
        mySql = new MySqlLog();
        this.labID = labID;
        config = ConfigDescriptor.getInstance().getConfig();
        sensorRandom = new Random(1 + config.QUERY_SEED);

        Url = config.DB_URL;
        queryUrl = Url + "/%s/_search";
        writeUrl = Url + "/api/v1/datapoints";
        mySql.initMysql(labID);
    }


    @Override
    public void init() throws SQLException {

    }

    @Override
    public void createSchema() throws SQLException {

    }

    @Override
    public long getLabID() {
        return labID;
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            String key = UUID.randomUUID().toString();
            dataMap.put(key, createDataModel(batchIndex, i, device));
            keys.add(key);
        }
        insertOneBatch(keys, batchIndex, totalTime, errorCount);
    }

    private LinkedList<KairosDataModel> createDataModel(int batchIndex, int dataIndex, String device) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        int deviceNum = getDeviceNum(device);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupNum = deviceNum / groupSize;
        String groupId = "group_" + groupNum;

        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP
                    + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            KairosDataModel model = new KairosDataModel();
            model.setName(sensor);
            model.setType(config.DATA_TYPE);
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String, String> tags = new HashMap<>();
            tags.put("group", groupId);
            tags.put("device", device);
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
    public void insertOneBatch(LinkedList<String> keys, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        long startTime = 0, endTime = 0;
        String response = null;
        LinkedList<KairosDataModel> models = new LinkedList<>();
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
            LOGGER.debug("response: " + response);
            LOGGER.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
                    Thread.currentThread().getName(), batchIndex, (endTime - startTime) / 1000000000.0,
                    ((totalTime.get() + (endTime - startTime)) / 1000000000.0),
                    (models.size() / (double) (endTime - startTime)) * 1000000000);
            totalTime.set(totalTime.get() + (endTime - startTime));
            mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, 0,
                    config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + models.size());
            LOGGER.error("Batch insert failed, the failed num is ,{}, Error：{}", models.size(), e.getMessage());
            mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, models.size(),
                    config.REMARK + e.getMessage());
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        return 0;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount) {

    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public long count(String group, String device, String sensor) {
        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) throws SQLException {
        return 0;
    }
}
