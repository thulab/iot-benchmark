package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.model.CTSDBMetricModel;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class CTSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CTSDB.class);
    private String Url;
    private String queryUrl;
    private String writeUrl;
    private String createMetricUrl;
    private String metric = "root.perform.";
    private String dataType = "double";
    private Config config;
    private MySqlLog mySql = new MySqlLog();;
    private long labID;
    private float nano2million = 1000000;
    private Map<String, LinkedList<TSDBDataModel>> dataMap = new HashMap<>();
    private Random sensorRandom = null;

    public CTSDB(long labID){
        mySql = new MySqlLog();
        this.labID = labID;
        config = ConfigDescriptor.getInstance().getConfig();
        sensorRandom = new Random(1 + config.QUERY_SEED);
    }

    @Override
    public void init() throws SQLException {
        Url = config.DB_URL;
        writeUrl = Url + "/api/put?summary ";
        queryUrl = Url + "/api/query";
        createMetricUrl = Url + "/PUT /_metric/";
        mySql.initMysql(labID);
    }

    @Override
    public void createSchema() throws SQLException {
        long startTime = 0, endTime = 0;
        String response = null;

        for(int i = 0;i < config.GROUP_NUMBER;i++){
            String url = createMetricUrl + metric + "group_" + i;
            CTSDBMetricModel ctsdbMetricModel = new CTSDBMetricModel();
            Map<String, String> tags = new HashMap<>();
            tags.put("device", "string");
            tags.put("sensor", "string");
            Map<String, String> fields = new HashMap<>();
            for (String sensor : config.SENSOR_CODES) {
                fields.put(sensor, dataType);
            }

            ctsdbMetricModel.setTags(tags);
            ctsdbMetricModel.setFields(fields);
            String body = JSON.toJSONString(ctsdbMetricModel);
            LOGGER.debug(body);

            try {
                startTime = System.nanoTime();
                response = HttpRequest.sendPost(url, body);
                endTime = System.nanoTime();
                float resTime = (endTime - startTime) / nano2million;
                String message = JSON.parseObject(response).getString("message");
                LOGGER.debug(response);
                if(message!=null){
                    LOGGER.info(message + " cost time: " +  resTime + " ms.");
                }else{
                    LOGGER.info("create metric" + "group_" + i + " failed !");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    @Override
    public long getLabID() {
        return 0;
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

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

    public static void main(String[] arg) throws SQLException{
        CTSDB ctsdb = new CTSDB(314);
        ctsdb.createSchema();
    }

}
