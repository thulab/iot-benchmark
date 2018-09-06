package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.CTSDBMetricModel;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class CTSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CTSDB.class);
    private String Url;
    private String queryUrl;
    private String writeUrl;
    private String metricUrl;
    private String metric = "root.perform.";
    private String dataType = "double";
    private Config config;
    private MySqlLog mySql = new MySqlLog();
    private long labID;
    private float nano2million = 1000000;
    private Map<String, LinkedList<TSDBDataModel>> dataMap = new HashMap<>();
    private Random sensorRandom = null;
    private static final String user = "root";
    private static final String pwd = "Root_1230!";
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);

    public CTSDB(long labID){
        mySql = new MySqlLog();
        this.labID = labID;
        config = ConfigDescriptor.getInstance().getConfig();
        sensorRandom = new Random(1 + config.QUERY_SEED);
        Authenticator.setDefault(new MyAuthenticator());
    }

    static class MyAuthenticator extends Authenticator{
        public PasswordAuthentication getPasswordAuthentication() {
            System.err.println("Feeding username and password for " + getRequestingScheme());
            return (new PasswordAuthentication(user, pwd.toCharArray()));
        }
    }

    @Override
    public void init() throws SQLException {
        Url = config.DB_URL;
        queryUrl = Url + "/%s/_search";
        metricUrl = Url + "/_metric/";
        mySql.initMysql(labID);
    }

    @Override
    public void createSchema() throws SQLException {
        long startTime = 0, endTime = 0;
        String response = null;

        //delete old metric
        for(int i = 0;i < config.GROUP_NUMBER;i++){
            String url = metricUrl + metric + "group_" + i;
            try {
                response = HttpRequest.sendDelete(url,"");
                String message = JSON.parseObject(response).getString("message");
                LOGGER.debug(response);
                LOGGER.info(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //create new metric
        for(int i = 0;i < config.GROUP_NUMBER;i++){
            String url = metricUrl + metric + "group_" + i;
            CTSDBMetricModel ctsdbMetricModel = new CTSDBMetricModel();
            Map<String, String> tags = new HashMap<>();
            tags.put("device", "string");
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
        return this.labID;
    }

    private String getMetricName(String device) {
        String[] parts = device.split("_");
        int deviceNum = Integer.parseInt(parts[1]);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupNum = deviceNum / groupSize;
        String groupId = "group_" + groupNum;
        return metric + groupId;
    }

    private String getMetaJSON(String device) {
        return "{\"index\":{\"_routing\":\"" + device + "\"}}";
    }

    /*
    example:
    {"index":{"_routing": "sh" }}
    {"region":"sh","cpuUsage":2.5,"timestamp":1505294654}
    {"index":{"_routing": "sh" }}
    {"region":"sh","cpuUsage":2.0,"timestamp":1505294654}
     */
    private String getDataJSON(String device, int batchIndex, int dataIndex){
        StringBuilder sb = new StringBuilder();
        sb.append("{\"device\":\"").append(device).append("\",");
        long currentTime = Constants.START_TIMESTAMP
                + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
        for (String sensor : config.SENSOR_CODES) {
            sb.append("\"").append(sensor).append("\":");
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            //DecimalFormat df = new DecimalFormat(".0000");
            //sb.append(df.format(value)).append(",");
            sb.append(value).append(",");
        }
        sb.append("\"timestamp\":").append(currentTime).append("}");
        return sb.toString();
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        StringBuilder body = new StringBuilder();
        long startTime = 0, endTime = 0;
        String response;
        for (int i = 0; i < config.CACHE_NUM; i++) {
            body.append(getMetaJSON(device)).append("\n");
            body.append(getDataJSON(device, batchIndex, i)).append("\n");
        }
        LOGGER.debug(body.toString());
        writeUrl = Url + "/" + getMetricName(device) + "/doc/_bulk";
        int batch_point_num = config.CACHE_NUM * config.SENSOR_NUMBER;
        long costTime = 0;
        try {
            startTime = System.nanoTime();
            response = HttpRequest.sendPost(writeUrl, body.toString());
            endTime = System.nanoTime();
            boolean isError = JSON.parseObject(response).getBoolean("errors");
            if(isError){
                errorCount.set(errorCount.get() + batch_point_num);
            }else{
                errorCount.set(errorCount.get());
            }
            costTime = endTime - startTime;
            LOGGER.debug(response);
            LOGGER.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
                    Thread.currentThread().getName(), batchIndex, costTime / 1000000000.0,
                    ((totalTime.get() + costTime) / 1000000000.0),
                    (batch_point_num / (double) costTime) * 1000000000);
            totalTime.set(totalTime.get() + costTime);
            mySql.saveInsertProcess(batchIndex, costTime / 1000000000.0, totalTime.get() / 1000000000.0, batch_point_num,
                    config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + batch_point_num);
            LOGGER.error("Batch insert failed, the failed num is ,{}, Error：{}", batch_point_num, e.getMessage());
            mySql.saveInsertProcess(batchIndex, costTime / 1000000000.0, totalTime.get() / 1000000000.0, batch_point_num,
                    config.REMARK + e.getMessage());
            throw new SQLException(e.getMessage());
        }
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

//    private String getQueryJSON(List<Integer> devices, long startTime, long endTime){
//        String sTime = sdf.format(new Date(startTime));
//        String eTime = sdf.format(new Date(endTime));
//
//        List<String> sensorList = new ArrayList<>(config.SENSOR_CODES);
//        Collections.shuffle(sensorList, sensorRandom);
//        StringBuilder queryJSONBuilder = new StringBuilder();
//        queryJSONBuilder.append(
//                "{" +
//                "\"query\":{" +
//                "\"bool\":{" +
//                "\"filter\":[" +
//                "{" +
//                "\"range\":{" +
//                "\"timestamp\":{" +
//                "\"format\":\"yyyy-MM-dd HH:mm:ss\"," +
//                "\"gt\":\"" + sTime + "\"," +
//                "\"lt\":\"" + eTime + "\"," +
//                "\"time_zone\":\"+08:00\"" +
//                "}" +
//                "}" +
//                "}," +
//                "{" +
//                "\"terms\":{" +
//                "\"device\":["
//        );
//        for(int d : devices){
//            queryJSONBuilder.append("\"").append(config.DEVICE_CODES.get(d)).append("\",");
//        }
//        queryJSONBuilder.deleteCharAt(queryJSONBuilder.lastIndexOf(","));
//        queryJSONBuilder.append(
//                "]" +
//                "}" +
//                "}" +
//                "]" +
//                "}" +
//                "}," +
//                "\"docvalue_fields\":[");
//        for(int i = 0;i < config.QUERY_SENSOR_NUM;i++){
//            queryJSONBuilder.append("\"").append(sensorList.get(i)).append("\",");
//        }
//        queryJSONBuilder.append("\"timestamp\"")
//                        .append("]")
//                        .append("}");
//
//        return queryJSONBuilder.toString();
//    }

    private String getQueryJSON(List<Integer> devices, long startTime, long endTime){
        String sTime = sdf.format(new Date(startTime));
        String eTime = sdf.format(new Date(endTime));
        List<String> sensorList = new ArrayList<>(config.SENSOR_CODES);
        Collections.shuffle(sensorList, sensorRandom);

        Map<String, Object> queryMap = new HashMap<>();
        Map<String, Object> subQueryMap = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        List<Map<String, Object>> filterList = new ArrayList<>();
        Map<String, Object> rangeMap = new HashMap<>();
        Map<String, Object> supRangeMap = new HashMap<>();
        Map<String, String> timestampMap = new HashMap<>();
        Map<String, Object> termsMap = new HashMap<>();
        Map<String, Object> supTermsMap = new HashMap<>();
        List<String> deviceList = new ArrayList<>();
        List<String> docValueFieldsList = new ArrayList<>();

        timestampMap.put("format", TIME_FORMAT);
        timestampMap.put("gt", sTime);
        timestampMap.put("lt", eTime);
        timestampMap.put("time_zone", "+08:00");
        rangeMap.put("timestamp", timestampMap);
        supRangeMap.put("range", rangeMap);
        filterList.add(supRangeMap);

        for(int d : devices){
            deviceList.add(config.DEVICE_CODES.get(d));
        }
        termsMap.put("device", deviceList);
        supTermsMap.put("terms", termsMap);
        filterList.add(supTermsMap);


        boolMap.put("filter", filterList);
        subQueryMap.put("bool", boolMap);
        queryMap.put("query", subQueryMap);

        for(int i = 0;i < config.QUERY_SENSOR_NUM;i++){
            docValueFieldsList.add(sensorList.get(i));
        }
        docValueFieldsList.add("timestamp");
        queryMap.put("docvalue_fields", docValueFieldsList);

        return JSON.toJSONString(queryMap);
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount) {
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0;
        String metricName = getMetricName("d_" + devices.get(0));
        String url = String.format(queryUrl, metricName);

        try {
            List<String> sensorList = new ArrayList<String>();
            switch (config.QUERY_CHOICE) {
                case 1:// 精确点查询
                    sql = getQueryJSON(devices, startTime - 1000, startTime + 1000);
                    break;
                case 2:// 模糊点查询（暂未实现）
                    break;
                case 3:// 聚合函数查询

                    break;
                case 4:// 范围查询
                    sql = getQueryJSON(devices, startTime, startTime + config.QUERY_INTERVAL);
                    break;
                case 5:// 条件查询

                    break;
                case 6:// 最近点查询

                    break;
                case 7:// groupBy查询（暂时只有一个时间段）

                    break;
            }
            LOGGER.debug("url: \n"+url);
            LOGGER.debug("sql JSON: \n"+sql);
            //sql = sql.replaceAll("[\r\n\t]","");
            startTimeStamp = System.nanoTime();
            String str = HttpRequest.sendPost(url, sql);
            endTimeStamp = System.nanoTime();

            LOGGER.debug("Response: " + str);

            //int pointNum = getOneQueryPointNum(str);
            int pointNum = 0;
            client.setTotalPoint(client.getTotalPoint() + pointNum);
            client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);
            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, (endTimeStamp - startTimeStamp) / 1000000000.0, pointNum,
                    pointNum * 1000000000.0 / (endTimeStamp - startTimeStamp), (client.getTotalTime()) / 1000000000.0,
                    client.getTotalPoint(), client.getTotalPoint() * 1000000000.0f / client.getTotalTime());
            mySql.saveQueryProcess(index, pointNum, (endTimeStamp - startTimeStamp) / 1000000000.0f, config.REMARK);
        } catch (Exception e) {
            errorCount.set(errorCount.get() + 1);
            LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
            LOGGER.error("执行失败的查询语句：{}", sql);
            mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000000000.0f, "query fail!" + sql);
            e.printStackTrace();
        }
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
