package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/11/16 0016.
 */
public class InfluxDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);

    private String InfluxURL;
    private String InfluxDBName;
    private String queryURL;
    private String writeURL;
    private Config config;

    private static class SQLTemplates{
        public static final String CREATE_DATABASE = "CREATE DATABASE %s";
    }

    @Override
    public void init() throws SQLException {
        config = ConfigDescriptor.getInstance().getConfig();
        InfluxURL = config.INFLUX_URL;
        InfluxDBName = config.INFLUX_DB_NAME;
        queryURL = InfluxURL + "/query";
        writeURL = InfluxURL + "/write";
        createDatabase(InfluxDBName);
    }

    @Override
    public void createSchema() throws SQLException {
        // no need for InfluxDB
    }

    /**
     *  Create a batch of data specified by "device" and "batchIndex", and send them to InfluxDB's REST API.
     *  E.g. send "cpu,host=server02,region=uswest load=78 1434055562000000000" as post body to
     *   "http://localhost:8086/write?db=mydb".
     * @param device   a string like "d_100", which will be used in a tag "device=d_100", and its measurement will be "group_x",
     *               while x = 100 / groupSize
     * @param batchIndex  a test mission is divided to multiple batches, this indicates which batch is being processed
     * @param totalTime  to record the total time of a test mission
     * @throws SQLException
     */
    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        long exeTime = 0;
        long startTime, endTime;
        LinkedList<String> dataStrs = new LinkedList<>();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            InfluxDataModel model = createDataModel(batchIndex, i, device);
            dataStrs.add(model.toString());
        }
        insertOneBatch(dataStrs, batchIndex, totalTime, errorCount);
    }

    /**
     * Send given data "cons" to InfluxDB's REST API.
     *  E.g. send "cpu,host=server02,region=uswest load=78 1434055562000000000" as post body to
     *   "http://localhost:8086/write?db=mydb".
     * @param cons  each of its element represent a data record
     * @param batchIndex  a test mission is divided to multiple batches, this indicates which batch is being processed
     * @param totalTime  to record the total time of a test mission
     * @throws SQLException
     */
    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        StringBuilder body = new StringBuilder();
        for(String dataRecord : cons) {
            body.append(dataRecord).append("\n");
        }
        StringBuilder url = new StringBuilder(writeURL);
        url.append("?db=").append(InfluxDBName);
        try {
            long startTime = System.currentTimeMillis();
            String response = HttpRequest.sendPost(url.toString(), body.toString());
            long endTime = System.currentTimeMillis();
            LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
                    Thread.currentThread().getName(),
                    batchIndex,
                    (endTime-startTime)/1000.0,
                    ((totalTime.get()+(endTime-startTime))/1000.0),
                    (cons.size() / (double) (endTime-startTime))*1000);
            totalTime.set(totalTime.get()+(endTime-startTime));
            LOGGER.info(response);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + cons.size());
            LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
                    cons.size(), e.getMessage());
           throw  new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        // no need for InfluxDB
    }

    /**
     * E.g. a post to "http://localhost:8086/query?q=CREATE DATABASE mydb".
     * @param databaseName
     * @throws SQLException
     */
    public void createDatabase(String databaseName) throws SQLException {
        StringBuilder url = new StringBuilder(queryURL);
        url.append("?q=");
        try {
            // MUST use URLEncoder if spaces occur
            url.append(URLEncoder.encode(String.format(SQLTemplates.CREATE_DATABASE, databaseName), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            // impossible, utf-8 must be supported
        }
        String response;
        try {
            response = HttpRequest.sendPost(url.toString(), null);
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
        LOGGER.info(response);
    }

    /**
     * Create a data record, e.g. cpu,host=server02,region=uswest load=78 1434055562000000000.
     * The measurement is the device. No tags are used currently. The timestamp is calculated from batchIndex and dataIndex.
     * Every sensor will be regarded as a field.
     * @param batchIndex a test mission is divided to multiple batches, this indicates which batch is being processed
     * @param dataIndex the record's position within this batch
     * @param device a string like "d_100", which will be used in a tag "device=d_100", and its measurement will be "group_x",
     *               while x = 100 / groupSize
     * @return a structure represents the data record
     */
    private InfluxDataModel createDataModel(int batchIndex, int dataIndex, String device) {
        InfluxDataModel model = new InfluxDataModel();
        int deviceNum = getDeviceNum(device);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupNum = deviceNum / groupSize;
        model.measurement = "group_" + groupNum;
        model.tagSet.put("device", device);
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
        model.timestamp = currentTime;
        for(String sensor: config.SENSOR_CODES){
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            model.fields.put(sensor, value);
        }
        return  model;
    }

    private int getDeviceNum(String device) {
        String[] parts = device.split("_");
        try {
            return Integer.parseInt(parts[1]);
        } catch (Exception e) {
            LOGGER.error("{} {}",device, e.getMessage());
            throw e;
        }
    }

    static public void main(String[] args) throws SQLException {
        InfluxDB influxDB = new InfluxDB();
        influxDB.init();
        ThreadLocal<Long> time = new ThreadLocal<>();
        time.set((long) 0);
        ThreadLocal<Long> errorCount = new ThreadLocal<>();
        errorCount.set((long) 0);
        influxDB.insertOneBatch("D_0", 0, time, errorCount);
    }

	@Override
	public long getTotalTimeInterval() throws SQLException {

		return 0;
	}

	@Override
	public void executeOneQuery(List<Integer> devices, int index,
			long startTime, QueryClientThread client,
			ThreadLocal<Long> errorCount) {
		
	}

	@Override
    public void flush(){

    }

    @Override
    public void getUnitPointStorageSize() throws SQLException {

    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount){

    }

    @Override
    public long count(String group, String device,String sensor){

        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String s, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public int exeSQLFromFileByOneBatch() {
        return 0;
    }
}
