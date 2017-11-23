package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;

import org.influxdb.dto.BatchPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * This version use influx-java api instead of simple http.
 */
public class InfluxDBV2 implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBV2.class);

    private String InfluxURL;
    private String InfluxDBName;
    private Config config;
    private final String DEFAULT_RP = "autogen";
    private org.influxdb.InfluxDB influxDB;

    @Override
    public void init() throws SQLException {
        config = ConfigDescriptor.getInstance().getConfig();
        InfluxURL = config.INFLUX_URL;
        InfluxDBName = config.INFLUX_DB_NAME;
        influxDB = org.influxdb.InfluxDBFactory.connect(InfluxURL);
        if(influxDB.databaseExists(InfluxDBName)){
            influxDB.deleteDatabase(InfluxDBName);
        }
        createDatabase(InfluxDBName);
    }

    @Override
    public void createSchema() throws SQLException {
        // no need for InfluxDB
    }

    /**
     *  Create a batch of data specified by "device" and "batchIndex", and send them to InfluxDB's Java API.
     * @param device   a string like "d_100", which will be used in a tag "device=d_100", and its measurement will be "group_x",
     *               while x = 100 / groupSize
     * @param batchIndex  a test mission is divided to multiple batches, this indicates which batch is being processed
     * @param totalTime  to record the total time of a test mission
     * @throws SQLException
     */
    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        LinkedList<String> dataStrs = new LinkedList<>();
        BatchPoints batchPoints = BatchPoints
                                        .database(InfluxDBName)
                                        .tag("async", "true")
                                        .retentionPolicy(DEFAULT_RP)
                                        .consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL)
                                        .build();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            InfluxDataModel model = createDataModel(batchIndex, i, device);
            batchPoints.point(model.toInfluxPoint());
        }
        try {
            long startTime = System.currentTimeMillis();
            influxDB.write(batchPoints);
            long endTime = System.currentTimeMillis();
            LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
                    Thread.currentThread().getName(),
                    batchIndex,
                    (endTime-startTime)/1000.0,
                    ((totalTime.get()+(endTime-startTime))/1000.0),
                    (batchPoints.getPoints().size() / (double) (endTime-startTime))*1000);
            totalTime.set(totalTime.get()+(endTime-startTime));
        } catch (Exception e) {
            // TODO : get accurate insert number
            errorCount.set(errorCount.get() + batchPoints.getPoints().size());
            LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
                    batchPoints.getPoints().size(), e.getMessage());
            throw  new SQLException(e.getMessage());
        }
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
        try {
            long startTime = System.currentTimeMillis();
            influxDB.write(cons);
            long endTime = System.currentTimeMillis();
            LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
                    Thread.currentThread().getName(),
                    batchIndex,
                    (endTime-startTime)/1000.0,
                    ((totalTime.get()+(endTime-startTime))/1000.0),
                    (cons.size() / (double) (endTime-startTime))*1000);
            totalTime.set(totalTime.get()+(endTime-startTime));
        } catch (Exception e) {
            // TODO : get accurate insert number
            errorCount.set(errorCount.get() + cons.size());
            LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
                    cons.size(), e.getMessage());
            throw  new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        influxDB.close();
    }

    /**
     * @param databaseName
     * @throws SQLException
     */
    public void createDatabase(String databaseName) throws SQLException {
        influxDB.createDatabase(InfluxDBName);
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void executeOneQuery(List<Integer> devices, int index,
			long startTime, QueryClientThread client,
			ThreadLocal<Long> errorCount) {
		// TODO Auto-generated method stub
		
	}

	@Override
    public void flush(){

    }

}
