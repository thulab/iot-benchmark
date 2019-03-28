package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdbengine;


import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
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
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.iotdb.db.api.ITSEngine;
import org.apache.iotdb.db.api.IoTDBEngineException;
import org.apache.iotdb.db.api.IoTDBOptions;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBEngine implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBEngine.class);

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static IoTDBOptions options = new IoTDBOptions();
  private static volatile int reference = 0;
  private static ITSEngine engine;

  public IoTDBEngine() {

  }

  @Override
  public void init() throws TsdbException {
    synchronized (IoTDBEngine.class) {
      if (engine == null) {
        File file = new File(config.GEN_DATA_FILE_PATH);
        // wal
        options.setWalPath("/data2/iotdb");
        // set flush time interval or  period: 3 minute
        options.setPeriodTimeForFlush(60 * 3);
        // set merge time interval or period: 5 minute
        options.setPeriodTimeForMerge(60 * 5);
        // the minimum threshold for triggering merge: 20MB
        options.setOverflowFileSizeThreshold(50 << 20);
        LOGGER.info("Create the IoTDB engine, {}", options);
        engine = new org.apache.iotdb.db.api.impl.IoTDBEngine(file, options);
        try {
          engine.openOrCreate();
        } catch (IoTDBEngineException e) {
          throw new TsdbException(e);
        }

      }
      reference++;
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // No need to implement
  }

  @Override
  public void close() throws TsdbException {
    synchronized (IoTDBEngine.class) {
      if (reference == 1) {
        try {
          engine.close();
          engine = null;
        } catch (IOException e) {
          throw new TsdbException(e);
        }
      }
      reference--;
    }
  }

  @Override
  public void registerSchema(Measurement measurement) throws TsdbException {
    DataSchema dataSchema = DataSchema.getInstance();
    // storage group
    for (int i = 0; i < config.GROUP_NUMBER; i++) {
      String sg = Constants.ROOT_SERIES_NAME + "." + DeviceSchema.GROUP_NAME_PREFIX + i;
      try {
        setStorgeGroup(sg);
      } catch (IOException e) {
        throw new TsdbException(e);
      }
    }
    // time series
    for (Entry<Integer, List<DeviceSchema>> entry : dataSchema.getClientBindSchema().entrySet()) {
      List<DeviceSchema> deviceSchemaList = entry.getValue();
      for (DeviceSchema deviceSchema : deviceSchemaList) {
        String path = Constants.ROOT_SERIES_NAME
            + "." + deviceSchema.getGroup()
            + "." + deviceSchema.getDevice();
        for (String sensor : deviceSchema.getSensors()) {
          try {
            createTimeseriesBatch(path, sensor);
          } catch (IOException e) {
            throw new TsdbException(e);
          }
        }
      }
    }
  }

  private void createTimeseriesBatch(String path, String sensor) throws IOException {
    String timeseries = path + "." + sensor;
    System.out.println("time series: " + timeseries);
    engine.addTimeSeries(timeseries, config.DATA_TYPE.toString(), config.ENCODING.toString(),
        new String[0]);
  }

  private void setStorgeGroup(String g) throws IOException {
    System.out.println("storage group path: " + g);
    engine.setStorageGroup(g);
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    // insert one batch
    String devicePath = batch.getDeviceSchema().getDevicePath();
    List<String> sensors = batch.getDeviceSchema().getSensors();
    long cost = 0;
    for (Record record : batch.getRecords()) {
      try {
        long st = System.nanoTime();
        engine.write(devicePath, record.getTimestamp(), sensors, record.getRecordDataValue());
        long et = System.nanoTime();
        cost += (et - st);
      } catch (IOException e) {
        // No need to throw an exception
        e.printStackTrace();
        return new Status(false, cost, e, e.getMessage());
      }
    }
    return new Status(true, cost);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    // query multi device and sensor
    long startTime = preciseQuery.getTimestamp();
    long endTime = preciseQuery.getTimestamp();
    long cost = 0;
    int line = 0;
    for (DeviceSchema deviceSchema : preciseQuery.getDeviceSchema()) {
      for (String sensor : deviceSchema.getSensors()) {
        String timeseres = deviceSchema.getDevicePath() + "." + sensor;
        try {
          long st = System.nanoTime();
          QueryDataSet queryDataSet = engine.query(timeseres, startTime, endTime);
          while (queryDataSet.hasNext()) {
            queryDataSet.next();
            line++;
          }
          long et = System.nanoTime();
          cost += (et - st);
        } catch (IOException e) {
          e.printStackTrace();
          return new Status(false, cost, 0, e, e.getMessage());
        }
      }
    }
    return new Status(true, cost, line);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    long cost = 0;
    int line = 0;
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    for (DeviceSchema deviceSchema : rangeQuery.getDeviceSchema()) {
      for (String sensor : deviceSchema.getSensors()) {
        String timeseries = deviceSchema.getDevicePath() + "." + sensor;
        try {
          long st = System.nanoTime();
          QueryDataSet queryDataSet = engine.query(timeseries, startTime, endTime);
          while (queryDataSet.hasNext()) {
            queryDataSet.next();
            line++;
          }
          long et = System.nanoTime();
          cost += (et - st);
        } catch (IOException e) {
          e.printStackTrace();
          return new Status(false, cost, 0, e, e.getMessage());
        }
      }
    }
    return new Status(true, cost, line);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    throw new RuntimeException("No need to implement");
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    throw new RuntimeException("No need to implement");
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    throw new RuntimeException("No need to implement");
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    throw new RuntimeException("No need to implement");
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    throw new RuntimeException("No need to implement");
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    throw new RuntimeException("No need to implement");
  }
}