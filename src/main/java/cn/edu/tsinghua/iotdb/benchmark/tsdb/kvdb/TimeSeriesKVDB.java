package cn.edu.tsinghua.iotdb.benchmark.tsdb.kvdb;


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
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import edu.tsinghua.k1.leveldb.LevelTimeSeriesDBFactory;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.iq80.leveldb.Options;

/**
 * 基于key-value数据库实现的TimeSeriesDB
 */
public class TimeSeriesKVDB implements IDatabase {


  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static ITimeSeriesDB timeSeriesDB;
  private static volatile int reference = 0;


  public TimeSeriesKVDB() {

  }

  private ITimeSeriesDB createKVDB() throws IOException {
    File file = new File(config.GEN_DATA_FILE_PATH);
    ITimeSeriesDB iTimeSeriesDB;
    System.out.println("create db " + config.DB_SWITCH);
    switch (config.DB_SWITCH) {
      case Constants.DB_LEVELDB:
        // create leveldb
        Options optionsLevel = new Options();
        optionsLevel.createIfMissing(true);
        iTimeSeriesDB = LevelTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsLevel);
        break;
      default:
        // create rocksdb
        org.rocksdb.Options optionsRocks = new org.rocksdb.Options();
        optionsRocks.setCreateIfMissing(true);
        iTimeSeriesDB = RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsRocks);
        break;
    }
    return iTimeSeriesDB;
  }

  @Override
  public void init() throws TsdbException {
    // Not need to implement
    synchronized (TimeSeriesKVDB.class) {
      if (this.timeSeriesDB == null) {
        try {
          this.timeSeriesDB = createKVDB();
        } catch (IOException e) {
          throw new TsdbException(e);
        }
      }
      reference++;
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // Not need to implement
  }

  @Override
  public void close() throws TsdbException {
    // Not need to implement
    synchronized (TimeSeriesKVDB.class) {
      if (reference == 1) {
        try {
          this.timeSeriesDB.close();
          this.timeSeriesDB = null;
        } catch (IOException e) {
          throw new TsdbException(e);
        }
      }
      reference--;
    }
  }

  @Override
  public void registerSchema(Measurement measurement) throws TsdbException {
    // No need to implement
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      ITimeSeriesWriteBatch timeSeriesWriteBatch = timeSeriesDB.createBatch();
      for (Record record : batch.getRecords()) {
        long time = record.getTimestamp();
        for (int i = 0; i < record.size(); i++) {
          String timeseries =
              batch.getDeviceSchema().getDevicePath() + "." + batch.getDeviceSchema().getSensors()
                  .get(i);
          byte[] value = double2Bytes(Double.valueOf(record.getRecordDataValue().get(i)));
          timeSeriesWriteBatch.write(timeseries, time, value);
        }
      }
      long st = System.nanoTime();
      timeSeriesDB.write(timeSeriesWriteBatch);
      long en = System.nanoTime();
      return new Status(true, en - st);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  public byte[] double2Bytes(double d) {
    long value = Double.doubleToRawLongBits(d);
    byte[] byteRet = new byte[8];
    for (int i = 0; i < 8; i++) {
      byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
    }
    return byteRet;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    // query multi path
    long cost = 0;
    int line = 0;
    for (DeviceSchema deviceSchema : rangeQuery.getDeviceSchema()) {
      for (String sensor : deviceSchema.getSensors()) {
        String timeseries = deviceSchema.getDevicePath() + "." + sensor;

        TimeSeriesDBIterator dbIterator;
        dbIterator = timeSeriesDB
            .iterator(timeseries, rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
        try {
          long st = System.nanoTime();
          while (dbIterator.hasNext()) {
            line++;
            Map.Entry<byte[], byte[]> entry = dbIterator.next();
            bytes2Double(entry.getValue());
            byte[] timeBytes = new byte[8];
            for (int i = 0; i < 8; i++) {
              timeBytes[i] = entry.getKey()[11 - i];
            }
            bytes2Long(timeBytes);
          }
          long et = System.nanoTime();
          cost += (et - st);
        } catch (Exception e) {
          return new Status(false, 0, e, e.toString());
        } finally {
          if (dbIterator != null) {
            dbIterator.close();
          }
        }
      }
    }
    return new Status(true, cost, line);
  }

  public static long bytes2Long(byte[] arr) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (arr[i] & 0xff)) << (8 * i);
    }
    return value;
  }

  public static double bytes2Double(byte[] arr) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (arr[i] & 0xff)) << (8 * i);
    }
    return Double.longBitsToDouble(value);
  }


  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    // Not need to implement
    throw new RuntimeException("Not support the preciseQuery");
  }
}