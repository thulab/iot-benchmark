package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.GeolifeReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.ReddReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.TDriveReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.ArrayList;
import java.util.List;

public class RealDatasetWorkLoad implements IWorkload {

  private BasicReader reader;

  private Config config;
  private List<DeviceSchema> deviceSchemaList;
  private long startTime;
  private long endTime;

  /**
   * write test.
   *
   * @param files real dataset files
   * @param config config
   */
  public RealDatasetWorkLoad(List<String> files, Config config) {
    switch (config.DATA_SET) {
      case TDRIVE:
        reader = new TDriveReader(config, files);
        break;
      case REDD:
        reader = new ReddReader(config, files);
        break;
      case GEOLIFE:
        reader = new GeolifeReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }
  }

  /**
   * read test.
   *
   * @param config config
   */
  public RealDatasetWorkLoad(Config config) {
    this.config = config;

    //init sensor list
    List<String> sensorList = new ArrayList<>();
    for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
      sensorList.add(config.FIELDS.get(i));
    }

    //init device schema list
    deviceSchemaList = new ArrayList<>();
    for (int i = 1; i <= config.QUERY_DEVICE_NUM; i++) {
      String deviceIdStr = "" + i;
      DeviceSchema deviceSchema = new DeviceSchema(calGroupIdStr(deviceIdStr, config.GROUP_NUMBER),
          deviceIdStr, sensorList);
      deviceSchemaList.add(deviceSchema);
    }

    //init startTime, endTime
    startTime = config.REAL_QUERY_START_TIME;
    endTime = config.REAL_QUERY_STOP_TIME;

  }

  public Batch getOneBatch() {
    if (reader.hasNextBatch()) {
      return reader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    throw new WorkloadException("not support in real data workload.");
  }

  @Override
  public PreciseQuery getPreciseQuery() {
    return new PreciseQuery(deviceSchemaList, startTime);
  }

  @Override
  public RangeQuery getRangeQuery() {
    return new RangeQuery(deviceSchemaList, startTime, endTime);
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() {
    return new ValueRangeQuery(deviceSchemaList, startTime, endTime, config.QUERY_LOWER_LIMIT);
  }

  @Override
  public AggRangeQuery getAggRangeQuery() {
    return new AggRangeQuery(deviceSchemaList, startTime, endTime, config.QUERY_AGGREGATE_FUN);
  }

  @Override
  public AggValueQuery getAggValueQuery() {
    return new AggValueQuery(startTime, endTime, deviceSchemaList, config.QUERY_AGGREGATE_FUN,
        config.QUERY_LOWER_LIMIT);
  }

  @Override
  public AggRangeValueQuery getAggRangeValueQuery() {
    return new AggRangeValueQuery(deviceSchemaList, startTime, endTime, config.QUERY_AGGREGATE_FUN,
        config.QUERY_LOWER_LIMIT);
  }

  @Override
  public GroupByQuery getGroupByQuery() {
    return new GroupByQuery(deviceSchemaList, startTime, endTime, config.QUERY_AGGREGATE_FUN,
        config.QUERY_INTERVAL);
  }

  @Override
  public LatestPointQuery getLatestPointQuery() {
    return new LatestPointQuery(deviceSchemaList, startTime, endTime, "last");
  }

  static String calGroupIdStr(String deviceId, int groupNum) {
    return String.valueOf(deviceId.hashCode() % groupNum);
  }

}
