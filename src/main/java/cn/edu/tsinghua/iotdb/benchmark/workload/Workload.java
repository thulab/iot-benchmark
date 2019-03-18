package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.DataPoint;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Workload {

  private static final Logger LOGGER = LoggerFactory.getLogger(Workload.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private int clientId;
  private long insertLoopIndex;
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private int curDeviceOffset = 0;
  private List<DeviceSchema> clientDeviceSchemaList;
  private long curTimestamp = Constants.START_TIMESTAMP;

  public Workload(int clientId) {
    this.clientId = clientId;
    this.insertLoopIndex = 0;
    clientDeviceSchemaList = DataSchema.getInstance().getClientBindSchema().get(clientId);
  }

  private Batch getOrderedBatch() {
    DeviceSchema curDeviceSchema = clientDeviceSchemaList.get(curDeviceOffset);
    long currentTimestamp;
    Batch batch = new Batch();
    String group = curDeviceSchema.getGroup();
    String device = curDeviceSchema.getDevice();
    for (int i = 0; i < config.BATCH_SIZE; i++) {
      curTimestamp += config.POINT_STEP;
      if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
        currentTimestamp = curTimestamp + (long) (config.POINT_STEP * timestampRandom.nextDouble());
      } else {
        currentTimestamp = curTimestamp;
      }
      for(String sensor: curDeviceSchema.getSensors()){
        FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
        String value = Function.getValueByFuntionidAndParam(param, currentTimestamp) + "";
        DataPoint dataPoint = new DataPoint(group, device, sensor, currentTimestamp, value);
        batch.add(dataPoint);
      }
    }
    curDeviceOffset ++;
    if(curDeviceOffset == clientDeviceSchemaList.size()){
      curDeviceOffset = 0;
    }
    return batch;
  }

  private Batch getDistOutOfOrderBatch() {
    return null;
  }

  private Batch getLocalOutOfOrderBatch() {
    return null;
  }

  private Batch getGlobalOutOfOrderBatch() {
    return null;
  }

  public Batch getOneBatch() throws WorkloadException {
    if (!config.IS_OVERFLOW) {
      return getOrderedBatch();
    } else {
      switch (config.OVERFLOW_MODE) {
        case 0:
          return getLocalOutOfOrderBatch();
        case 1:
          return getGlobalOutOfOrderBatch();
        case 2:
          return getDistOutOfOrderBatch();
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
    }
  }

  public PreciseQuery getPreciseQuery() {
    return null;
  }

  public RangeQuery getRangeQuery() {
    return null;
  }

  public ValueRangeQuery getValueRangeQuery() {
    return null;
  }

  public AggRangeQuery getAggRangeQuery() {
    return null;
  }

  public AggValueQuery getAggValueQuery() {
    return null;
  }

  public AggRangeValueQuery getAggRangeValueQuery() {
    return null;
  }

  public GroupByQuery getGroupByQuery() {
    return null;
  }

  public LatestPointQuery getLatestPointQuery() {
    return null;
  }


}
