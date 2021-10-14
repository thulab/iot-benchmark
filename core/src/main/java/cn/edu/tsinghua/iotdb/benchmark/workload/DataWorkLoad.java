package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IDataWorkLoad;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class DataWorkLoad implements IDataWorkLoad {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  protected static final Integer BUFFER_SIZE = 500;

  protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  protected ArrayBlockingQueue<Batch> batches = new ArrayBlockingQueue<>(BUFFER_SIZE);

  @Override
  public void startGetData() throws WorkloadException {
    for (int i = 0; i < BUFFER_SIZE; i++) {
      generateBatch();
    }
    executorService.scheduleAtFixedRate(
        () -> {
          try {
            generateBatch();
          } catch (WorkloadException e) {
            e.printStackTrace();
          }
        },
        0,
        100,
        TimeUnit.MICROSECONDS);
  }

  @Override
  public Batch getOneBatch() throws WorkloadException {
    try {
      Batch batch = batches.poll();
      return batch;
    } catch (Exception exception) {
      throw new WorkloadException("Failed to get batch");
    }
  }

  protected abstract void generateBatch() throws WorkloadException;

  @Override
  public void finishGenerate() throws WorkloadException {
    batches.clear();
    executorService.shutdown();
  }

  public static IDataWorkLoad getInstance(int clientId) throws WorkloadException {
    IDataWorkLoad dataWorkLoad = null;
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_WRITE
        || config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_QUERY) {
      List<String> files = MetaUtil.getClientFiles().get(clientId);
      dataWorkLoad = new RealDataWorkLoad(files);
    } else {
      List<DeviceSchema> deviceSchemas = metaDataSchema.getDeviceSchemaByClientId(clientId);
      if (config.isIS_CLIENT_BIND()) {
        dataWorkLoad = new SyntheticDataWorkLoad(deviceSchemas);
      } else {
        dataWorkLoad = SingletonWorkDataWorkLoad.getInstance();
      }
    }
    dataWorkLoad.startGetData();
    return dataWorkLoad;
  }
}
