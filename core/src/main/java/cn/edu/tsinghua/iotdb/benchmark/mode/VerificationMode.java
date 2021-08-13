package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDataSetQueryClient;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDataSetWriteClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** @Author stormbroken Create by 2021/08/13 @Version 1.0 */
public class VerificationMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(VerificationMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  /** Start benchmark */
  @Override
  public void run() {
    List<DeviceSchema> deviceSchemaList = baseDataSchema.getAllDeviceSchema();

    Measurement measurement = new Measurement();
    DBWrapper dbWrapper = new DBWrapper(measurement);
    // register schema if needed
    try {
      LOGGER.info("start to init database {}", config.getNET_DEVICE());
      dbWrapper.init();
      if (config.isIS_DELETE_DATA()) {
        try {
          LOGGER.info("start to clean old data");
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
        }
      }
      try {
        // register device schema
        LOGGER.info("start to register schema");
        dbWrapper.registerSchema(deviceSchemaList);
      } catch (TsdbException e) {
        LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
      }
    } catch (TsdbException e) {
      LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
    } finally {
      try {
        dbWrapper.close();
      } catch (TsdbException e) {
        LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
      }
    }
    LOGGER.info("Write Real Data!");
    write(measurement);
    LOGGER.info("Query Real Data!");
    check(measurement);
  }

  private void write(Measurement measurement) {
    CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

    // create getCLIENT_NUMBER() client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
    long st = System.nanoTime();
    ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      Client client = new RealDataSetWriteClient(i, downLatch, barrier, new RealDataWorkload(i));
      clients.add(client);
      executorService.submit(client);
    }
    finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
  }

  private void check(Measurement measurement) {
    CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

    // create getCLIENT_NUMBER() client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
    long st = System.nanoTime();
    ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      Client client = new RealDataSetQueryClient(i, downLatch, barrier, new RealDataWorkload(i));
      clients.add(client);
      executorService.submit(client);
    }
    finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
  }
}
