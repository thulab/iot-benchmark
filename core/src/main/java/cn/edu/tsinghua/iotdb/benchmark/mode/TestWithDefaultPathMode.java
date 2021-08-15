package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
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

public class TestWithDefaultPathMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestWithDefaultPathMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Start benchmark */
  @Override
  public void run() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();
    recorder.saveTestConfig();

    Measurement measurement = new Measurement();
    DBWrapper dbWrapper = new DBWrapper(measurement);
    // register schema if needed
    try {
      dbWrapper.init();
      if (config.isIS_DELETE_DATA()) {
        try {
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
        }
      }
      try {
        BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();
        List<DeviceSchema> schemaList = new ArrayList<>();
        for (List<DeviceSchema> schemas : baseDataSchema.getClientBindSchema().values()) {
          schemaList.addAll(schemas);
        }
        dbWrapper.registerSchema(schemaList);
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
    // create getCLIENT_NUMBER() client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
    CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());
    long st = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
    LOGGER.info("Generating workload buffer...");
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
      clients.add(client);
      st = System.nanoTime();
      executorService.submit(client);
    }
    finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
  }
}
