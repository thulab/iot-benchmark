package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.utils.FileUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.IGenerateDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class GenerateDataClient implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private final CountDownLatch countDownLatch;
  private final CyclicBarrier barrier;

  private int clientThreadId;

  private final IGenerateDataWorkload syntheticWorkload;
  private final SingletonWorkload singletonWorkload;
  private long insertLoopIndex;
  private final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long loopIndex;

  public GenerateDataClient(
      int id,
      CountDownLatch countDownLatch,
      CyclicBarrier barrier,
      IGenerateDataWorkload workload) {
    this.clientThreadId = id;
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    syntheticWorkload = workload;
    singletonWorkload = SingletonWorkload.getInstance();
    insertLoopIndex = 0;
  }

  /** Run for generate Data */
  @Override
  public void run() {
    try {
      try {
        // wait for that all clients start test simultaneously
        barrier.await();

        generate();

      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  void generate() {
    String currentThread = Thread.currentThread().getName();
    // actualDeviceFloor equals to device number when REAL_INSERT_RATE = 1
    // actualDeviceFloor = device_number * first_device_index(The first index of this benchmark)
    //                   + device_number * real_insert_rate(Actual number of devices generated)
    double actualDeviceFloor =
        config.getDEVICE_NUMBER() * config.getFIRST_DEVICE_INDEX()
            + config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE();

    // print current progress periodically
    service.scheduleAtFixedRate(
        () -> {
          String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.getLOOP());
          LOGGER.info("{} {}% syntheticWorkload is done.", currentThread, percent);
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);
    loop:
    for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
      // According to the probabilities (proportion) of operations.
      if (!doGenerate(actualDeviceFloor)) {
        break loop;
      }
    }
    service.shutdown();
  }

  /**
   * Do Ingestion Operation
   *
   * @param actualDeviceFloor @Return when connect failed return false
   */
  private boolean doGenerate(double actualDeviceFloor) {
    if (config.isIS_CLIENT_BIND()) {
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
        try {
          List<DeviceSchema> schemas = baseDataSchema.getClientBindSchema().get(clientThreadId);
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() < actualDeviceFloor) {
              Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
              writeBatch(batch);
            }
          }
        } catch (Exception e) {
          LOGGER.error("Failed to insert one batch data because ", e);
        }
        insertLoopIndex++;
      } else {
        // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
        try {
          List<DeviceSchema> schemas = baseDataSchema.getClientBindSchema().get(clientThreadId);
          DeviceSchema sensorSchema = null;
          List<String> sensorList = new ArrayList<String>();
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() < actualDeviceFloor) {
              int colIndex = 0;
              for (String sensor : deviceSchema.getSensors()) {
                sensorList = new ArrayList<String>();
                sensorList.add(sensor);
                sensorSchema = (DeviceSchema) deviceSchema.clone();
                sensorSchema.setSensors(sensorList);
                Batch batch =
                    syntheticWorkload.getOneBatch(sensorSchema, insertLoopIndex, colIndex);
                batch.setColIndex(colIndex);
                Type colType = baseDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
                batch.setColType(colType);
                writeBatch(batch);
                colIndex++;
                insertLoopIndex++;
              }
            }
          }
        } catch (Exception e) {
          LOGGER.error("Failed to insert one batch data because ", e);
        }
      }
    } else {
      // IS_CLIENT_BIND = false
      // not in use
      try {
        Batch batch = singletonWorkload.getOneBatch();
        if (batch.getDeviceSchema().getDeviceId() < actualDeviceFloor) {
          writeBatch(batch);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      }
    }
    return true;
  }

  void writeBatch(Batch batch) {
    String device = batch.getDeviceSchema().getDevice();
    try {
      Path dirFile = Paths.get(FileUtils.union(config.getFILE_PATH(), device));
      if (!Files.exists(dirFile)) {
        Files.createDirectories(dirFile);
      }
      Path dataFile =
          Paths.get(FileUtils.union(config.getFILE_PATH(), device, "batch_" + loopIndex + ".txt"));
      Files.createFile(dataFile);
      List<String> sensors = batch.getDeviceSchema().getSensors();
      String sensorLine = String.join(" ", sensors);
      sensorLine = "Sensor " + sensorLine + "\n";
      Files.write(dataFile, sensorLine.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      for (Record record : batch.getRecords()) {
        StringBuilder line = new StringBuilder(String.valueOf(record.getTimestamp()));
        for (String sensor : sensors) {
          int index = Integer.valueOf(sensor.split("_")[1]);
          line.append(" ").append(record.getRecordDataValue().get(index));
        }
        line.append("\n");
        Files.write(
            dataFile, line.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      }
    } catch (IOException ioException) {
      LOGGER.error("Write batch Error!" + batch.toString());
    }
  }
}
