package cn.edu.tsinghua.iot.benchmark.client.generate;

import cn.edu.tsinghua.iot.benchmark.client.progress.TaskProgress;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.BuiltTsFile;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadResult;
import cn.edu.tsinghua.iot.benchmark.workload.SyntheticDataWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.interfaces.IDataWorkLoad;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Generates this client's normal write workload, then writes and LOADs one external TsFile. */
public class TsFileLoadClient extends GenerateBaseClient {
  private static final BuiltTsFile POISON = new BuiltTsFile(new File(""), -1L, -1L);

  public TsFileLoadClient(
      int id, CountDownLatch latch, CyclicBarrier barrier, TaskProgress progress) {
    super(id, latch, barrier, progress);
  }

  @Override
  protected void doTest() {
    List<List<DeviceSchema>> deviceGroups = splitDeviceGroups();
    taskProgress.setTotalLoop((long) config.getLOOP() * deviceGroups.size());
    LoadStatistics statistics = new LoadStatistics();
    AtomicReference<Exception> transferFailure = new AtomicReference<>();
    int pipelineDepth = Math.max(0, config.getTSFILE_LOAD_PIPELINE_DEPTH());
    ExecutorService transferExecutor = null;
    BlockingQueue<BuiltTsFile> transferQueue = null;
    if (pipelineDepth > 0) {
      transferQueue = new ArrayBlockingQueue<>(pipelineDepth);
      transferExecutor =
          Executors.newSingleThreadExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "tsfile-load-transfer-" + clientThreadId);
                thread.setDaemon(true);
                return thread;
              });
      BlockingQueue<BuiltTsFile> queue = transferQueue;
      transferExecutor.execute(() -> drainTransfers(queue, statistics, transferFailure));
    }

    taskProgress.resetLoopIndex();
    try {
      for (List<DeviceSchema> deviceGroup : deviceGroups) {
        if (!generateDeviceGroup(deviceGroup, statistics, transferQueue, transferFailure)) {
          break;
        }
      }
      if (transferQueue != null) {
        transferQueue.put(POISON);
      }
    } catch (Exception e) {
      LOGGER.error("TsFile LOAD client {} failed to generate workload", clientThreadId, e);
      if (transferQueue != null) {
        transferQueue.offer(POISON);
      }
    } finally {
      if (transferExecutor != null) {
        transferExecutor.shutdown();
        try {
          if (!transferExecutor.awaitTermination(1, TimeUnit.HOURS)) {
            transferExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          transferExecutor.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
    }

    Exception failure = transferFailure.get();
    if (failure != null) {
      LOGGER.error("TsFile LOAD client {} failed during transfer/LOAD", clientThreadId, failure);
      return;
    }
    LOGGER.info(
        "TsFile LOAD client {}: files={}, rows={}, points={}, build={} ms, transfer={} ms, load={} ms",
        clientThreadId,
        statistics.files,
        statistics.rows,
        statistics.points,
        statistics.buildNanos / 1_000_000.0,
        statistics.transferNanos / 1_000_000.0,
        statistics.loadNanos / 1_000_000.0);
  }

  /**
   * Generates and flushes one bounded device group before advancing to the next group. This keeps
   * only one external TsFile's rows in memory instead of accumulating one buffer per device group
   * for the entire workload.
   */
  private boolean generateDeviceGroup(
      List<DeviceSchema> deviceGroup,
      LoadStatistics statistics,
      BlockingQueue<BuiltTsFile> transferQueue,
      AtomicReference<Exception> transferFailure)
      throws Exception {
    IDataWorkLoad groupWorkload = new SyntheticDataWorkLoad(deviceGroup);
    FileBuffer fileBuffer = new FileBuffer();
    int innerLoop = config.isIS_SENSOR_TS_ALIGNMENT() ? 1 : config.getSENSOR_NUMBER();
    for (int loop = 0; loop < config.getLOOP(); loop++) {
      for (int i = 0; i < deviceGroup.size(); i += config.getDEVICE_NUM_PER_WRITE()) {
        for (int j = 0; j < innerLoop; j++) {
          IBatch batch = groupWorkload.getOneBatch();
          if (checkBatch(batch)) {
            appendBatch(batch, fileBuffer, statistics, transferQueue, transferFailure);
          }
        }
      }
      taskProgress.incrementLoopIndex();
      if (isStop.get()) {
        return false;
      }
    }
    flush(fileBuffer, statistics, transferQueue, transferFailure);
    return true;
  }

  private void appendBatch(
      IBatch batch,
      FileBuffer fileBuffer,
      LoadStatistics statistics,
      BlockingQueue<BuiltTsFile> transferQueue,
      AtomicReference<Exception> transferFailure)
      throws Exception {
    batch.reset();
    while (true) {
      if (batch.getDeviceSchema().getSensors().stream()
          .anyMatch(sensor -> sensor.getSensorType() == SensorType.OBJECT)) {
        throw new IllegalArgumentException("tsFileLoadMode does not support OBJECT sensors.");
      }
      List<Record> records = batch.getRecords();
      int recordStart = 0;
      while (recordStart < records.size()) {
        int valuesPerRecord = records.get(recordStart).getRecordDataValue().size();
        if (valuesPerRecord <= 0) {
          throw new IllegalArgumentException("A TsFile record must contain at least one value.");
        }
        if (!fileBuffer.isEmpty() && wouldExceedFileLimit(fileBuffer, valuesPerRecord)) {
          flush(fileBuffer, statistics, transferQueue, transferFailure);
        }

        int recordEnd = recordStart;
        while (recordEnd < records.size()) {
          int recordValues = records.get(recordEnd).getRecordDataValue().size();
          if (recordValues <= 0) {
            throw new IllegalArgumentException("A TsFile record must contain at least one value.");
          }
          if (wouldExceedFileLimit(fileBuffer, recordValues)) {
            break;
          }
          fileBuffer.points += recordValues;
          fileBuffer.rows++;
          recordEnd++;
        }
        // Rows cannot be split without changing the original Tablet data.
        if (recordEnd == recordStart) {
          fileBuffer.points += valuesPerRecord;
          fileBuffer.rows++;
          recordEnd++;
        }
        fileBuffer.append(batch.getDeviceSchema(), records.subList(recordStart, recordEnd));
        recordStart = recordEnd;
        if (isFileLimitReached(fileBuffer)) {
          flush(fileBuffer, statistics, transferQueue, transferFailure);
        }
      }
      if (!batch.hasNext()) {
        break;
      }
      batch.next();
    }
    batch.reset();
  }

  private void drainTransfers(
      BlockingQueue<BuiltTsFile> queue,
      LoadStatistics statistics,
      AtomicReference<Exception> transferFailure) {
    try {
      while (true) {
        BuiltTsFile built = queue.take();
        if (built == POISON) {
          return;
        }
        TsFileLoadResult result = dbWrapper.transferAndLoadTsFile(built, clientThreadId);
        synchronized (statistics) {
          statistics.add(result);
        }
      }
    } catch (Exception e) {
      transferFailure.compareAndSet(null, e);
      queue.clear();
    }
  }

  private void flush(
      FileBuffer fileBuffer,
      LoadStatistics statistics,
      BlockingQueue<BuiltTsFile> transferQueue,
      AtomicReference<Exception> transferFailure)
      throws Exception {
    if (fileBuffer.isEmpty()) {
      return;
    }
    Exception failure = transferFailure.get();
    if (failure != null) {
      throw failure;
    }
    List<IBatch> batches = fileBuffer.takeBatches();
    long rows = fileBuffer.takeRows();
    BuiltTsFile built = buildTsFile(batches, statistics.files++);
    if (transferQueue == null) {
      TsFileLoadResult result = dbWrapper.transferAndLoadTsFile(built, clientThreadId);
      synchronized (statistics) {
        statistics.rows += rows;
        statistics.add(result);
      }
    } else {
      synchronized (statistics) {
        statistics.rows += rows;
      }
      transferQueue.put(built);
    }
  }

  private BuiltTsFile buildTsFile(List<IBatch> batches, int fileIndex) throws Exception {
    File localDirectory = new File(config.getTSFILE_LOAD_LOCAL_DIR().trim());
    if (config.getTSFILE_LOAD_LOCAL_DIR().trim().isEmpty()) {
      localDirectory = new File(".");
    }
    if (!localDirectory.isDirectory() && !localDirectory.mkdirs()) {
      throw new IOException("Cannot create TSFILE_LOAD_LOCAL_DIR: " + localDirectory);
    }
    File file =
        new File(localDirectory, "tsfile-load-" + clientThreadId + "-" + fileIndex + ".tsfile")
            .getAbsoluteFile();
    return dbWrapper.buildTsFile(batches, file);
  }

  private List<List<DeviceSchema>> splitDeviceGroups() {
    if (config.getDEVICE_NUM_PER_WRITE() != 1) {
      throw new IllegalArgumentException(
          "tsFileLoadMode requires DEVICE_NUM_PER_WRITE=1 for bounded device-group generation.");
    }
    Map<String, List<DeviceSchema>> schemasByGroup = new LinkedHashMap<>();
    for (DeviceSchema schema : clientDeviceSchemas) {
      schemasByGroup.computeIfAbsent(schema.getGroup(), ignored -> new ArrayList<>()).add(schema);
    }
    List<List<DeviceSchema>> deviceGroups = new ArrayList<>();
    for (List<DeviceSchema> schemas : schemasByGroup.values()) {
      for (int offset = 0;
          offset < schemas.size();
          offset += config.getTSFILE_LOAD_MAX_DEVICES_PER_FILE()) {
        deviceGroups.add(
            new ArrayList<>(
                schemas.subList(
                    offset,
                    Math.min(
                        offset + config.getTSFILE_LOAD_MAX_DEVICES_PER_FILE(), schemas.size()))));
      }
    }
    return deviceGroups;
  }

  private boolean wouldExceedFileLimit(FileBuffer fileBuffer, int valuesPerRecord) {
    if (config.getTSFILE_LOAD_ROWS_PER_FILE() > 0) {
      return fileBuffer.rows + 1 > config.getTSFILE_LOAD_ROWS_PER_FILE();
    }
    return fileBuffer.points + valuesPerRecord > config.getTSFILE_LOAD_POINTS_PER_FILE();
  }

  private boolean isFileLimitReached(FileBuffer fileBuffer) {
    if (config.getTSFILE_LOAD_ROWS_PER_FILE() > 0) {
      return fileBuffer.rows == config.getTSFILE_LOAD_ROWS_PER_FILE();
    }
    return fileBuffer.points == config.getTSFILE_LOAD_POINTS_PER_FILE();
  }

  private static class FileBuffer {
    private final LinkedHashMap<String, DeviceRows> devices = new LinkedHashMap<>();
    private long points;
    private long rows;
    private long pendingRows;

    private boolean isEmpty() {
      return devices.isEmpty();
    }

    private void append(DeviceSchema schema, List<Record> records) {
      String deviceKey = schema.getGroup() + "." + schema.getDevice();
      DeviceRows deviceRows = devices.computeIfAbsent(deviceKey, ignored -> new DeviceRows(schema));
      deviceRows.records.addAll(records);
    }

    private List<IBatch> takeBatches() {
      List<IBatch> batches = new ArrayList<>(devices.size());
      for (DeviceRows deviceRows : devices.values()) {
        batches.add(new Batch(deviceRows.schema, deviceRows.records));
      }
      devices.clear();
      pendingRows = rows;
      points = 0;
      rows = 0;
      return batches;
    }

    private long takeRows() {
      long value = pendingRows;
      pendingRows = 0;
      return value;
    }
  }

  private static class DeviceRows {
    private final DeviceSchema schema;
    private final List<Record> records = new ArrayList<>();

    private DeviceRows(DeviceSchema schema) {
      this.schema = schema;
    }
  }

  private static class LoadStatistics {
    private long buildNanos;
    private long transferNanos;
    private long loadNanos;
    private long points;
    private long rows;
    private int files;

    private void add(TsFileLoadResult result) {
      buildNanos += result.getBuildNanos();
      transferNanos += result.getTransferNanos();
      loadNanos += result.getLoadNanos();
      points += result.getPoints();
    }
  }
}
