package cn.edu.tsinghua.iot.benchmark.iotdb130;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.BuiltTsFile;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadResult;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadRouting;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadTransfer;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class TsFileLoadWriter {
  private static final int LOAD_SESSION_MAX_ATTEMPTS = 10;
  private static final ThreadLocal<Session> LOAD_SESSIONS = new ThreadLocal<>();

  private TsFileLoadWriter() {}

  static TsFileLoadResult buildAndLoad(
      DBConfig config, String root, List<IBatch> batches, File file, int clientId)
      throws Exception {
    return transferAndLoad(config, build(root, batches, file), clientId);
  }

  static BuiltTsFile build(String root, List<IBatch> batches, File file) throws Exception {
    Config benchmarkConfig = ConfigDescriptor.getInstance().getConfig();
    long points = 0;
    long buildStart = System.nanoTime();
    boolean sortNeeded = benchmarkConfig.isIS_OUT_OF_ORDER();
    boolean isAligned = benchmarkConfig.isVECTOR();
    CompressionType compression = CompressionType.valueOf(benchmarkConfig.getCOMPRESSOR());

    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (Map.Entry<String, DeviceRows> entry : groupByDevice(root, batches).entrySet()) {
        DeviceRows deviceRows = entry.getValue();
        List<Record> rows = deviceRows.records;
        if (rows.isEmpty()) {
          continue;
        }
        if (sortNeeded) {
          rows.sort(Comparator.comparingLong(Record::getTimestamp));
        }

        List<Sensor> sensors = deviceRows.schema.getSensors();
        List<MeasurementSchema> schemas = new ArrayList<>(sensors.size());
        SensorTypeFill[] fills = new SensorTypeFill[sensors.size()];
        for (int i = 0; i < sensors.size(); i++) {
          Sensor sensor = sensors.get(i);
          schemas.add(
              new MeasurementSchema(
                  sensor.getName(),
                  TSDataType.valueOf(sensor.getSensorType().name),
                  TSEncoding.valueOf(getEncodingType(benchmarkConfig, sensor.getSensorType())),
                  compression));
          fills[i] = SensorTypeFill.of(sensor.getSensorType());
        }

        Path devicePath = new Path(entry.getKey());
        if (isAligned) {
          writer.registerAlignedTimeseries(devicePath, schemas);
        } else {
          writer.registerTimeseries(devicePath, schemas);
        }

        Tablet tablet = new Tablet(entry.getKey(), schemas, rows.size());
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
          Record row = rows.get(rowIndex);
          timestamps[rowIndex] = row.getTimestamp();
          List<Object> recordValues = row.getRecordDataValue();
          for (int sensorIndex = 0; sensorIndex < recordValues.size(); sensorIndex++) {
            fills[sensorIndex].fill(values[sensorIndex], rowIndex, recordValues.get(sensorIndex));
          }
        }
        tablet.rowSize = rows.size();
        if (isAligned) {
          writer.writeAligned(tablet);
        } else {
          writer.write(tablet);
        }
        points += (long) rows.size() * sensors.size();
      }
    }
    return new BuiltTsFile(file, System.nanoTime() - buildStart, points);
  }

  static TsFileLoadResult transferAndLoad(DBConfig config, BuiltTsFile built, int clientId)
      throws Exception {
    Config benchmarkConfig = ConfigDescriptor.getInstance().getConfig();
    TsFileLoadRouting.validate(benchmarkConfig, config);
    TsFileLoadTransfer.StagedFile staged =
        TsFileLoadTransfer.stage(built.getFile(), benchmarkConfig, config, clientId);
    long loadStart = System.nanoTime();
    try {
      executeLoadWithRetry(config, clientId, staged.getPath());
    } finally {
      TsFileLoadTransfer.cleanup(staged, built.getFile(), benchmarkConfig);
    }
    return new TsFileLoadResult(
        built.getBuildNanos(),
        staged.getTransferNanos(),
        System.nanoTime() - loadStart,
        built.getPoints());
  }

  private static Map<String, DeviceRows> groupByDevice(String root, List<IBatch> batches) {
    Map<String, DeviceRows> recordsByDevice = new LinkedHashMap<>();
    for (IBatch batch : batches) {
      batch.reset();
      while (true) {
        DeviceSchema device = batch.getDeviceSchema();
        String devicePath = root + "." + device.getGroup() + "." + device.getDevice();
        DeviceRows deviceRows =
            recordsByDevice.computeIfAbsent(devicePath, ignored -> new DeviceRows(device));
        deviceRows.records.addAll(batch.getRecords());
        if (!batch.hasNext()) {
          break;
        }
        batch.next();
      }
      batch.reset();
    }
    return recordsByDevice;
  }

  static void closeLoadSession() {
    Session session = LOAD_SESSIONS.get();
    if (session != null) {
      try {
        session.close();
      } catch (Exception ignored) {
        // Closing the benchmark-only LOAD session must not mask the primary client shutdown error.
      } finally {
        LOAD_SESSIONS.remove();
      }
    }
  }

  private static void executeLoadWithRetry(DBConfig config, int clientId, String stagedPath)
      throws Exception {
    Exception failure = null;
    String statement = "LOAD '" + stagedPath.replace("'", "''") + "' onSuccess=delete";
    for (int attempt = 1; attempt <= LOAD_SESSION_MAX_ATTEMPTS; attempt++) {
      try {
        getLoadSession(config, clientId).executeNonQueryStatement(statement);
        return;
      } catch (Exception e) {
        failure = e;
        closeLoadSession();
      }
    }
    throw new IllegalStateException(
        "LOAD failed after " + LOAD_SESSION_MAX_ATTEMPTS + " session attempts", failure);
  }

  private static Session getLoadSession(DBConfig config, int clientId) throws Exception {
    Session session = LOAD_SESSIONS.get();
    if (session == null) {
      String assignedNode = TsFileLoadRouting.loadEndpoint(config, clientId);
      session =
          new Session.Builder()
              .nodeUrls(java.util.Collections.singletonList(assignedNode))
              .username(config.getUSERNAME())
              .password(config.getPASSWORD())
              .enableRedirection(false)
              .version(Version.V_1_0)
              .build();
      session.open();
      LOAD_SESSIONS.set(session);
    }
    return session;
  }

  private static String getEncodingType(Config config, SensorType sensorType) {
    switch (sensorType) {
      case BOOLEAN:
        return config.getENCODING_BOOLEAN();
      case INT32:
        return config.getENCODING_INT32();
      case INT64:
        return config.getENCODING_INT64();
      case FLOAT:
        return config.getENCODING_FLOAT();
      case DOUBLE:
        return config.getENCODING_DOUBLE();
      case TEXT:
        return config.getENCODING_TEXT();
      case STRING:
        return config.getENCODING_STRING();
      case BLOB:
        return config.getENCODING_BLOB();
      case TIMESTAMP:
        return config.getENCODING_TIMESTAMP();
      case DATE:
        return config.getENCODING_DATE();
      default:
        throw new IllegalArgumentException("Unsupported TSFile sensor type: " + sensorType);
    }
  }

  private static final class DeviceRows {
    private final DeviceSchema schema;
    private final List<Record> records = new ArrayList<>();

    private DeviceRows(DeviceSchema schema) {
      this.schema = schema;
    }
  }

  @FunctionalInterface
  private interface SensorTypeFill {
    void fill(Object column, int rowIndex, Object value);

    static SensorTypeFill of(SensorType sensorType) {
      switch (sensorType) {
        case BOOLEAN:
          return (column, rowIndex, value) -> ((boolean[]) column)[rowIndex] = (Boolean) value;
        case INT32:
          return (column, rowIndex, value) ->
              ((int[]) column)[rowIndex] = ((Number) value).intValue();
        case INT64:
        case TIMESTAMP:
          return (column, rowIndex, value) ->
              ((long[]) column)[rowIndex] = ((Number) value).longValue();
        case FLOAT:
          return (column, rowIndex, value) ->
              ((float[]) column)[rowIndex] = ((Number) value).floatValue();
        case DOUBLE:
          return (column, rowIndex, value) ->
              ((double[]) column)[rowIndex] = ((Number) value).doubleValue();
        case TEXT:
        case STRING:
          return (column, rowIndex, value) ->
              ((Binary[]) column)[rowIndex] = BytesUtils.valueOf((String) value);
        case BLOB:
          return (column, rowIndex, value) -> {
            byte[] bytes =
                value instanceof byte[]
                    ? (byte[]) value
                    : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            ((Binary[]) column)[rowIndex] = new Binary(bytes);
          };
        case DATE:
          return (column, rowIndex, value) -> ((LocalDate[]) column)[rowIndex] = (LocalDate) value;
        default:
          throw new IllegalArgumentException(
              "tsFileLoadMode does not support sensor type: " + sensorType);
      }
    }
  }
}
