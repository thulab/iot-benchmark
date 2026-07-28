package cn.edu.tsinghua.iot.benchmark.iotdb200;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.BuiltTsFile;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadResult;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadRouting;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsFileLoadTransfer;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

final class TsFileLoadWriter {
  private static final int LOAD_SESSION_MAX_ATTEMPTS = 10;
  private static final ThreadLocal<Session> LOAD_SESSIONS = new ThreadLocal<>();

  private TsFileLoadWriter() {}

  static TsFileLoadResult buildAndLoad(
      DBConfig config, String root, List<IBatch> batches, File file, int clientId)
      throws Exception {
    return transferAndLoad(config, build(config, root, batches, file), clientId);
  }

  static BuiltTsFile build(DBConfig config, String root, List<IBatch> batches, File file)
      throws Exception {
    Config benchmarkConfig = ConfigDescriptor.getInstance().getConfig();
    if (benchmarkConfig.getIoTDB_DIALECT_MODE() == SQLDialect.TABLE) {
      return buildTable(config, batches, file, benchmarkConfig);
    }
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
        List<IMeasurementSchema> schemas = new ArrayList<>(sensors.size());
        List<String> measurements = new ArrayList<>(sensors.size());
        List<TSDataType> dataTypes = new ArrayList<>(sensors.size());
        SensorTypeFill[] fills = new SensorTypeFill[sensors.size()];
        for (int i = 0; i < sensors.size(); i++) {
          Sensor sensor = sensors.get(i);
          TSDataType dataType = TSDataType.valueOf(sensor.getSensorType().name);
          schemas.add(
              new MeasurementSchema(
                  sensor.getName(),
                  dataType,
                  TSEncoding.valueOf(IoTDB.getEncodingType(sensor.getSensorType())),
                  compression));
          measurements.add(sensor.getName());
          dataTypes.add(dataType);
          fills[i] = SensorTypeFill.of(sensor.getSensorType().name);
        }

        IDeviceID deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(entry.getKey());
        if (isAligned) {
          writer.registerAlignedTimeseries(deviceId, schemas);
        } else {
          for (IMeasurementSchema schema : schemas) {
            writer.registerTimeseries(deviceId, schema);
          }
        }

        Tablet tablet = new Tablet(deviceId, measurements, dataTypes, rows.size());
        long[] timestamps = tablet.getTimestamps();
        Object[] values = tablet.getValues();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
          Record row = rows.get(rowIndex);
          timestamps[rowIndex] = row.getTimestamp();
          List<Object> recordValues = row.getRecordDataValue();
          for (int sensorIndex = 0; sensorIndex < recordValues.size(); sensorIndex++) {
            fills[sensorIndex].fill(values[sensorIndex], rowIndex, recordValues.get(sensorIndex));
          }
        }
        tablet.setRowSize(rows.size());
        if (isAligned) {
          writer.writeAligned(tablet);
        } else {
          writer.writeTree(tablet);
        }
        points += (long) rows.size() * sensors.size();
      }
    }
    return new BuiltTsFile(file, System.nanoTime() - buildStart, points);
  }

  private static BuiltTsFile buildTable(
      DBConfig config, List<IBatch> batches, File file, Config benchmarkConfig) throws Exception {
    Map<String, DeviceRows> devices = groupByTable(batches);
    String group = null;
    long points = 0;
    long buildStart = System.nanoTime();
    CompressionType compression = CompressionType.valueOf(benchmarkConfig.getCOMPRESSOR());
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (DeviceRows deviceRows : devices.values()) {
        DeviceSchema schema = deviceRows.schema;
        if (group == null) group = schema.getGroup();
        if (!group.equals(schema.getGroup())) {
          throw new IllegalArgumentException("A table-model LOAD TsFile must contain one database");
        }
        List<Record> rows = deviceRows.records;
        if (benchmarkConfig.isIS_OUT_OF_ORDER())
          rows.sort(Comparator.comparingLong(Record::getTimestamp));
        List<String> names = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<ColumnCategory> categories = new ArrayList<>();
        for (Sensor sensor : schema.getSensors()) {
          names.add(sensor.getName());
          types.add(TSDataType.valueOf(sensor.getSensorType().name));
          categories.add(ColumnCategory.FIELD);
        }
        names.add("device_id");
        types.add(TSDataType.STRING);
        categories.add(ColumnCategory.TAG);
        List<String> tagNames = new ArrayList<>(new TreeSet<>(schema.getTags().keySet()));
        for (String tagName : tagNames) {
          names.add(tagName);
          types.add(TSDataType.STRING);
          categories.add(ColumnCategory.TAG);
        }
        writer.registerTableSchema(new TableSchema(schema.getTable(), names, types, categories));
        Tablet tablet = new Tablet(schema.getTable(), names, types, categories, rows.size());
        Object[] values = tablet.getValues();
        SensorTypeFill[] fills = new SensorTypeFill[schema.getSensors().size()];
        for (int i = 0; i < fills.length; i++) {
          fills[i] = SensorTypeFill.of(schema.getSensors().get(i).getSensorType().name);
        }
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
          Record row = rows.get(rowIndex);
          tablet.getTimestamps()[rowIndex] = row.getTimestamp();
          for (int i = 0; i < fills.length; i++) {
            fills[i].fill(values[i], rowIndex, row.getRecordDataValue().get(i));
          }
          SensorTypeFill.of("STRING").fill(values[fills.length], rowIndex, schema.getDevice());
          for (int i = 0; i < tagNames.size(); i++) {
            SensorTypeFill.of("STRING")
                .fill(
                    values[fills.length + 1 + i], rowIndex, schema.getTags().get(tagNames.get(i)));
          }
        }
        tablet.setRowSize(rows.size());
        writer.writeTable(tablet);
        points += (long) rows.size() * schema.getSensors().size();
      }
    }
    return new BuiltTsFile(
        file,
        System.nanoTime() - buildStart,
        points,
        group == null ? null : config.getDB_NAME() + "_" + group);
  }

  static TsFileLoadResult transferAndLoad(DBConfig config, BuiltTsFile built, int clientId)
      throws Exception {
    Config benchmarkConfig = ConfigDescriptor.getInstance().getConfig();
    TsFileLoadRouting.validate(benchmarkConfig, config);
    TsFileLoadTransfer.StagedFile staged =
        TsFileLoadTransfer.stage(built.getFile(), benchmarkConfig, config, clientId);
    long loadStart = System.nanoTime();
    try {
      executeLoadWithRetry(config, clientId, staged.getPath(), built.getDatabase());
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

  private static Map<String, DeviceRows> groupByTable(List<IBatch> batches) {
    Map<String, DeviceRows> recordsByDevice = new LinkedHashMap<>();
    for (IBatch batch : batches) {
      DeviceSchema schema = batch.getDeviceSchema();
      String key = schema.getGroup() + "." + schema.getTable() + "." + schema.getDevice();
      DeviceRows deviceRows =
          recordsByDevice.computeIfAbsent(key, ignored -> new DeviceRows(schema));
      deviceRows.records.addAll(batch.getRecords());
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

  private static void executeLoadWithRetry(
      DBConfig config, int clientId, String stagedPath, String database) throws Exception {
    Exception failure = null;
    String statement = "LOAD '" + stagedPath.replace("'", "''") + "' onSuccess=delete";
    for (int attempt = 1; attempt <= LOAD_SESSION_MAX_ATTEMPTS; attempt++) {
      try {
        Session session = getLoadSession(config, clientId);
        if (database != null) {
          session.executeNonQueryStatement("USE " + database);
        }
        session.executeNonQueryStatement(statement);
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
              .sqlDialect(ConfigDescriptor.getInstance().getConfig().getIoTDB_DIALECT_MODE().name())
              .build();
      session.open();
      LOAD_SESSIONS.set(session);
    }
    return session;
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

    static SensorTypeFill of(String sensorType) {
      switch (sensorType) {
        case "BOOLEAN":
          return (column, rowIndex, value) -> ((boolean[]) column)[rowIndex] = (Boolean) value;
        case "INT32":
          return (column, rowIndex, value) ->
              ((int[]) column)[rowIndex] = ((Number) value).intValue();
        case "INT64":
        case "TIMESTAMP":
          return (column, rowIndex, value) ->
              ((long[]) column)[rowIndex] = ((Number) value).longValue();
        case "FLOAT":
          return (column, rowIndex, value) ->
              ((float[]) column)[rowIndex] = ((Number) value).floatValue();
        case "DOUBLE":
          return (column, rowIndex, value) ->
              ((double[]) column)[rowIndex] = ((Number) value).doubleValue();
        case "TEXT":
        case "STRING":
          return (column, rowIndex, value) ->
              ((Binary[]) column)[rowIndex] = new Binary((String) value, StandardCharsets.UTF_8);
        case "BLOB":
          return (column, rowIndex, value) -> {
            byte[] bytes =
                value instanceof byte[]
                    ? (byte[]) value
                    : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            ((Binary[]) column)[rowIndex] = new Binary(bytes);
          };
        case "DATE":
          return (column, rowIndex, value) -> ((LocalDate[]) column)[rowIndex] = (LocalDate) value;
        default:
          throw new IllegalArgumentException(
              "tsFileLoadMode does not support sensor type: " + sensorType);
      }
    }
  }
}
