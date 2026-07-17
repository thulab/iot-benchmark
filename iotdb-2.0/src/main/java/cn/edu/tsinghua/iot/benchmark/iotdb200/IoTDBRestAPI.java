package cn.edu.tsinghua.iot.benchmark.iotdb200;

import org.apache.iotdb.rpc.IoTDBConnectionException;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import com.google.gson.Gson;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IoTDBRestAPI extends IoTDB {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBRestAPI.class);
  private static final Gson GSON = new Gson();
  private final OkHttpClient client;
  private final String baseURL;
  private final DBConfig dbConfig;
  protected final String ROOT_SERIES_NAME;
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  public IoTDBRestAPI(DBConfig dbConfig) throws IoTDBConnectionException {
    this(dbConfig, false);
  }

  IoTDBRestAPI(DBConfig dbConfig, boolean skipDmlStrategy) throws IoTDBConnectionException {
    super(dbConfig, !skipDmlStrategy);
    this.dbConfig = dbConfig;
    long timeout = config.getWRITE_OPERATION_TIMEOUT_MS();
    client =
        new OkHttpClient.Builder()
            .connectTimeout(timeout, TimeUnit.MILLISECONDS)
            .readTimeout(timeout, TimeUnit.MILLISECONDS)
            .writeTimeout(timeout, TimeUnit.MILLISECONDS)
            .build();
    String host = dbConfig.getHOST().get(0);
    baseURL = String.format("http://%s:%d", host, config.getREST_PORT());
    ROOT_SERIES_NAME = String.format("root.%s", dbConfig.getDB_NAME());
  }

  private Request constructRequest(String api, String json) {
    RequestBody jsonBody = RequestBody.create(MediaType.parse("application/json"), json);
    return new Request.Builder()
        .url(String.format("%s%s", baseURL, api))
        .header("Authorization", config.getREST_AUTHORIZATION())
        .post(jsonBody)
        .build();
  }

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() {
    if (isTableMode()) {
      for (int group = 0; group < config.getGROUP_NUMBER(); group++) {
        String database =
            String.format("%s_%s%d", dbConfig.getDB_NAME(), config.getGROUP_NAME_PREFIX(), group);
        executeNonQuery("drop database if exists " + database);
      }
    } else {
      executeNonQuery("delete database root." + dbConfig.getDB_NAME() + ".**");
    }
    LOGGER.info("Finish clean data!");
  }

  @Override
  public void close() throws TsdbException {}

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    String json = generatePayload(batch);
    String api = isTableMode() ? "/rest/table/v1/insertTablet" : "/rest/v2/insertTablet";
    Request request = constructRequest(api, json);
    try (Response response = client.newCall(request).execute()) {
      ResponseBody responseBody = response.body();
      String body = responseBody == null ? "" : responseBody.string();
      boolean success = isSuccessful(response, body);
      if (!success) {
        LOGGER.warn("Insert failed: HTTP {}, body {}", response.code(), body);
      }
      return new Status(success);
    } catch (IOException e) {
      LOGGER.warn("Insert failed: {}", e.toString());
      return new Status(false, e, "REST insert failed");
    }
  }

  private String generatePayload(IBatch batch) {
    return isTableMode() ? generateTablePayload(batch) : generateTreePayload(batch);
  }

  private String generateTreePayload(IBatch batch) {
    DeviceSchema schema = batch.getDeviceSchema();
    IoTDBRestPayload payload = new IoTDBRestPayload();
    payload.device = String.format("%s.%s", ROOT_SERIES_NAME, schema.getDevicePath());
    payload.is_aligned = config.isIS_SENSOR_TS_ALIGNMENT();

    List<String> measurements = new ArrayList<>();
    List<String> dataTypes = new ArrayList<>();
    for (Sensor sensor : schema.getSensors()) {
      measurements.add(sensor.getName());
      dataTypes.add(sensor.getSensorType().name);
    }
    payload.measurements = measurements;
    payload.data_types = dataTypes;

    List<Long> timestamps = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (Record record : batch.getRecords()) {
      timestamps.add(record.getTimestamp());
      List<Object> row = record.getRecordDataValue();
      for (int j = 0; j < row.size(); j++) {
        if (values.size() <= j) {
          values.add(new ArrayList<>());
        }
        values.get(j).add(row.get(j));
      }
    }
    payload.timestamps = timestamps;
    payload.values = values;

    return GSON.toJson(payload);
  }

  private String generateTablePayload(IBatch batch) {
    DeviceSchema schema = batch.getDeviceSchema();
    IoTDBTableRestPayload payload = new IoTDBTableRestPayload();
    payload.database = dbConfig.getDB_NAME() + "_" + schema.getGroup();
    payload.table = schema.getTable();

    for (Sensor sensor : schema.getSensors()) {
      payload.column_names.add(sensor.getName());
      payload.column_categories.add("FIELD");
      payload.data_types.add(sensor.getSensorType().name);
    }
    payload.column_names.add("device_id");
    payload.column_categories.add("TAG");
    payload.data_types.add("STRING");
    for (String key : schema.getTags().keySet()) {
      payload.column_names.add(key);
      payload.column_categories.add("TAG");
      payload.data_types.add("STRING");
    }

    for (Record record : batch.getRecords()) {
      payload.timestamps.add(record.getTimestamp());
      List<Object> row = new ArrayList<>(record.getRecordDataValue());
      row.add(schema.getDevice());
      for (String key : schema.getTags().keySet()) {
        row.add(schema.getTags().get(key));
      }
      payload.values.add(row);
    }
    return GSON.toJson(payload);
  }

  @Override
  protected Status executeQueryAndGetStatus(String sql, Operation operation) {
    return executeQueryAndGetStatus(sql);
  }

  private Status executeQueryAndGetStatus(String sql) {
    String json = String.format("{\"sql\":\"%s\"}", sql);
    String api = isTableMode() ? "/rest/table/v1/query" : "/rest/v2/query";
    Request request = constructRequest(api, json);
    try (Response response = client.newCall(request).execute()) {
      ResponseBody responseBody = response.body();
      if (responseBody == null) {
        throw new IOException("REST query response body is empty");
      }
      String body = responseBody.string();
      IoTDBRestQueryResult queryResult = GSON.fromJson(body, IoTDBRestQueryResult.class);
      if (queryResult == null) {
        throw new IOException("REST query response body is empty");
      }
      if (queryResult.timestamps == null && queryResult.values != null) {
        return new Status(true, queryResult.values.size());
      } else if (queryResult.timestamps == null && response.code() == 200) {
        // The aggregate query has no timestamps and only one result
        return new Status(true);
      } else {
        return new Status(true, queryResult.timestamps.size());
      }
    } catch (IOException | RuntimeException e) {
      LOGGER.warn("Execute query failed: {}", e.toString());
      return new Status(false, e, "REST query failed");
    }
  }

  private void executeNonQuery(String sql) {
    String api = isTableMode() ? "/rest/table/v1/nonQuery" : "/rest/v2/nonQuery";
    String json = GSON.toJson(new IoTDBRestSql(sql));
    Request request = constructRequest(api, json);
    try (Response response = client.newCall(request).execute()) {
      String body = response.body() == null ? "" : response.body().string();
      if (!isSuccessful(response, body)) {
        LOGGER.warn("Non-query failed: HTTP {}, body {}", response.code(), body);
      }
    } catch (IOException e) {
      LOGGER.warn("Non-query failed: {}", e.getMessage());
    }
  }

  private boolean isTableMode() {
    return config.getIoTDB_DIALECT_MODE() == SQLDialect.TABLE;
  }

  private boolean isSuccessful(Response response, String body) {
    if (!response.isSuccessful()) {
      return false;
    }
    try {
      IoTDBRestStatus status = GSON.fromJson(body, IoTDBRestStatus.class);
      return status != null && status.code == 200;
    } catch (RuntimeException e) {
      LOGGER.warn("Invalid REST response body: {}", body);
      return false;
    }
  }

  private static class IoTDBRestPayload {
    public String device;
    public boolean is_aligned;
    public List<List<Object>> values;
    public List<String> data_types;
    public List<String> measurements;
    public List<Long> timestamps;
  }

  private static class IoTDBRestQueryResult {
    public List<String> expressions;
    public List<String> column_names;
    public List<Long> timestamps;
    public List<List<Object>> values;
  }

  private static class IoTDBRestStatus {
    public int code;
  }

  private static class IoTDBRestSql {
    public final String sql;

    private IoTDBRestSql(String sql) {
      this.sql = sql;
    }
  }

  private static class IoTDBTableRestPayload {
    public final List<Long> timestamps = new ArrayList<>();
    public final List<String> column_names = new ArrayList<>();
    public final List<String> column_categories = new ArrayList<>();
    public final List<String> data_types = new ArrayList<>();
    public final List<List<Object>> values = new ArrayList<>();
    public String table;
    public String database;
  }
}
