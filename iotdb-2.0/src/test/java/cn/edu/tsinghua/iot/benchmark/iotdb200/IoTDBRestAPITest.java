/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb200;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTDBRestAPITest {
  static {
    System.setProperty(
        "benchmark-conf",
        Paths.get("..", "configuration", "conf").toAbsolutePath().normalize().toString());
  }

  private static final Config CONFIG = ConfigDescriptor.getInstance().getConfig();

  private MockWebServer server;
  private SQLDialect originalDialect;
  private int originalRestPort;
  private int originalWriteTimeout;
  private int originalGroupNumber;
  private String originalGroupPrefix;

  @Before
  public void setUp() throws Exception {
    originalDialect = CONFIG.getIoTDB_DIALECT_MODE();
    originalRestPort = CONFIG.getREST_PORT();
    originalWriteTimeout = CONFIG.getWRITE_OPERATION_TIMEOUT_MS();
    originalGroupNumber = CONFIG.getGROUP_NUMBER();
    originalGroupPrefix = CONFIG.getGROUP_NAME_PREFIX();

    server = new MockWebServer();
    server.start();
    CONFIG.setIoTDB_DIALECT_MODE(SQLDialect.TABLE);
    CONFIG.setREST_PORT(server.getPort());
    CONFIG.setWRITE_OPERATION_TIMEOUT_MS(4321);
    CONFIG.setGROUP_NUMBER(1);
    CONFIG.setGROUP_NAME_PREFIX("g_");
  }

  @After
  public void tearDown() throws Exception {
    CONFIG.setIoTDB_DIALECT_MODE(originalDialect);
    CONFIG.setREST_PORT(originalRestPort);
    CONFIG.setWRITE_OPERATION_TIMEOUT_MS(originalWriteTimeout);
    CONFIG.setGROUP_NUMBER(originalGroupNumber);
    CONFIG.setGROUP_NAME_PREFIX(originalGroupPrefix);
    server.shutdown();
  }

  @Test
  public void tableInsertUsesTableEndpointAndRowOrientedPayload() throws Exception {
    server.enqueue(jsonResponse(200));
    IoTDBRestAPI restAPI = newRestAPI();

    Status status = restAPI.insertOneBatch(newBatch());

    assertTrue(status.isOk());
    RecordedRequest request = server.takeRequest();
    assertEquals("/rest/table/v1/insertTablet", request.getPath());
    JsonObject payload = JsonParser.parseString(request.getBody().readUtf8()).getAsJsonObject();
    assertEquals("benchmark_g_0", payload.get("database").getAsString());
    assertEquals("table_0", payload.get("table").getAsString());
    assertEquals(
        Arrays.asList("s_0", "s_1", "device_id", "region"),
        strings(payload.getAsJsonArray("column_names")));
    assertEquals(
        Arrays.asList("FIELD", "FIELD", "TAG", "TAG"),
        strings(payload.getAsJsonArray("column_categories")));
    assertEquals(
        Arrays.asList("DOUBLE", "INT64", "STRING", "STRING"),
        strings(payload.getAsJsonArray("data_types")));
    assertEquals(2, payload.getAsJsonArray("values").size());
    assertEquals(
        "d_0", payload.getAsJsonArray("values").get(0).getAsJsonArray().get(2).getAsString());
    assertEquals(
        "beijing", payload.getAsJsonArray("values").get(0).getAsJsonArray().get(3).getAsString());
  }

  @Test
  public void businessErrorIsReportedAsFailureEvenWhenHttpSucceeds() throws Exception {
    server.enqueue(jsonResponse(500));

    Status status = newRestAPI().insertOneBatch(newBatch());

    assertFalse(status.isOk());
  }

  @Test
  public void tableCleanupOnlyDropsConfiguredBenchmarkDatabase() throws Exception {
    server.enqueue(jsonResponse(200));

    newRestAPI().cleanup();

    RecordedRequest request = server.takeRequest();
    assertEquals("/rest/table/v1/nonQuery", request.getPath());
    JsonObject payload = JsonParser.parseString(request.getBody().readUtf8()).getAsJsonObject();
    assertEquals("drop database if exists benchmark_g_0", payload.get("sql").getAsString());
  }

  @Test
  public void httpTimeoutUsesConfiguredWriteOperationTimeout() throws Exception {
    IoTDBRestAPI restAPI = newRestAPI();
    Field clientField = IoTDBRestAPI.class.getDeclaredField("client");
    clientField.setAccessible(true);
    OkHttpClient client = (OkHttpClient) clientField.get(restAPI);

    assertEquals(4321, client.connectTimeoutMillis());
    assertEquals(4321, client.readTimeoutMillis());
    assertEquals(4321, client.writeTimeoutMillis());
  }

  private IoTDBRestAPI newRestAPI() throws Exception {
    DBConfig dbConfig = new DBConfig();
    dbConfig.setDB_SWITCH(DBSwitch.DB_IOT_200_REST);
    dbConfig.setHOST(Collections.singletonList("127.0.0.1"));
    dbConfig.setPORT(Collections.singletonList("6667"));
    dbConfig.setDB_NAME("benchmark");
    return new IoTDBRestAPI(dbConfig, true);
  }

  private Batch newBatch() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("region", "beijing");
    DeviceSchema schema =
        new DeviceSchema(
            "0",
            "0",
            "d_0",
            Arrays.asList(
                new Sensor("s_0", SensorType.DOUBLE), new Sensor("s_1", SensorType.INT64)),
            tags);
    return new Batch(
        schema,
        Arrays.asList(
            new Record(1L, Arrays.asList(1.5D, 10L)), new Record(2L, Arrays.asList(2.5D, 20L))));
  }

  private MockResponse jsonResponse(int code) {
    return new MockResponse()
        .setResponseCode(200)
        .setHeader("Content-Type", "application/json")
        .setBody("{\"code\":" + code + ",\"message\":\"status\"}");
  }

  private java.util.List<String> strings(JsonArray array) {
    java.util.List<String> values = new java.util.ArrayList<>();
    array.forEach(value -> values.add(value.getAsString()));
    return values;
  }
}
