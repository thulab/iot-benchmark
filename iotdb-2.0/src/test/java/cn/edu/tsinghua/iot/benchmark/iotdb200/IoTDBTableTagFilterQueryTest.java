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

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.iotdb200.ModelStrategy.TableStrategy;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.workload.GenerateQueryWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.query.TagFilter;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.SetOpQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTDBTableTagFilterQueryTest {

  static {
    System.setProperty(
        "benchmark-conf",
        Paths.get("..", "configuration", "conf").toAbsolutePath().normalize().toString());
  }

  private static final Config CONFIG = ConfigDescriptor.getInstance().getConfig();
  private static final String TAG_PREDICATE =
      "custom_tag_2 IN ('custom_value_0', 'custom_value_1', 'O''Brien')";

  private SQLDialect originalDialect;
  private BenchmarkMode originalBenchmarkMode;
  private String originalTimeColumn;
  private boolean originalWritableView;
  private boolean originalObjectQueryFullContent;
  private CapturingIoTDB database;
  private List<DeviceSchema> devices;

  @Before
  public void setUp() throws Exception {
    originalDialect = CONFIG.getIoTDB_DIALECT_MODE();
    originalBenchmarkMode = CONFIG.getBENCHMARK_WORK_MODE();
    originalTimeColumn = CONFIG.getTABLE_TIME_COLUMN();
    originalWritableView = CONFIG.isIoTDB_TABLE_WRITABLE_VIEW();
    originalObjectQueryFullContent = CONFIG.isOBJECT_QUERY_FULL_CONTENT();

    CONFIG.setIoTDB_DIALECT_MODE(SQLDialect.TABLE);
    CONFIG.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    CONFIG.setTABLE_TIME_COLUMN("time");
    CONFIG.setIoTDB_TABLE_WRITABLE_VIEW(false);
    CONFIG.setOBJECT_QUERY_FULL_CONTENT(false);

    database = new CapturingIoTDB(newDbConfig());
    devices = Arrays.asList(device("d_0", "custom_value_0"), device("d_1", "custom_value_1"));
  }

  @After
  public void tearDown() {
    CONFIG.setIoTDB_DIALECT_MODE(originalDialect);
    CONFIG.setBENCHMARK_WORK_MODE(originalBenchmarkMode);
    CONFIG.setTABLE_TIME_COLUMN(originalTimeColumn);
    CONFIG.setIoTDB_TABLE_WRITABLE_VIEW(originalWritableView);
    CONFIG.setOBJECT_QUERY_FULL_CONTENT(originalObjectQueryFullContent);
  }

  @Test
  public void allPerformanceQueriesUseTagFilterInsteadOfDeviceIds() {
    database.preciseQuery(filtered(new PreciseQuery(devices, 123)));
    assertUsesTagFilter(1);

    database.rangeQuery(filtered(new RangeQuery(devices, 100, 200)));
    assertUsesTagFilter(1);

    database.valueRangeQuery(filtered(new ValueRangeQuery(devices, 100, 200, 10)));
    assertUsesTagFilter(1);

    database.aggRangeQuery(filtered(new AggRangeQuery(devices, 100, 200, "count")));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("GROUP BY device_id"));

    database.aggValueQuery(filtered(new AggValueQuery(devices, "count", 10)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("GROUP BY device_id"));

    database.aggRangeValueQuery(filtered(new AggRangeValueQuery(devices, 100, 200, "count", 10)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("GROUP BY device_id"));

    database.groupByQuery(filtered(new GroupByQuery(devices, 100, 200, "count", 10)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("group by device_id"));

    database.latestPointQuery(filtered(new LatestPointQuery(devices, 100, 200, "last")));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("GROUP BY device_id"));

    database.rangeQueryOrderByDesc(filtered(new RangeQuery(devices, 100, 200)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("ORDER BY device_id, time desc"));

    database.valueRangeQueryOrderByDesc(filtered(new ValueRangeQuery(devices, 100, 200, 10)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("ORDER BY device_id, time desc"));

    database.groupByQueryOrderByDesc(filtered(new GroupByQuery(devices, 100, 200, "count", 10)));
    assertUsesTagFilter(1);
    assertTrue(database.getSql().contains("order by device_id"));

    RangeQuery firstChild = filtered(new RangeQuery(devices, 100, 200));
    RangeQuery secondChild = filtered(new RangeQuery(devices, 300, 400));
    database.setOpQuery(new SetOpQuery(Arrays.asList(firstChild, secondChild), "UNION ALL"));
    assertUsesTagFilter(2);
  }

  @Test
  public void queryWithoutTagFilterPreservesLegacyDevicePredicate() {
    database.preciseQuery(new PreciseQuery(devices, 123));

    assertEquals(
        "SELECT s_0, s_1 FROM benchmark_g_0.table_0 WHERE time = 123 AND  "
            + "(device_id = 'd_0' OR device_id = 'd_1')",
        database.getSql());
  }

  @Test
  public void tagFilterUsesEveryValueAndEscapesSqlStringLiterals() {
    database.rangeQuery(filtered(new RangeQuery(devices, 100, 200)));

    assertTrue(database.getSql().contains(TAG_PREDICATE));
  }

  @Test
  public void verificationQueryRemainsDeviceAndTagSpecific() {
    TableStrategy strategy = new TableStrategy(newDbConfig());
    StringBuffer sql = new StringBuffer("SELECT s_0, s_1 FROM benchmark_g_0.table_0");

    strategy.addVerificationQueryWhereClause(
        sql,
        Arrays.asList(new Record(123, Arrays.asList(1.0, 1L))),
        new HashMap<>(),
        devices.get(0));

    assertEquals(
        "SELECT s_0, s_1 FROM benchmark_g_0.table_0 WHERE (time = 123 ) "
            + "AND device_id = 'd_0' AND custom_tag_2 = 'custom_value_0'",
        sql.toString());
  }

  @Test
  public void generatedSqlOnlyUsesActualTagValuesFromItsTargetTable() throws WorkloadException {
    boolean originalTagFilterEnabled = CONFIG.isENABLE_QUERY_TAG_FILTER();
    boolean originalFixedQuery = CONFIG.isENABLE_FIXED_QUERY();
    int originalFirstDeviceIndex = CONFIG.getFIRST_DEVICE_INDEX();
    int originalDeviceNumber = CONFIG.getDEVICE_NUMBER();
    int originalTableNumber = CONFIG.getIoTDB_TABLE_NUMBER();
    String originalSgStrategy = CONFIG.getSG_STRATEGY();
    int originalTagNumber = CONFIG.getTAG_NUMBER();
    String originalTagKeyPrefix = CONFIG.getTAG_KEY_PREFIX();
    String originalTagValuePrefix = CONFIG.getTAG_VALUE_PREFIX();
    List<Integer> originalTagValueCardinality = new ArrayList<>(CONFIG.getTAG_VALUE_CARDINALITY());
    int originalQueryTagIndex = CONFIG.getQUERY_TAG_INDEX();
    int originalQueryTagValueNum = CONFIG.getQUERY_TAG_VALUE_NUM();
    long originalQuerySeed = CONFIG.getQUERY_SEED();
    double originalRealInsertRate = CONFIG.getREAL_INSERT_RATE();
    try {
      CONFIG.setENABLE_QUERY_TAG_FILTER(true);
      CONFIG.setENABLE_FIXED_QUERY(false);
      CONFIG.setFIRST_DEVICE_INDEX(12);
      CONFIG.setDEVICE_NUMBER(12);
      CONFIG.setIoTDB_TABLE_NUMBER(2);
      CONFIG.setSG_STRATEGY("mod");
      CONFIG.setTAG_NUMBER(1);
      CONFIG.setTAG_KEY_PREFIX("actual_tag_");
      CONFIG.setTAG_VALUE_PREFIX("actual_value_");
      CONFIG.setTAG_VALUE_CARDINALITY(Arrays.asList(12));
      CONFIG.setQUERY_TAG_INDEX(0);
      CONFIG.setQUERY_TAG_VALUE_NUM(0);
      CONFIG.setQUERY_SEED(20260805L);
      CONFIG.setREAL_INSERT_RATE(1.0);

      PreciseQuery query = new GenerateQueryWorkLoad(3).getPreciseQuery();
      database.preciseQuery(query);

      Set<String> actualValues = getActualTagValuesInTargetTable(query);
      assertEquals(actualValues.size(), query.getTagFilter().getTagValues().size());
      assertTrue(actualValues.containsAll(query.getTagFilter().getTagValues()));
      String expectedPredicate =
          "actual_tag_0 IN ("
              + query.getTagFilter().getTagValues().stream()
                  .map(value -> "'" + value + "'")
                  .collect(Collectors.joining(", "))
              + ")";
      assertTrue(database.getSql().contains(expectedPredicate));
      assertFalse(database.getSql().contains("device_id ="));
    } finally {
      CONFIG.setENABLE_QUERY_TAG_FILTER(originalTagFilterEnabled);
      CONFIG.setENABLE_FIXED_QUERY(originalFixedQuery);
      CONFIG.setFIRST_DEVICE_INDEX(originalFirstDeviceIndex);
      CONFIG.setDEVICE_NUMBER(originalDeviceNumber);
      CONFIG.setIoTDB_TABLE_NUMBER(originalTableNumber);
      CONFIG.setSG_STRATEGY(originalSgStrategy);
      CONFIG.setTAG_NUMBER(originalTagNumber);
      CONFIG.setTAG_KEY_PREFIX(originalTagKeyPrefix);
      CONFIG.setTAG_VALUE_PREFIX(originalTagValuePrefix);
      CONFIG.setTAG_VALUE_CARDINALITY(originalTagValueCardinality);
      CONFIG.setQUERY_TAG_INDEX(originalQueryTagIndex);
      CONFIG.setQUERY_TAG_VALUE_NUM(originalQueryTagValueNum);
      CONFIG.setQUERY_SEED(originalQuerySeed);
      CONFIG.setREAL_INSERT_RATE(originalRealInsertRate);
    }
  }

  private Set<String> getActualTagValuesInTargetTable(PreciseQuery query) throws WorkloadException {
    int targetTable =
        MetaUtil.mappingId(
            query.getDeviceSchema().get(0).getDeviceId(),
            CONFIG.getDEVICE_NUMBER(),
            CONFIG.getIoTDB_TABLE_NUMBER());
    Set<String> actualValues = new HashSet<>();
    int lastInsertedDeviceIndex =
        Math.min(
            CONFIG.getFIRST_DEVICE_INDEX() + CONFIG.getDEVICE_NUMBER() - 1,
            CONFIG.getFIRST_DEVICE_INDEX()
                + (int) (CONFIG.getDEVICE_NUMBER() * CONFIG.getREAL_INSERT_RATE()));
    for (int offset = 0; offset < CONFIG.getDEVICE_NUMBER(); offset++) {
      int deviceId = CONFIG.getFIRST_DEVICE_INDEX() + offset;
      if (deviceId > lastInsertedDeviceIndex) {
        continue;
      }
      if (MetaUtil.mappingId(deviceId, CONFIG.getDEVICE_NUMBER(), CONFIG.getIoTDB_TABLE_NUMBER())
          != targetTable) {
        continue;
      }
      actualValues.add(
          MetaUtil.getTags(
                  MetaUtil.getDeviceName(offset),
                  CONFIG.getTAG_NUMBER(),
                  CONFIG.getTAG_KEY_PREFIX(),
                  CONFIG.getTAG_VALUE_PREFIX(),
                  CONFIG.getTAG_VALUE_CARDINALITY())
              .get("actual_tag_0"));
    }
    return actualValues;
  }

  private void assertUsesTagFilter(int expectedOccurrences) {
    assertEquals(expectedOccurrences, occurrences(database.getSql(), TAG_PREDICATE));
    assertFalse(database.getSql().contains("device_id ="));
  }

  private int occurrences(String value, String target) {
    int count = 0;
    int offset = 0;
    while ((offset = value.indexOf(target, offset)) >= 0) {
      count++;
      offset += target.length();
    }
    return count;
  }

  private <T extends Query> T filtered(T query) {
    query.setTagFilter(
        new TagFilter(
            "custom_tag_2", Arrays.asList("custom_value_0", "custom_value_1", "O'Brien")));
    return query;
  }

  private DeviceSchema device(String deviceId, String tagValue) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("custom_tag_2", tagValue);
    return new DeviceSchema(
        "0",
        "0",
        deviceId,
        Arrays.asList(new Sensor("s_0", SensorType.DOUBLE), new Sensor("s_1", SensorType.INT64)),
        tags);
  }

  private DBConfig newDbConfig() {
    DBConfig dbConfig = new DBConfig();
    dbConfig.setDB_NAME("benchmark");
    return dbConfig;
  }

  private static class CapturingIoTDB extends IoTDB {

    private String sql;

    private CapturingIoTDB(DBConfig dbConfig) throws Exception {
      super(dbConfig, false);
    }

    @Override
    protected Status executeQueryAndGetStatus(String sql, Operation operation) {
      this.sql = sql;
      return new Status(true);
    }

    private String getSql() {
      return sql;
    }
  }
}
