/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.workload;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.workload.query.TagFilter;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.SetOpQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GenerateQueryWorkLoadTagFilterTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private boolean originalEnableQueryTagFilter;
  private boolean originalEnableFixedQuery;
  private int originalTagNumber;
  private String originalTagKeyPrefix;
  private String originalTagValuePrefix;
  private List<Integer> originalTagValueCardinality;
  private int originalQueryTagIndex;
  private int originalQueryTagValueNum;
  private long originalQuerySeed;
  private int originalQuerySetOpNum;
  private int originalFirstDeviceIndex;
  private int originalDeviceNumber;
  private int originalTableNumber;
  private String originalSgStrategy;
  private SQLDialect originalDialect;
  private double originalRealInsertRate;

  @Before
  public void setUp() {
    originalEnableQueryTagFilter = config.isENABLE_QUERY_TAG_FILTER();
    originalEnableFixedQuery = config.isENABLE_FIXED_QUERY();
    originalTagNumber = config.getTAG_NUMBER();
    originalTagKeyPrefix = config.getTAG_KEY_PREFIX();
    originalTagValuePrefix = config.getTAG_VALUE_PREFIX();
    originalTagValueCardinality = new ArrayList<>(config.getTAG_VALUE_CARDINALITY());
    originalQueryTagIndex = config.getQUERY_TAG_INDEX();
    originalQueryTagValueNum = config.getQUERY_TAG_VALUE_NUM();
    originalQuerySeed = config.getQUERY_SEED();
    originalQuerySetOpNum = config.getQUERY_SET_OP_NUM();
    originalFirstDeviceIndex = config.getFIRST_DEVICE_INDEX();
    originalDeviceNumber = config.getDEVICE_NUMBER();
    originalTableNumber = config.getIoTDB_TABLE_NUMBER();
    originalSgStrategy = config.getSG_STRATEGY();
    originalDialect = config.getIoTDB_DIALECT_MODE();
    originalRealInsertRate = config.getREAL_INSERT_RATE();

    config.setENABLE_QUERY_TAG_FILTER(true);
    config.setENABLE_FIXED_QUERY(false);
    config.setFIRST_DEVICE_INDEX(0);
    config.setDEVICE_NUMBER(12);
    config.setIoTDB_TABLE_NUMBER(2);
    config.setSG_STRATEGY("mod");
    config.setIoTDB_DIALECT_MODE(SQLDialect.TABLE);
    config.setREAL_INSERT_RATE(1.0);
    config.setTAG_NUMBER(2);
    config.setTAG_KEY_PREFIX("query_tag_");
    config.setTAG_VALUE_PREFIX("query_value_");
    config.setTAG_VALUE_CARDINALITY(Arrays.asList(3, 12));
    config.setQUERY_TAG_INDEX(1);
    config.setQUERY_TAG_VALUE_NUM(4);
    config.setQUERY_SEED(934857L);
    config.setQUERY_SET_OP_NUM(3);
    GenerateQueryWorkLoad.resetQueryCachesForTest();
  }

  @After
  public void tearDown() {
    config.setENABLE_QUERY_TAG_FILTER(originalEnableQueryTagFilter);
    config.setENABLE_FIXED_QUERY(originalEnableFixedQuery);
    config.setTAG_NUMBER(originalTagNumber);
    config.setTAG_KEY_PREFIX(originalTagKeyPrefix);
    config.setTAG_VALUE_PREFIX(originalTagValuePrefix);
    config.setTAG_VALUE_CARDINALITY(originalTagValueCardinality);
    config.setQUERY_TAG_INDEX(originalQueryTagIndex);
    config.setQUERY_TAG_VALUE_NUM(originalQueryTagValueNum);
    config.setQUERY_SEED(originalQuerySeed);
    config.setQUERY_SET_OP_NUM(originalQuerySetOpNum);
    config.setFIRST_DEVICE_INDEX(originalFirstDeviceIndex);
    config.setDEVICE_NUMBER(originalDeviceNumber);
    config.setIoTDB_TABLE_NUMBER(originalTableNumber);
    config.setSG_STRATEGY(originalSgStrategy);
    config.setIoTDB_DIALECT_MODE(originalDialect);
    config.setREAL_INSERT_RATE(originalRealInsertRate);
    GenerateQueryWorkLoad.resetQueryCachesForTest();
  }

  @Test
  public void testEveryPerformanceQueryCarriesTagFilter() throws WorkloadException {
    GenerateQueryWorkLoad workLoad = new GenerateQueryWorkLoad(7);
    List<Query> queries =
        Arrays.asList(
            workLoad.getPreciseQuery(),
            workLoad.getRangeQuery(),
            workLoad.getValueRangeQuery(),
            workLoad.getAggRangeQuery(),
            workLoad.getAggValueQuery(),
            workLoad.getAggRangeValueQuery(),
            workLoad.getGroupByQuery(),
            workLoad.getLatestPointQuery());

    for (Query query : queries) {
      assertValidSample(query, 4);
    }

    SetOpQuery setOpQuery = workLoad.getSetOpQuery();
    assertNull(setOpQuery.getTagFilter());
    assertEquals(3, setOpQuery.getChildRangeQueries().size());
    for (RangeQuery child : setOpQuery.getChildRangeQueries()) {
      assertValidSample(child, 4);
    }
    assertNotEquals(
        setOpQuery.getChildRangeQueries().get(0).getTagFilter(),
        setOpQuery.getChildRangeQueries().get(1).getTagFilter());

    assertNull(workLoad.getVerifiedQuery(new Batch()).getTagFilter());
    assertNull(workLoad.getDeviceQuery().getTagFilter());
  }

  @Test
  public void testZeroSelectsEveryValueInStableOrder() throws WorkloadException {
    config.setQUERY_TAG_VALUE_NUM(0);
    PreciseQuery query = new GenerateQueryWorkLoad(3).getPreciseQuery();
    TagFilter tagFilter = query.getTagFilter();

    assertEquals("query_tag_1", tagFilter.getTagKey());
    List<String> actualTableValues = getActualTagValues(query.getDeviceSchema());
    assertEquals(actualTableValues, tagFilter.getTagValues());
    assertFalse(
        "a table-local maximum cardinality may be smaller than the configured global cardinality",
        tagFilter.getTagValues().size() == config.getTAG_VALUE_CARDINALITY().get(1));
  }

  @Test
  public void testTagRandomSequenceIsReproducible() throws WorkloadException {
    GenerateQueryWorkLoad first = new GenerateQueryWorkLoad(15);
    GenerateQueryWorkLoad second = new GenerateQueryWorkLoad(15);

    assertEquals(first.getRangeQuery().getTagFilter(), second.getRangeQuery().getTagFilter());
    assertEquals(first.getAggRangeQuery().getTagFilter(), second.getAggRangeQuery().getTagFilter());
  }

  @Test
  public void testZeroUsesOnlyValuesFromDevicesSelectedByRealInsertRate() throws WorkloadException {
    config.setREAL_INSERT_RATE(0.25);
    config.setQUERY_TAG_VALUE_NUM(0);
    GenerateQueryWorkLoad.resetQueryCachesForTest();

    PreciseQuery query = new GenerateQueryWorkLoad(20).getPreciseQuery();

    assertEquals(getActualTagValues(query.getDeviceSchema()), query.getTagFilter().getTagValues());
    assertEquals(2, query.getTagFilter().getTagValues().size());
  }

  @Test
  public void testSmallSampleDoesNotMaterializeLargeCardinality() throws WorkloadException {
    config.setTAG_VALUE_CARDINALITY(Arrays.asList(3, 1_000_000_000));

    PreciseQuery query = new GenerateQueryWorkLoad(21).getPreciseQuery();

    assertValidSample(query, 4);
  }

  @Test
  public void testRejectsCountLargerThanEveryTablesActualCardinality() {
    config.setDEVICE_NUMBER(4);
    config.setTAG_VALUE_CARDINALITY(Arrays.asList(3, 4));
    config.setQUERY_TAG_VALUE_NUM(3);
    GenerateQueryWorkLoad.resetQueryCachesForTest();

    WorkloadException exception =
        assertThrows(
            WorkloadException.class, () -> new GenerateQueryWorkLoad(22).getPreciseQuery());

    assertTrue(exception.getMessage().contains("No table has at least 3 actual values"));
  }

  @Test
  public void testFixedFiltersAreStableAcrossCallsAndWorkloads() throws WorkloadException {
    config.setENABLE_FIXED_QUERY(true);
    GenerateQueryWorkLoad first = new GenerateQueryWorkLoad(1);
    GenerateQueryWorkLoad second = new GenerateQueryWorkLoad(99);

    TagFilter firstFilter = first.getPreciseQuery().getTagFilter();
    assertSame(firstFilter, first.getRangeQuery().getTagFilter());
    assertEquals(firstFilter, second.getPreciseQuery().getTagFilter());

    SetOpQuery firstSetOp = first.getSetOpQuery();
    SetOpQuery repeatedSetOp = first.getSetOpQuery();
    SetOpQuery otherWorkloadSetOp = second.getSetOpQuery();
    assertNull(firstSetOp.getTagFilter());
    for (int i = 0; i < firstSetOp.getChildRangeQueries().size(); i++) {
      TagFilter childFilter = firstSetOp.getChildRangeQueries().get(i).getTagFilter();
      assertEquals(childFilter, repeatedSetOp.getChildRangeQueries().get(i).getTagFilter());
      assertEquals(childFilter, otherWorkloadSetOp.getChildRangeQueries().get(i).getTagFilter());
      assertValidSample(firstSetOp.getChildRangeQueries().get(i), 4);
    }
    assertNotEquals(
        firstSetOp.getChildRangeQueries().get(0).getTagFilter(),
        firstSetOp.getChildRangeQueries().get(1).getTagFilter());
  }

  @Test
  public void testDisabledFilterPreservesExistingQueryShape() throws WorkloadException {
    config.setENABLE_QUERY_TAG_FILTER(false);
    GenerateQueryWorkLoad workLoad = new GenerateQueryWorkLoad(8);

    assertNull(workLoad.getPreciseQuery().getTagFilter());
    SetOpQuery setOpQuery = workLoad.getSetOpQuery();
    assertNull(setOpQuery.getTagFilter());
    setOpQuery.getChildRangeQueries().forEach(child -> assertNull(child.getTagFilter()));
  }

  private static void assertValidSample(Query query, int expectedSize) throws WorkloadException {
    TagFilter tagFilter = query.getTagFilter();
    assertNotNull(tagFilter);
    assertEquals("query_tag_1", tagFilter.getTagKey());
    assertEquals(expectedSize, tagFilter.getTagValues().size());
    assertEquals(expectedSize, new HashSet<>(tagFilter.getTagValues()).size());
    assertTrue(getActualTagValues(getDeviceSchemas(query)).containsAll(tagFilter.getTagValues()));

    int previousIndex = -1;
    for (String tagValue : tagFilter.getTagValues()) {
      int valueIndex = Integer.parseInt(tagValue.substring("query_value_".length()));
      assertFalse(valueIndex <= previousIndex);
      previousIndex = valueIndex;
    }
  }

  private static List<DeviceSchema> getDeviceSchemas(Query query) {
    if (query instanceof PreciseQuery) {
      return ((PreciseQuery) query).getDeviceSchema();
    }
    return ((RangeQuery) query).getDeviceSchema();
  }

  private static List<String> getActualTagValues(List<DeviceSchema> queryDevices)
      throws WorkloadException {
    int targetTable =
        MetaUtil.mappingId(
            queryDevices.get(0).getDeviceId(),
            config.getDEVICE_NUMBER(),
            config.getIoTDB_TABLE_NUMBER());
    Map<Long, String> indexedValues = new LinkedHashMap<>();
    int lastInsertedDeviceIndex =
        Math.min(
            config.getFIRST_DEVICE_INDEX() + config.getDEVICE_NUMBER() - 1,
            config.getFIRST_DEVICE_INDEX()
                + (int) (config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE()));
    for (int offset = 0; offset < config.getDEVICE_NUMBER(); offset++) {
      int deviceId = config.getFIRST_DEVICE_INDEX() + offset;
      if (deviceId > lastInsertedDeviceIndex) {
        continue;
      }
      if (MetaUtil.mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER())
          != targetTable) {
        continue;
      }
      String tagValue =
          MetaUtil.getTags(
                  MetaUtil.getDeviceName(offset),
                  config.getTAG_NUMBER(),
                  config.getTAG_KEY_PREFIX(),
                  config.getTAG_VALUE_PREFIX(),
                  config.getTAG_VALUE_CARDINALITY())
              .get(config.getTAG_KEY_PREFIX() + config.getQUERY_TAG_INDEX());
      long valueIndex = Long.parseLong(tagValue.substring(config.getTAG_VALUE_PREFIX().length()));
      indexedValues.put(valueIndex, tagValue);
    }
    List<Long> sortedIndexes = new ArrayList<>(indexedValues.keySet());
    sortedIndexes.sort(Long::compareTo);
    List<String> sortedValues = new ArrayList<>();
    for (long valueIndex : sortedIndexes) {
      sortedValues.add(indexedValues.get(valueIndex));
    }
    return sortedValues;
  }
}
