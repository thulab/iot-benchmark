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

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.TagFilter;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.SetOpQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GenerateQueryWorkLoad extends QueryWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateQueryWorkLoad.class);
  private static final long QUERY_TAG_RANDOM_SEED_SALT = 0x6A09E667F3BCC909L;
  private static final long QUERY_TAG_RANDOM_ID_FACTOR = 0x9E3779B97F4A7C15L;
  private static final long SET_OP_TAG_RANDOM_SEED_SALT = 0xBB67AE8584CAA73BL;

  private static volatile List<DeviceSchema> cachedQueryDeviceSchemaListTypeAllow = null;
  private static volatile List<DeviceSchema> cachedQueryDeviceSchemaListTypeFiltered = null;
  private static final Map<Integer, List<DeviceSchema>> cachedSetOpQueryDeviceSchemas =
      new ConcurrentHashMap<>();
  private static volatile QueryTagMetadata cachedQueryTagMetadata = null;

  private final Random queryDeviceRandom;
  private final Random querySensorRandom;
  private final Random queryTagRandom;
  private final Map<Integer, TagFilter> fixedQueryTagFilters = new HashMap<>();
  private static final long timeStampConst =
      TimeUtils.getTimestampConst(config.getTIMESTAMP_PRECISION());
  private static final AtomicInteger nowDeviceId =
      new AtomicInteger(config.getFIRST_DEVICE_INDEX());
  private Long currentWriteTimestamp = null;
  private final Map<Operation, AtomicLong> operationLoops = new ConcurrentHashMap<>();
  private static volatile Map<Integer, List<Integer>> tableDeviceMap = initTableDeviceMap();

  public GenerateQueryWorkLoad(int id) {
    super(id);
    this.queryDeviceRandom = new Random(config.getQUERY_SEED() + id);
    this.querySensorRandom = new Random(config.getQUERY_SEED() + id);
    this.queryTagRandom = new Random(getQueryTagRandomSeed(config.getQUERY_SEED(), id));
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, new AtomicLong(0L));
    }
  }

  @Override
  public PreciseQuery getPreciseQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(true, config.isENABLE_FIXED_QUERY());
    long timestamp = getQueryStartTimestamp(Operation.PRECISE_QUERY);
    return attachQueryTagFilter(new PreciseQuery(queryDevices, timestamp), queryDevices);
  }

  public List<DeviceSchema> getQueryDeviceSchema(boolean typeAllow, boolean fixedSQl)
      throws WorkloadException {
    if (!fixedSQl) {
      return getQueryDeviceSchemaList(typeAllow);
    }
    if (typeAllow) {
      if (cachedQueryDeviceSchemaListTypeAllow == null) {
        synchronized (GenerateQueryWorkLoad.class) {
          if (cachedQueryDeviceSchemaListTypeAllow == null) {
            cachedQueryDeviceSchemaListTypeAllow = getQueryDeviceSchemaList(typeAllow);
          }
        }
      }
      return cachedQueryDeviceSchemaListTypeAllow;
    } else {
      if (cachedQueryDeviceSchemaListTypeFiltered == null) {
        synchronized (GenerateQueryWorkLoad.class) {
          if (cachedQueryDeviceSchemaListTypeFiltered == null) {
            cachedQueryDeviceSchemaListTypeFiltered = getQueryDeviceSchemaList(typeAllow);
          }
        }
      }
      return cachedQueryDeviceSchemaListTypeFiltered;
    }
  }

  @Override
  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(true, config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new RangeQuery(queryDevices, startTimestamp, endTimestamp), queryDevices);
  }

  /**
   * The setOpQuery(union, intersect, except) has left and right range, which have same time
   * attributes and different device range Every child's device in the deviceSchema should be
   * different, and number of column among every child query should be the same, and the data type
   * should be compatible
   */
  @Override
  public SetOpQuery getSetOpQuery() throws WorkloadException {

    long startTimestamp = getQueryStartTimestamp(Operation.RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();

    List<RangeQuery> childRangeQueries = new ArrayList<>();
    int querySetOpNum = config.getQUERY_SET_OP_NUM();
    if (querySetOpNum < config.getQUERY_SET_LEAST_OP_NUM()) {
      throw new IllegalArgumentException(
          "the number of child set in set operation must be greater than or equal to 2");
    }

    List<DeviceSchema> firstQueryDeviceSchema =
        getQueryDeviceSchema(true, config.isENABLE_FIXED_QUERY());
    DeviceSchema firstDeviceSchema = firstQueryDeviceSchema.get(0);

    for (int i = 0; i < querySetOpNum; i++) {

      List<DeviceSchema> copiedDeviceSchema = new ArrayList<>();
      List<DeviceSchema> childQueryDevices = getSetOpQueryDeviceSchema(i);
      for (DeviceSchema childDeviceSchema : childQueryDevices) {
        copiedDeviceSchema.add(
            new DeviceSchema(
                childDeviceSchema.getDeviceId(),
                firstDeviceSchema.getSensors(),
                childDeviceSchema.getTags()));
      }

      RangeQuery childRangeQuery = new RangeQuery(copiedDeviceSchema, startTimestamp, endTimestamp);
      childRangeQuery.setTagFilter(getSetOpChildTagFilter(i, copiedDeviceSchema));
      childRangeQueries.add(childRangeQuery);
    }

    return new SetOpQuery(childRangeQueries, config.getQUERY_SET_OP_TYPE());
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(false, config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.VALUE_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new ValueRangeQuery(
            queryDevices, startTimestamp, endTimestamp, config.getQUERY_LOWER_VALUE()),
        queryDevices);
  }

  @Override
  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices =
        getQueryDeviceSchema(
            config.getQUERY_AGGREGATE_FUN().startsWith("count"), config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.AGG_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new AggRangeQuery(
            queryDevices, startTimestamp, endTimestamp, config.getQUERY_AGGREGATE_FUN()),
        queryDevices);
  }

  @Override
  public AggValueQuery getAggValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(false, config.isENABLE_FIXED_QUERY());
    return attachQueryTagFilter(
        new AggValueQuery(
            queryDevices, config.getQUERY_AGGREGATE_FUN(), config.getQUERY_LOWER_VALUE()),
        queryDevices);
  }

  @Override
  public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(false, config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.AGG_RANGE_VALUE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new AggRangeValueQuery(
            queryDevices,
            startTimestamp,
            endTimestamp,
            config.getQUERY_AGGREGATE_FUN(),
            config.getQUERY_LOWER_VALUE()),
        queryDevices);
  }

  @Override
  public GroupByQuery getGroupByQuery() throws WorkloadException {
    boolean typeAllow = false;
    if (config.getQUERY_AGGREGATE_FUN().contains("count")) {
      typeAllow = true;
    }
    if (config.getDbConfig().getDB_SWITCH() == DBSwitch.DB_INFLUX_2
        || (config.isIS_DOUBLE_WRITE()
            && config.getANOTHER_DBConfig().getDB_SWITCH() == DBSwitch.DB_INFLUX_2)) {
      typeAllow = false;
    }
    List<DeviceSchema> queryDevices =
        getQueryDeviceSchema(typeAllow, config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.GROUP_BY_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new GroupByQuery(
            queryDevices,
            startTimestamp,
            endTimestamp,
            config.getQUERY_AGGREGATE_FUN(),
            config.getGROUP_BY_TIME_UNIT()),
        queryDevices);
  }

  @Override
  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchema(true, config.isENABLE_FIXED_QUERY());
    long startTimestamp = getQueryStartTimestamp(Operation.LATEST_POINT_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return attachQueryTagFilter(
        new LatestPointQuery(
            queryDevices, startTimestamp, endTimestamp, config.getQUERY_AGGREGATE_FUN()),
        queryDevices);
  }

  @Override
  public VerificationQuery getVerifiedQuery(IBatch batch) throws WorkloadException {
    return new VerificationQuery(batch);
  }

  @Override
  public DeviceQuery getDeviceQuery() {
    Integer deviceId = nowDeviceId.getAndIncrement();
    if (deviceId >= config.getFIRST_DEVICE_INDEX() + config.getDEVICE_NUMBER()) {
      return null;
    }
    DeviceSchema deviceSchema =
        new DeviceSchema(deviceId, config.getSENSORS(), MetaUtil.getTags(deviceId));
    return new DeviceQuery(deviceSchema);
  }

  @Override
  public void updateTime(long currentTimestamp) {
    this.currentWriteTimestamp = currentTimestamp;
  }

  private List<DeviceSchema> getSetOpQueryDeviceSchema(int childIndex) throws WorkloadException {
    if (!config.isENABLE_FIXED_QUERY()) {
      return getQueryDeviceSchema(true, false);
    }
    List<DeviceSchema> cachedDeviceSchemas = cachedSetOpQueryDeviceSchemas.get(childIndex);
    if (cachedDeviceSchemas != null) {
      return cachedDeviceSchemas;
    }
    synchronized (GenerateQueryWorkLoad.class) {
      cachedDeviceSchemas = cachedSetOpQueryDeviceSchemas.get(childIndex);
      if (cachedDeviceSchemas == null) {
        cachedDeviceSchemas = getQueryDeviceSchema(true, false);
        cachedSetOpQueryDeviceSchemas.put(childIndex, cachedDeviceSchemas);
      }
    }
    return cachedDeviceSchemas;
  }

  private <T extends Query> T attachQueryTagFilter(T query, List<DeviceSchema> queryDevices)
      throws WorkloadException {
    query.setTagFilter(getQueryTagFilter(queryDevices));
    return query;
  }

  private TagFilter getQueryTagFilter(List<DeviceSchema> queryDevices) throws WorkloadException {
    if (!config.isENABLE_QUERY_TAG_FILTER()) {
      return null;
    }
    if (!config.isENABLE_FIXED_QUERY()) {
      return createQueryTagFilter(queryTagRandom, queryDevices);
    }
    int tableId = getQueryTableId(queryDevices);
    TagFilter fixedQueryTagFilter = fixedQueryTagFilters.get(tableId);
    if (fixedQueryTagFilter == null) {
      long tableSeed =
          getQueryTagRandomSeed(config.getQUERY_SEED(), 0)
              ^ (Integer.toUnsignedLong(tableId) * QUERY_TAG_RANDOM_ID_FACTOR);
      fixedQueryTagFilter = createQueryTagFilter(new Random(tableSeed), queryDevices);
      fixedQueryTagFilters.put(tableId, fixedQueryTagFilter);
    }
    return fixedQueryTagFilter;
  }

  private TagFilter getSetOpChildTagFilter(int childIndex, List<DeviceSchema> childQueryDevices)
      throws WorkloadException {
    if (!config.isENABLE_QUERY_TAG_FILTER()) {
      return null;
    }
    if (!config.isENABLE_FIXED_QUERY()) {
      return createQueryTagFilter(queryTagRandom, childQueryDevices);
    }
    int tableId = getQueryTableId(childQueryDevices);
    long childSeed =
        getQueryTagRandomSeed(config.getQUERY_SEED(), childIndex)
            ^ SET_OP_TAG_RANDOM_SEED_SALT
            ^ (Integer.toUnsignedLong(tableId) * QUERY_TAG_RANDOM_ID_FACTOR);
    return createQueryTagFilter(new Random(childSeed), childQueryDevices);
  }

  private TagFilter createQueryTagFilter(Random random, List<DeviceSchema> queryDevices)
      throws WorkloadException {
    int tagIndex = config.getQUERY_TAG_INDEX();
    List<Integer> tagValueCardinality = config.getTAG_VALUE_CARDINALITY();
    if (tagIndex < 0 || tagIndex >= tagValueCardinality.size()) {
      throw new WorkloadException(
          "QUERY_TAG_INDEX must have a corresponding TAG_VALUE_CARDINALITY entry");
    }
    int cardinality = tagValueCardinality.get(tagIndex);
    int configuredValueCount = config.getQUERY_TAG_VALUE_NUM();
    if (cardinality <= 0 || configuredValueCount < 0 || configuredValueCount > cardinality) {
      throw new WorkloadException(
          "QUERY_TAG_VALUE_NUM must be between 0 and the selected positive tag cardinality");
    }

    int tableId = getQueryTableId(queryDevices);
    List<String> availableTagValues = getQueryTagMetadata().getTagValuesByTable().get(tableId);
    if (availableTagValues == null || availableTagValues.isEmpty()) {
      throw new WorkloadException(
          "No actual values of the selected tag exist in query table " + tableId);
    }
    int selectedValueCount =
        configuredValueCount == 0 ? availableTagValues.size() : configuredValueCount;
    if (selectedValueCount > availableTagValues.size()) {
      throw new WorkloadException(
          String.format(
              Locale.ROOT,
              "QUERY_TAG_VALUE_NUM is %d, but query table %d only has %d actual values for tag %s",
              selectedValueCount,
              tableId,
              availableTagValues.size(),
              config.getTAG_KEY_PREFIX() + tagIndex));
    }

    List<Integer> valueIndexes;
    if (selectedValueCount == availableTagValues.size()) {
      valueIndexes = new ArrayList<>(selectedValueCount);
      for (int valueIndex = 0; valueIndex < selectedValueCount; valueIndex++) {
        valueIndexes.add(valueIndex);
      }
    } else {
      // Floyd's algorithm samples k unique indexes in O(k) space and time. This avoids allocating
      // and shuffling the complete tag domain when only a small IN-list is requested.
      Set<Integer> selectedIndexes = new HashSet<>(selectedValueCount);
      for (int candidate = availableTagValues.size() - selectedValueCount;
          candidate < availableTagValues.size();
          candidate++) {
        int selected = random.nextInt(candidate + 1);
        if (!selectedIndexes.add(selected)) {
          selectedIndexes.add(candidate);
        }
      }
      valueIndexes = new ArrayList<>(selectedIndexes);
      Collections.sort(valueIndexes);
    }

    List<String> tagValues = new ArrayList<>(selectedValueCount);
    for (int valueIndex : valueIndexes) {
      tagValues.add(availableTagValues.get(valueIndex));
    }
    return new TagFilter(config.getTAG_KEY_PREFIX() + tagIndex, tagValues);
  }

  private int getQueryTableId(List<DeviceSchema> queryDevices) throws WorkloadException {
    if (queryDevices == null || queryDevices.isEmpty()) {
      throw new WorkloadException("Query devices must not be empty when tag filtering is enabled");
    }
    int tableId =
        MetaUtil.mappingId(
            queryDevices.get(0).getDeviceId(),
            config.getDEVICE_NUMBER(),
            config.getIoTDB_TABLE_NUMBER());
    for (int i = 1; i < queryDevices.size(); i++) {
      int currentTableId =
          MetaUtil.mappingId(
              queryDevices.get(i).getDeviceId(),
              config.getDEVICE_NUMBER(),
              config.getIoTDB_TABLE_NUMBER());
      if (currentTableId != tableId) {
        throw new WorkloadException("A tag-filter query must target exactly one table");
      }
    }
    return tableId;
  }

  private static long getQueryTagRandomSeed(long querySeed, int queryWorkLoadId) {
    return querySeed
        ^ QUERY_TAG_RANDOM_SEED_SALT
        ^ (Integer.toUnsignedLong(queryWorkLoadId) * QUERY_TAG_RANDOM_ID_FACTOR);
  }

  private long getQueryStartTimestamp(Operation operation) {
    if (currentWriteTimestamp != null) {
      if (operation == Operation.PRECISE_QUERY) {
        return currentWriteTimestamp;
      } else {
        return currentWriteTimestamp >= config.getQUERY_INTERVAL()
            ? currentWriteTimestamp - config.getQUERY_INTERVAL()
            : 0;
      }
    }
    long currentQueryLoop = operationLoops.get(operation).getAndIncrement();
    long timestampOffset = 0;
    if (!config.isENABLE_FIXED_QUERY()) {
      timestampOffset = currentQueryLoop * config.getSTEP_SIZE() * config.getPOINT_STEP();
    }
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
  }

  /**
   * Return the list of deviceSchema.
   *
   * <p>TODO When multi-table query is supported, there is no need to.
   *
   * @param typeAllow true: allow generating bool and text type.
   */
  private List<DeviceSchema> getQueryDeviceSchemaList(boolean typeAllow) throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> queryDeviceIds = new ArrayList<>();
    List<Sensor> sensors = config.getSENSORS();
    int deviceId;
    List<Integer> devices;
    int deviceQueryMaxCount;
    boolean devicesUseAbsoluteIds;
    if (config.isENABLE_QUERY_TAG_FILTER()) {
      QueryTagMetadata queryTagMetadata = getQueryTagMetadata();
      List<Integer> eligibleTableIds = getEligibleTagFilterTableIds(queryTagMetadata);
      if (eligibleTableIds.isEmpty()) {
        throw new WorkloadException(
            String.format(
                Locale.ROOT,
                "No table has at least %d actual values for tag %s",
                config.getQUERY_TAG_VALUE_NUM(),
                config.getTAG_KEY_PREFIX() + config.getQUERY_TAG_INDEX()));
      }
      int tableId = eligibleTableIds.get(queryDeviceRandom.nextInt(eligibleTableIds.size()));
      devices = queryTagMetadata.getDeviceIdsByTable().get(tableId);
      deviceQueryMaxCount = devices.size();
      devicesUseAbsoluteIds = true;
    } else {
      deviceId =
          queryDeviceRandom.nextInt(config.getDEVICE_NUMBER()) + config.getFIRST_DEVICE_INDEX();
      int tableId =
          MetaUtil.mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
      devices = tableDeviceMap.get(tableId);
      deviceQueryMaxCount =
          (config.getIoTDB_DIALECT_MODE() == SQLDialect.TABLE)
              ? config.getDEVICE_NUMBER()
                  / Math.min(config.getIoTDB_TABLE_NUMBER(), config.getDEVICE_NUMBER())
              : config.getDEVICE_NUMBER();
      devicesUseAbsoluteIds = false;
    }
    while (queryDevices.size() < Math.min(deviceQueryMaxCount, config.getQUERY_DEVICE_NUM())
        && queryDeviceIds.size() < deviceQueryMaxCount) {
      // get a device belong to [first_device_index, first_device_index + device_number)
      deviceId = devices.get(queryDeviceRandom.nextInt(devices.size()));
      if (!devicesUseAbsoluteIds) {
        deviceId += config.getFIRST_DEVICE_INDEX();
      }
      // avoid duplicate
      if (!queryDeviceIds.contains(deviceId)) {
        queryDeviceIds.add(deviceId);
      } else {
        continue;
      }
      List<Sensor> querySensors = new ArrayList<>();
      List<Integer> querySensorIds = new ArrayList<>();
      while (querySensors.size() < Math.min(config.getSENSOR_NUMBER(), config.getQUERY_SENSOR_NUM())
          && querySensorIds.size() < config.getSENSOR_NUMBER()) {
        // getSensor belong to [0, sensor_number)
        int sensorId = querySensorRandom.nextInt(config.getSENSOR_NUMBER());
        // avoid duplicate
        if (!querySensorIds.contains(sensorId)) {
          querySensorIds.add(sensorId);
        } else {
          continue;
        }
        Sensor sensor = sensors.get(sensorId);
        if (!typeAllow) {
          SensorType sensorType = sensor.getSensorType();
          if (sensorType == SensorType.BOOLEAN
              || sensorType == SensorType.TEXT
              || sensorType == SensorType.STRING
              || sensorType == SensorType.BLOB) {
            continue;
          }
        }
        querySensors.add(sensor);
      }
      if (querySensors.size() != config.getQUERY_SENSOR_NUM()) {
        continue;
      }
      DeviceSchema deviceSchema =
          new DeviceSchema(deviceId, querySensors, getConfiguredTags(deviceId));
      queryDevices.add(deviceSchema);
    }
    if (queryDevices.size() != Math.min(deviceQueryMaxCount, config.getQUERY_DEVICE_NUM())) {
      LOGGER.warn("There is no suitable sensor for query, please check INSERT_DATATYPE_PROPORTION");
      throw new WorkloadException(
          "There is no suitable sensor for query, please check INSERT_DATATYPE_PROPORTION");
    }
    return queryDevices;
  }

  private List<Integer> getEligibleTagFilterTableIds(QueryTagMetadata queryTagMetadata) {
    int configuredValueCount = config.getQUERY_TAG_VALUE_NUM();
    List<Integer> eligibleTableIds = new ArrayList<>();
    for (Map.Entry<Integer, List<String>> entry :
        queryTagMetadata.getTagValuesByTable().entrySet()) {
      int actualValueCount = entry.getValue().size();
      if (actualValueCount > 0
          && (configuredValueCount == 0 || actualValueCount >= configuredValueCount)) {
        eligibleTableIds.add(entry.getKey());
      }
    }
    return eligibleTableIds;
  }

  private static Map<String, String> getConfiguredTags(int deviceId) {
    int tagSourceDeviceId = deviceId - config.getFIRST_DEVICE_INDEX();
    return MetaUtil.getTags(
        MetaUtil.getDeviceName(tagSourceDeviceId),
        config.getTAG_NUMBER(),
        config.getTAG_KEY_PREFIX(),
        config.getTAG_VALUE_PREFIX(),
        config.getTAG_VALUE_CARDINALITY());
  }

  private static QueryTagMetadata getQueryTagMetadata() throws WorkloadException {
    String metadataKey = getQueryTagMetadataKey();
    QueryTagMetadata queryTagMetadata = cachedQueryTagMetadata;
    if (queryTagMetadata != null && queryTagMetadata.getMetadataKey().equals(metadataKey)) {
      return queryTagMetadata;
    }
    synchronized (GenerateQueryWorkLoad.class) {
      queryTagMetadata = cachedQueryTagMetadata;
      if (queryTagMetadata == null || !queryTagMetadata.getMetadataKey().equals(metadataKey)) {
        queryTagMetadata = buildQueryTagMetadata(metadataKey);
        cachedQueryTagMetadata = queryTagMetadata;
      }
    }
    return queryTagMetadata;
  }

  private static QueryTagMetadata buildQueryTagMetadata(String metadataKey)
      throws WorkloadException {
    Map<Integer, List<Integer>> deviceIdsByTable = new TreeMap<>();
    Map<Integer, SortedMap<Long, String>> indexedTagValuesByTable = new TreeMap<>();
    String tagValuePrefix = config.getTAG_VALUE_PREFIX();
    int tagIndex = config.getQUERY_TAG_INDEX();
    int firstDeviceIndex = config.getFIRST_DEVICE_INDEX();
    int lastInsertedDeviceOffset =
        Math.min(
            config.getDEVICE_NUMBER() - 1,
            (int) (config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE()));
    for (int deviceOffset = 0; deviceOffset <= lastInsertedDeviceOffset; deviceOffset++) {
      int deviceId = firstDeviceIndex + deviceOffset;
      int tableId =
          MetaUtil.mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
      deviceIdsByTable.computeIfAbsent(tableId, key -> new ArrayList<>()).add(deviceId);

      String tagValue;
      try {
        tagValue =
            MetaUtil.getTagValue(
                MetaUtil.getDeviceName(deviceOffset),
                tagIndex,
                tagValuePrefix,
                config.getTAG_VALUE_CARDINALITY());
      } catch (IllegalArgumentException e) {
        throw new WorkloadException("Failed to calculate actual tag values", e);
      }
      long tagValueIndex;
      try {
        tagValueIndex = Long.parseLong(tagValue.substring(tagValuePrefix.length()));
      } catch (NumberFormatException e) {
        throw new WorkloadException("Invalid generated tag value " + tagValue, e);
      }
      indexedTagValuesByTable
          .computeIfAbsent(tableId, key -> new TreeMap<>())
          .put(tagValueIndex, tagValue);
    }

    Map<Integer, List<Integer>> immutableDeviceIdsByTable = new LinkedHashMap<>();
    for (Map.Entry<Integer, List<Integer>> entry : deviceIdsByTable.entrySet()) {
      immutableDeviceIdsByTable.put(
          entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    Map<Integer, List<String>> immutableTagValuesByTable = new LinkedHashMap<>();
    for (Map.Entry<Integer, SortedMap<Long, String>> entry : indexedTagValuesByTable.entrySet()) {
      immutableTagValuesByTable.put(
          entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue().values())));
    }
    return new QueryTagMetadata(
        metadataKey,
        Collections.unmodifiableMap(immutableDeviceIdsByTable),
        Collections.unmodifiableMap(immutableTagValuesByTable));
  }

  private static String getQueryTagMetadataKey() {
    return config.getFIRST_DEVICE_INDEX()
        + "|"
        + config.getDEVICE_NUMBER()
        + "|"
        + config.getREAL_INSERT_RATE()
        + "|"
        + config.getIoTDB_TABLE_NUMBER()
        + "|"
        + config.getSG_STRATEGY()
        + "|"
        + config.getTAG_NUMBER()
        + "|"
        + config.getTAG_KEY_PREFIX()
        + "|"
        + config.getTAG_VALUE_PREFIX()
        + "|"
        + config.getTAG_VALUE_CARDINALITY()
        + "|"
        + config.getQUERY_TAG_INDEX();
  }

  static synchronized void resetQueryCachesForTest() {
    cachedQueryDeviceSchemaListTypeAllow = null;
    cachedQueryDeviceSchemaListTypeFiltered = null;
    cachedSetOpQueryDeviceSchemas.clear();
    cachedQueryTagMetadata = null;
    tableDeviceMap = initTableDeviceMap();
  }

  private static final class QueryTagMetadata {

    private final String metadataKey;
    private final Map<Integer, List<Integer>> deviceIdsByTable;
    private final Map<Integer, List<String>> tagValuesByTable;

    private QueryTagMetadata(
        String metadataKey,
        Map<Integer, List<Integer>> deviceIdsByTable,
        Map<Integer, List<String>> tagValuesByTable) {
      this.metadataKey = metadataKey;
      this.deviceIdsByTable = deviceIdsByTable;
      this.tagValuesByTable = tagValuesByTable;
    }

    private String getMetadataKey() {
      return metadataKey;
    }

    private Map<Integer, List<Integer>> getDeviceIdsByTable() {
      return deviceIdsByTable;
    }

    private Map<Integer, List<String>> getTagValuesByTable() {
      return tagValuesByTable;
    }
  }

  private static Map<Integer, List<Integer>> initTableDeviceMap() {
    Map<Integer, List<Integer>> tableDeviceMap = new ConcurrentHashMap<>();
    try {
      for (int deviceId = 0; deviceId < config.getDEVICE_NUMBER(); deviceId++) {
        int tableId =
            MetaUtil.mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
        tableDeviceMap.computeIfAbsent(tableId, k -> new ArrayList<>()).add(deviceId);
      }
    } catch (WorkloadException e) {
      LOGGER.error(e.getMessage());
    }
    return tableDeviceMap;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.getQUERY_DEVICE_NUM() > 0
        && config.getQUERY_DEVICE_NUM() <= config.getDEVICE_NUMBER())) {
      throw new WorkloadException("getQUERY_DEVICE_NUM() is not correct, please check.");
    }
    if (!(config.getQUERY_SENSOR_NUM() > 0
        && config.getQUERY_SENSOR_NUM() <= config.getSENSOR_NUMBER())) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }
}
