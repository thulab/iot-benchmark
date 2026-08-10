package cn.edu.tsinghua.iot.benchmark.schema;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.utils.CommonAlgorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaUtil.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String TAG_KEY_PREFIX = config.getTAG_KEY_PREFIX();
  private static final String TAG_VALUE_PREFIX = config.getTAG_VALUE_PREFIX();
  private static final int TAG_NUMBER = config.getTAG_NUMBER();
  private static final List<Integer> TAG_VALUE_CARDINALITY = config.getTAG_VALUE_CARDINALITY();
  private static final List<Long> LEVEL_CARDINALITY = buildLevelCardinality(TAG_VALUE_CARDINALITY);

  private static List<List<String>> CLIENT_FILES;

  /** Used under cluster mode of benchmark */
  public static int getDeviceId(int deviceId) {
    return config.getFIRST_DEVICE_INDEX() + deviceId;
  }

  /** tableId(deviceId) maps to groupId(tableId) according to SG_STRATEGY */
  public static int mappingId(int objectId, int objectNumber, int allocatingObjectNumber)
      throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return objectId % allocatingObjectNumber;
      case Constants.HASH_SG_ASSIGN_MODE:
        return String.valueOf(objectId).hashCode() % allocatingObjectNumber;
      case Constants.DIV_SG_ASSIGN_MODE:
        int itemPerObject = objectNumber / allocatingObjectNumber;
        return itemPerObject == 0 ? objectId : (objectId / itemPerObject) % allocatingObjectNumber;
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
    }
  }

  /**
   * It traverses all device IDs, assigns each device to the corresponding table, and further
   * aggregates the devices in the table into the corresponding database. <br>
   * IoTDB-TableMode : Ensure that multiple devices written in a single batch come from the same
   * table.<br>
   * IoTDB-TreeMode : It will not affect its writing speed.
   *
   * @return deviceIds
   */
  public static List<Integer> sortDeviceId() {
    List<Integer> deviceIds = new ArrayList<>();
    Map<Integer, List<Integer>> tableDeviceMap =
        new HashMap<>(config.getIoTDB_TABLE_NUMBER(), 1.00f);
    Map<Integer, List<Integer>> databaseDeviceMap = new HashMap<>(config.getGROUP_NUMBER(), 1.00f);
    try {
      // Get the device contained in each table
      for (int deviceId = 0; deviceId < config.getDEVICE_NUMBER(); deviceId++) {
        // Calculate tableId from deviceId
        int tableId =
            mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
        tableDeviceMap
            .computeIfAbsent(
                tableId,
                k ->
                    new ArrayList<>(config.getDEVICE_NUMBER() / config.getIoTDB_TABLE_NUMBER() + 1))
            .add(deviceId);
      }
      // By using tableDeviceMap, quickly get the devices contained in each database
      for (int tableId = 0; tableId < config.getIoTDB_TABLE_NUMBER(); tableId++) {
        // Calculate databaseId from tableId
        int databaseId =
            mappingId(tableId, config.getIoTDB_TABLE_NUMBER(), config.getGROUP_NUMBER());
        databaseDeviceMap
            .computeIfAbsent(databaseId, k -> new ArrayList<>())
            .addAll(tableDeviceMap.getOrDefault(tableId, Collections.emptyList()));
      }
    } catch (WorkloadException e) {
      LOGGER.error(e.getMessage());
    }
    databaseDeviceMap.values().forEach(deviceIds::addAll);
    return deviceIds;
  }

  public static void distributeDevices(
      int clientNumber,
      Map<Integer, List<DeviceSchema>> clientDataSchema,
      List<Sensor> sensors,
      Map<String, DeviceSchema> nameDataSchema,
      Set<String> groups) {
    Map<Integer, Integer> deviceDistributionForClient =
        CommonAlgorithms.distributeDevicesToClients(config.getDEVICE_NUMBER(), clientNumber);
    int deviceIndex = MetaUtil.getDeviceId(0);
    List<Integer> deviceIds = sortDeviceId();
    for (int clientId = 0; clientId < clientNumber; clientId++) {
      int deviceNumber = deviceDistributionForClient.get(clientId);
      List<DeviceSchema> deviceSchemasList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema =
            new DeviceSchema(
                deviceIds.get(deviceIndex), sensors, MetaUtil.getTags(deviceIds.get(deviceIndex)));
        deviceSchemasList.add(deviceSchema);
        nameDataSchema.putIfAbsent(deviceSchema.getDevice(), deviceSchema);
        groups.add(deviceSchema.getGroup());
        deviceIndex++;
      }
      clientDataSchema.put(clientId, deviceSchemasList);
    }
    if (config.getIoTDB_DIALECT_MODE() == SQLDialect.TABLE && config.hasWrite()) {
      for (Map.Entry<Integer, List<DeviceSchema>> entry : clientDataSchema.entrySet()) {
        List<DeviceSchema> schemas = entry.getValue();
        if (schemas.isEmpty()) {
          continue;
        }
        String expectedGroup = schemas.get(0).getGroup();
        for (DeviceSchema schema : schemas) {
          if (!expectedGroup.equals(schema.getGroup())) {
            LOGGER.error(
                "Client {} has devices across multiple databases ({} and {}). "
                    + "In TableMode, each client must be bound to a single database.",
                entry.getKey(),
                expectedGroup,
                schema.getGroup());
            throw new RuntimeException(
                "Device distribution violated single-database-per-client constraint in TableMode");
          }
        }
      }
    }
  }

  public static String getGroupIdFromDeviceName(String deviceName) {
    int groupId = deviceName.hashCode();
    if (groupId < 0) {
      groupId = -groupId;
    }
    groupId = groupId % config.getGROUP_NUMBER();
    return String.valueOf(groupId);
  }

  public static String getTableIdFromDeviceName(String deviceName) {
    int tableId = -1;
    try {
      int deviceId =
          Integer.parseInt(deviceName.substring(config.getDEVICE_NAME_PREFIX().length()));
      tableId = mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
    } catch (NumberFormatException | WorkloadException e) {
      LOGGER.error("getTableIdFromDeviceName failed.", e);
    }
    return String.valueOf(tableId);
  }

  /** Get deviceId from str */
  public static int getDeviceIdFromStr(String device) {
    int deviceId = device.hashCode();
    if (deviceId < 0) {
      deviceId = -deviceId;
    }
    return deviceId;
  }

  /** Get Format Name */
  public static String getGroupName(Object groupId) {
    return config.getGROUP_NAME_PREFIX() + groupId;
  }

  public static String getTableName(Object tableId) {
    return config.getIoTDB_TABLE_NAME_PREFIX() + tableId;
  }

  public static String getDeviceName(Object deviceId) {
    return config.getDEVICE_NAME_PREFIX() + deviceId;
  }

  public static String getSensorName(Object sensorId) {
    return config.getSENSOR_NAME_PREFIX() + sensorId;
  }

  public static List<List<String>> getClientFiles() {
    return CLIENT_FILES;
  }

  public static void setClientFiles(List<List<String>> clientFiles) {
    CLIENT_FILES = clientFiles;
  }

  /**
   * Get tags pair by deviceName. Tags are allocated based on hashCode to ensure an even number of
   * devices under each tag as much as possible.
   *
   * @param deviceName deviceName
   * @return tags pair
   */
  public static Map<String, String> getTags(String deviceName) {
    return createTags(deviceName, TAG_NUMBER, TAG_KEY_PREFIX, TAG_VALUE_PREFIX, LEVEL_CARDINALITY);
  }

  /**
   * Calculates tags from an explicit tag configuration.
   *
   * <p>The normal {@link #getTags(String)} path uses the configuration captured at benchmark
   * startup. Query metadata discovery uses this overload so its per-table value enumeration is
   * based on the exact configuration being validated and tested.
   */
  public static Map<String, String> getTags(
      String deviceName,
      int tagNumber,
      String tagKeyPrefix,
      String tagValuePrefix,
      List<Integer> tagValueCardinality) {
    if (tagNumber != tagValueCardinality.size()) {
      throw new IllegalArgumentException("tagNumber must be equal to tagValueCardinality's size");
    }
    return createTags(
        deviceName,
        tagNumber,
        tagKeyPrefix,
        tagValuePrefix,
        buildLevelCardinality(tagValueCardinality));
  }

  /** Calculates one tag value without materializing all tags for the device. */
  public static String getTagValue(
      String deviceName, int tagIndex, String tagValuePrefix, List<Integer> tagValueCardinality) {
    if (tagIndex < 0 || tagIndex >= tagValueCardinality.size()) {
      throw new IllegalArgumentException("tagIndex is outside tagValueCardinality");
    }
    long levelCardinality = 1;
    for (int i = tagIndex; i < tagValueCardinality.size(); i++) {
      int cardinality = tagValueCardinality.get(i);
      if (cardinality <= 0) {
        throw new IllegalArgumentException("tagValueCardinality must contain positive values");
      }
      levelCardinality *= cardinality;
    }
    long nextLevelCardinality = levelCardinality / tagValueCardinality.get(tagIndex);
    long id = Math.abs(deviceName.hashCode());
    long tagValueId = (id % levelCardinality) / nextLevelCardinality;
    return tagValuePrefix + tagValueId;
  }

  private static Map<String, String> createTags(
      String deviceName,
      int tagNumber,
      String tagKeyPrefix,
      String tagValuePrefix,
      List<Long> levelCardinality) {
    if (tagNumber == 0) {
      return Collections.emptyMap();
    }
    long id = Math.abs(deviceName.hashCode());
    Map<String, String> res = new HashMap<>();
    for (int i = 0; i < levelCardinality.size() - 1; i++) {
      id = id % levelCardinality.get(i);
      long tagValueId = id / levelCardinality.get(i + 1);
      res.put(tagKeyPrefix + i, tagValuePrefix + tagValueId);
    }
    return res;
  }

  private static List<Long> buildLevelCardinality(List<Integer> tagValueCardinality) {
    List<Long> levelCardinality = Arrays.asList(new Long[tagValueCardinality.size() + 1]);
    int idx = tagValueCardinality.size();
    long product = 1;
    levelCardinality.set(idx--, 1L);
    for (; idx >= 0; idx--) {
      product *= tagValueCardinality.get(idx);
      levelCardinality.set(idx, product);
    }
    return levelCardinality;
  }

  /**
   * Get tags pair by deviceId. Tags are allocated based on hashCode to ensure an even number of
   * devices under each tag as much as possible.
   *
   * @param deviceId deviceId
   * @return tags pair
   */
  public static Map<String, String> getTags(int deviceId) {
    return getTags(getDeviceName(deviceId));
  }
}
