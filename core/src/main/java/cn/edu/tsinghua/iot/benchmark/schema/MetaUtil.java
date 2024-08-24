package cn.edu.tsinghua.iot.benchmark.schema;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaUtil {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String TAG_KEY_PREFIX = config.getTAG_KEY_PREFIX();
  private static final String TAG_VALUE_PREFIX = config.getTAG_VALUE_PREFIX();
  private static final int TAG_NUMBER = config.getTAG_NUMBER();
  private static final List<Integer> TAG_VALUE_CARDINALITY = config.getTAG_VALUE_CARDINALITY();
  private static final List<Long> LEVEL_CARDINALITY =
      Arrays.asList(new Long[TAG_VALUE_CARDINALITY.size() + 1]);

  static {
    int idx = TAG_VALUE_CARDINALITY.size();
    long sum = 1;
    LEVEL_CARDINALITY.set(idx--, 1L);
    for (; idx >= 0; idx--) {
      sum *= TAG_VALUE_CARDINALITY.get(idx);
      LEVEL_CARDINALITY.set(idx, sum);
    }
  }

  private static List<List<String>> CLIENT_FILES;

  /** Used under cluster mode of benchmark */
  public static int getDeviceId(int deviceId) {
    return config.getFIRST_DEVICE_INDEX() + deviceId;
  }

  /** Calculate GroupId(integer) from device according to SG_STRATEGY */
  public static int calGroupId(int deviceId) throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return deviceId % config.getGROUP_NUMBER();
      case Constants.HASH_SG_ASSIGN_MODE:
        return (deviceId + "").hashCode() % config.getGROUP_NUMBER();
      case Constants.DIV_SG_ASSIGN_MODE:
        int devicePerGroup = config.getDEVICE_NUMBER() / config.getGROUP_NUMBER();
        return devicePerGroup == 0
            ? deviceId
            : (deviceId / devicePerGroup) % config.getGROUP_NUMBER();
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
    }
  }

  public static int calTableId(int deviceId) throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return deviceId % config.getIoTDB_TABLE_NUMBER();
      case Constants.HASH_SG_ASSIGN_MODE:
        return (deviceId + "").hashCode() % config.getIoTDB_TABLE_NUMBER();
      case Constants.DIV_SG_ASSIGN_MODE:
        int devicePerTable = config.getDEVICE_NUMBER() / config.getIoTDB_TABLE_NUMBER();
        return devicePerTable == 0
            ? deviceId
            : (deviceId / devicePerTable) % config.getIoTDB_TABLE_NUMBER();
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
    }
  }

  public static int calGroupIdV2(int tableId) throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return tableId % config.getGROUP_NUMBER();
      case Constants.HASH_SG_ASSIGN_MODE:
        return (tableId + "").hashCode() % config.getGROUP_NUMBER();
      case Constants.DIV_SG_ASSIGN_MODE:
        int tablePerGroup = config.getIoTDB_TABLE_NUMBER() / config.getGROUP_NUMBER();
        return tablePerGroup == 0 ? tableId : (tableId / tablePerGroup) % config.getGROUP_NUMBER();
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
    }
  }

  public static int calId(int id, int number1, int number2) throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return id % number2;
      case Constants.HASH_SG_ASSIGN_MODE:
        return (id + "").hashCode() % number2;
      case Constants.DIV_SG_ASSIGN_MODE:
        int itemPerObject = number1 / number2;
        return itemPerObject == 0 ? id : (id / itemPerObject) % number2;
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
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
    return config.getTABLE_NAME_PREFIX() + tableId;
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
    if (TAG_NUMBER == 0) {
      return Collections.emptyMap();
    }
    long id = Math.abs(deviceName.hashCode());
    Map<String, String> res = new HashMap<>();
    for (int i = 0; i < LEVEL_CARDINALITY.size() - 1; i++) {
      id = id % LEVEL_CARDINALITY.get(i);
      long tagValueId = id / LEVEL_CARDINALITY.get(i + 1);
      res.put(TAG_KEY_PREFIX + i, TAG_VALUE_PREFIX + tagValueId);
    }
    return res;
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
