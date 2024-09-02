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

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
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

  /** tableId(deviceId) maps to groupId(tableId) according to SG_STRATEGY */
  public static int mappingId(int id, int factor1, int factor2) throws WorkloadException {
    switch (config.getSG_STRATEGY()) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return id % factor2;
      case Constants.HASH_SG_ASSIGN_MODE:
        return String.valueOf(id).hashCode() % factor2;
      case Constants.DIV_SG_ASSIGN_MODE:
        int itemPerObject = factor1 / factor2;
        return itemPerObject == 0 ? id : (id / itemPerObject) % factor2;
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

  public static String getTableIdFromDeviceName(String deviceName) {
    int tableId = Math.abs(deviceName.hashCode());
    tableId = tableId % config.getIoTDB_TABLE_NUMBER();
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
