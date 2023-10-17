package cn.edu.tsinghua.iot.benchmark.schema;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;

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
  private static final int[] LEVEL_CARDINALITY = new int[TAG_VALUE_CARDINALITY.size() + 1];

  static {
    int idx = TAG_VALUE_CARDINALITY.size();
    int sum = 1;
    LEVEL_CARDINALITY[idx--] = 1;
    for (; idx >= 0; idx--) {
      sum *= TAG_VALUE_CARDINALITY.get(idx);
      LEVEL_CARDINALITY[idx] = sum;
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

  public static Map<String, String> getTag(String deviceName) {
    if (TAG_NUMBER == 0) {
      return Collections.emptyMap();
    }
    int id = deviceName.hashCode();
    Map<String, String> res = new HashMap<>();
    for (int i = 0; i < LEVEL_CARDINALITY.length - 1; i++) {
      id = id % LEVEL_CARDINALITY[i];
      int tagValueId = id / LEVEL_CARDINALITY[i + 1];
      res.put(TAG_KEY_PREFIX + i, TAG_VALUE_PREFIX + tagValueId);
    }
    return res;
  }

  public static Map<String, String> getTag(int deviceId) {
    return getTag(getDeviceName(deviceId));
  }
}
