package cn.edu.tsinghua.iotdb.benchmark.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;

import java.util.List;

public class MetaUtil {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static List<List<String>> CLIENT_FILES;

  /**
   * Used under cluster mode of benchmark TODO do
   *
   * @param deviceId
   * @return
   */
  public static int getDeviceId(int deviceId) {
    return config.getFIRST_DEVICE_INDEX() + deviceId;
  }

  /**
   * Calculate GroupId(integer) from device according to SG_STRATEGY
   *
   * @param deviceId
   * @return
   * @throws WorkloadException
   */
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
    return String.valueOf(groupId);
  }

  /**
   * Get deviceId from str
   *
   * @param device
   * @return
   */
  public static int getDeviceIdFromStr(String device) {
    int deviceId = device.hashCode();
    if (deviceId < 0) {
      deviceId = -deviceId;
    }
    return deviceId;
  }

  /** Get Format Name */
  public static String getGroupName(Object groupId) {
    return Constants.GROUP_NAME_PREFIX + groupId;
  }

  public static String getDeviceName(Object deviceId) {
    return Constants.DEVICE_NAME_PREFIX + deviceId;
  }

  public static String getSensorName(Object sensorId) {
    return Constants.SENSOR_NAME_PREFIX + sensorId;
  }

  public static List<List<String>> getClientFiles() {
    return CLIENT_FILES;
  }

  public static void setClientFiles(List<List<String>> clientFiles) {
    CLIENT_FILES = clientFiles;
  }
}
