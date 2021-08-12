package cn.edu.tsinghua.iotdb.benchmark.utils;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(MetaUtil.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

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
        return (deviceId / devicePerGroup) % config.getGROUP_NUMBER();
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
    }
  }

  /**
   * Get group name by device str
   * @param deviceId
   * @return
   * @throws WorkloadException
   */
  public static String getGroupNameByDeviceStr(String deviceId){
    int group = deviceId.hashCode();
    if(group < 0){
      group = -group;
    }
    return String.valueOf(group % config.getGROUP_NUMBER());
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
}
