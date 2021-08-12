package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @Author stormbroken Create by 2021/08/12 @Version 1.0 */
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
}
