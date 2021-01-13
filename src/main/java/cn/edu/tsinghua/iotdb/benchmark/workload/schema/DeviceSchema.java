package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String DEVICE_NAME_PREFIX = "d_";

  // each device belongs to one group, i.e., database
  private String group;

  // deviceId
  private String device;

  // sensorIds
  private List<String> sensors;

  // only for synthetic data set
  private int deviceId;

  public DeviceSchema(int deviceId) {
    this.deviceId = deviceId;
    this.device = DEVICE_NAME_PREFIX + deviceId;
    sensors = new ArrayList<>();
    try {
      createEvenlyAllocDeviceSchema();
    } catch (WorkloadException e) {
      LOGGER.error("Create device schema failed.", e);
    }
  }

  public DeviceSchema(String group, String device, List<String> sensors) {
    this.group = config.GROUP_NAME_PREFIX + group;
    this.device = DEVICE_NAME_PREFIX + device;
    this.sensors = sensors;
  }


  private void createEvenlyAllocDeviceSchema() throws WorkloadException {
    int thisDeviceGroupIndex = calGroupId(deviceId);
    //System.out.println("device " + deviceId +" sg " + thisDeviceGroupIndex);
    group = config.GROUP_NAME_PREFIX + thisDeviceGroupIndex;
    sensors.addAll(config.SENSOR_CODES);
  }

  int calGroupId(int deviceId) throws WorkloadException {
    switch (config.SG_STRATEGY) {
      case Constants.MOD_SG_ASSIGN_MODE:
        return deviceId % config.GROUP_NUMBER;
      case Constants.HASH_SG_ASSIGN_MODE:
        return (deviceId + "").hashCode() % config.GROUP_NUMBER;
      case Constants.DIV_SG_ASSIGN_MODE:
        int devicePerGroup = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        return (deviceId / devicePerGroup) % config.GROUP_NUMBER;
      default:
        throw new WorkloadException("Unsupported SG_STRATEGY: " + config.SG_STRATEGY);
    }
  }

  public String getDevice() {
    return device;
  }

  public int getDeviceId() {
    return deviceId;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public List<String> getSensors() {
    return sensors;
  }

  public void setSensors(List<String> sensors) {
    this.sensors = sensors;
  }

}
