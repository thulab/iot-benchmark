package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  public static final String GROUP_NAME_PREFIX = "group_";
  private static final String DEVICE_NAME_PREFIX = "d_";

  private int deviceId;
  private String group;
  private String device;
  private List<String> sensors;

  public DeviceSchema(int deviceId){
    this.deviceId = deviceId;
    this.device = DEVICE_NAME_PREFIX + deviceId;
    sensors = new ArrayList<>();
    try {
      createEvenlyAllocDeviceSchema();
    } catch (WorkloadException e) {
      LOGGER.error("Create device schema failed.", e);
    }
  }

  private void createEvenlyAllocDeviceSchema() throws WorkloadException{
    if(config.GROUP_NUMBER > config.DEVICE_NUMBER)
      throw new WorkloadException("DEVICE_NUMBER must less than or equal to GROUP_NUMBER.");
    int eachGroupDeviceNum = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    int thisDeviceGroupIndex = deviceId / eachGroupDeviceNum;
    if(deviceId >= eachGroupDeviceNum * config.GROUP_NUMBER){
      thisDeviceGroupIndex = config.GROUP_NUMBER - 1;
    }
    if(thisDeviceGroupIndex < 0)
      throw new WorkloadException("DEVICE_NUMBER and GROUP_NUMBER must be positive.");
    group = GROUP_NAME_PREFIX + thisDeviceGroupIndex;
    sensors.addAll(config.SENSOR_CODES);
  }

  public int getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(int deviceId) {
    this.deviceId = deviceId;
  }

  public String getDevice() {
    return device;
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
