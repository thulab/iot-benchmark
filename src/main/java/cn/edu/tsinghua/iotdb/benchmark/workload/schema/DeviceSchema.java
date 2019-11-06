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
    this.group = GROUP_NAME_PREFIX + group;
    this.device = DEVICE_NAME_PREFIX + device;
    this.sensors = sensors;
  }


  private void createEvenlyAllocDeviceSchema() throws WorkloadException {
    int thisDeviceGroupIndex = calGroupId(deviceId, config.GROUP_NUMBER);
    //System.out.println("device " + deviceId +" sg " + thisDeviceGroupIndex);
    group = GROUP_NAME_PREFIX + thisDeviceGroupIndex;
    sensors.addAll(config.SENSOR_CODES);
  }

  static int calGroupId(int deviceId, int groupNum) throws WorkloadException {
    return deviceId % groupNum;
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
