package cn.edu.tsinghua.iotdb.benchmark.source;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;

import java.util.Map;

public abstract class SchemaReader {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  /**
   * get device schema based on file name and data set sensorType
   *
   * @return device schema list to register
   */
  public abstract Map<String, Map<String, SensorType>> getDeviceSchemaList();

  /**
   * Check whether dataset is valid
   *
   * @return result
   */
  public abstract boolean checkDataSet();
}
