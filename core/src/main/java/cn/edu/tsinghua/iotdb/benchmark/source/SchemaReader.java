package cn.edu.tsinghua.iotdb.benchmark.source;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class SchemaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaReader.class);
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
