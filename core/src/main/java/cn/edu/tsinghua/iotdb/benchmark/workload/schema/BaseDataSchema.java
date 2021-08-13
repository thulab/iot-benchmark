package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Base of DataSchema */
public abstract class BaseDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Store DeviceSchema for each client */
  protected static final Map<Integer, List<DeviceSchema>> CLIENT_BIND_SCHEMA = new HashMap<>();
  /** Type map for each sensors, mapping rule: device(e.g. d_0) -> sensor (e.g. s_0) -> type */
  protected static final Map<String, Map<String, Type>> TYPE_MAPPING = new HashMap<>();
  /** The singleton of BaseDataSchema */
  private static BaseDataSchema baseDataSchema;

  protected BaseDataSchema() {
    createDataSchema();
  }

  /** Create Data Schema for each device */
  protected abstract void createDataSchema();

  /** Getter */

  /**
   * Get Device Schema for each client
   *
   * @return
   */
  public Map<Integer, List<DeviceSchema>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  /**
   * Add sensor type into TYPE_MAPPING
   *
   * @param device
   * @param sensor
   * @param type
   */
  public void addSensorType(String device, String sensor, Type type) {
    if (!TYPE_MAPPING.containsKey(device)) {
      TYPE_MAPPING.put(device, new HashMap<>());
    }
    TYPE_MAPPING.get(device).put(sensor, type);
  }

  /**
   * Add sensor type into TYPE_MAPPING
   *
   * @param device
   * @param types
   */
  public void addSensorType(String device, Map<String, Type> types) {
    TYPE_MAPPING.put(device, types);
  }

  // TODO modify hard code

  /**
   * Get sensor type of one sensor
   *
   * @param device
   * @param sensor name e.g. s_0
   * @return default: Constants.DEFAULT_TYPE;
   */
  public Type getSensorType(String device, String sensor) {
    try {
      return TYPE_MAPPING.get(device).get(sensor);
    } catch (Exception exception) {
      LOGGER.warn(String.format("Unknown type for device: %s, sensor: %s", device, sensor));
      return Type.TEXT;
    }
  }

  /**
   * Get Thread Device Schema
   *
   * @param threadId
   * @return
   */
  public List<DeviceSchema> getThreadDeviceSchema(int threadId) {
    return CLIENT_BIND_SCHEMA.get(threadId);
  }

  /**
   * Get All Device Schema
   *
   * @return
   */
  public List<DeviceSchema> getAllDeviceSchema() {
    List<DeviceSchema> deviceSchemaList = new ArrayList<>();
    for (Map.Entry<Integer, List<DeviceSchema>> schema : CLIENT_BIND_SCHEMA.entrySet()) {
      deviceSchemaList.addAll(schema.getValue());
    }
    return deviceSchemaList;
  }

  /**
   * Get sensors type of one device
   *
   * @param device
   * @return empty
   */
  public Map<String, Type> getSensorType(String device) {
    try {
      return TYPE_MAPPING.get(device);
    } catch (Exception e) {
      LOGGER.warn(String.format("Unknown type for device: %s", device));
      return new HashMap<>();
    }
  }

  /** Singleton */
  public static BaseDataSchema getInstance() {
    if (baseDataSchema == null) {
      synchronized (BaseDataSchema.class) {
        if (baseDataSchema == null) {
          if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_VERIFICATION)) {
            baseDataSchema = new RealDataSchema();
          } else {
            baseDataSchema = new DataSchema();
          }
        }
      }
    }
    return baseDataSchema;
  }
}
