package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Base of DataSchema */
public abstract class BaseDataSchema {
  /** Store DeviceSchema for each client */
  protected static final Map<Integer, List<DeviceSchema>> CLIENT_BIND_SCHEMA = new HashMap<>();
  /** Type map for each sensors, mapping rule: device(e.g. d_0) -> sensor (e.g. s_0) -> type */
  protected static final Map<String, Map<String, String>> TYPE_MAPPING = new HashMap<>();
  /** The singleton of BaseDataSchema */
  private static BaseDataSchema baseDataSchema;

  protected BaseDataSchema() {
    createDataSchema();
  }

  protected abstract void createDataSchema();

  /**
   * Getter
   */

  public Map<Integer, List<DeviceSchema>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  public Map<String, Map<String, String>> getTypeMapping(){
    return TYPE_MAPPING;
  }

  /**
   * Singleton
   */

  public static BaseDataSchema getInstance() {
    if(baseDataSchema == null){
      synchronized (BaseDataSchema.class){
        if(baseDataSchema ==null){
          // TODO modify by Configuration
          baseDataSchema = new DataSchema();
        }
      }
    }
    return baseDataSchema;
  }

}
