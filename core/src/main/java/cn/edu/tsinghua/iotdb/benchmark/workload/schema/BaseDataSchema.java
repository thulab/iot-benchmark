package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Base of DataSchema */
public abstract class BaseDataSchema {
  /** Store DeviceSchema for each client */
  protected static final Map<Integer, List<DeviceSchema>> CLIENT_BIND_SCHEMA = new HashMap<>();

  public Map<Integer, List<DeviceSchema>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  protected BaseDataSchema() {
    createDataSchema();
  }

  protected abstract void createDataSchema();

  public static DataSchema getInstance() {
    return DataSchemaHolder.INSTANCE;
  }

  private static class DataSchemaHolder {
    private static final DataSchema INSTANCE = new DataSchema();
  }
}
