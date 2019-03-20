package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSchema.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Map<Integer, List<DeviceSchema>> CLIENT_BIND_SCHEMA = new HashMap<>();

  public Map<Integer, List<DeviceSchema>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  private DataSchema(){
    createClientBindSchema();
  }

  public static DataSchema getInstance() {
    return DataSchemaHolder.INSTANCE;
  }

  private static class  DataSchemaHolder {
    private static final DataSchema INSTANCE = new DataSchema();
  }

  private void createClientBindSchema() {
    int eachClientDeviceNum = 0;
    if(config.CLIENT_NUMBER!=0) {
      eachClientDeviceNum = config.DEVICE_NUMBER / config.CLIENT_NUMBER;
    } else {
      LOGGER.error("CLIENT_NUMBER can not be zero.");
      return;
    }
    for (int clientId = 0; clientId < config.CLIENT_NUMBER - 1; clientId++) {
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int i = clientId * eachClientDeviceNum; i < (clientId + 1) * eachClientDeviceNum; i++) {
        deviceSchemaList.add(new DeviceSchema(i));
      }
      CLIENT_BIND_SCHEMA.put(clientId, deviceSchemaList);
    }
    // get the schema of the last client
    List<DeviceSchema> lastDeviceSchemaList = new ArrayList<>();
    for (int i = (config.CLIENT_NUMBER - 1) * eachClientDeviceNum; i < config.DEVICE_NUMBER; i++) {
      lastDeviceSchemaList.add(new DeviceSchema(i));
    }
    CLIENT_BIND_SCHEMA.put(config.CLIENT_NUMBER - 1, lastDeviceSchemaList);
  }
}
