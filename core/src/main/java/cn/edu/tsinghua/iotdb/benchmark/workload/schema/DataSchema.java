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
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
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
    int eachClientDeviceNum;
    if (config.getCLIENT_NUMBER() != 0) {
      eachClientDeviceNum = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    } else {
      LOGGER.error("getCLIENT_NUMBER() can not be zero.");
      return;
    }

    int deviceId = config.getFIRST_DEVICE_INDEX();
    //TODO FIXME
    //不能均分的数量
    int mod = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    for (int clientId = 0; clientId < config.getCLIENT_NUMBER(); clientId++) {
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int j = 0; j < eachClientDeviceNum; j++) {
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++)));
      }
      //不能均分的这部分，就给那些编号比较小的客户端了。
      if (clientId < mod) {
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++)));
      }
      CLIENT_BIND_SCHEMA.put(clientId, deviceSchemaList);
    }
  }
}
