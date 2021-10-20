package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IDataWorkLoad;

import java.util.List;

public abstract class DataWorkLoad implements IDataWorkLoad {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  protected long recentTimestamp = 0;

  public static IDataWorkLoad getInstance(int clientId) {
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_WRITE
        || config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_QUERY) {
      List<String> files = MetaUtil.getClientFiles().get(clientId);
      return new RealDataWorkLoad(files);
    } else {
      List<DeviceSchema> deviceSchemas = metaDataSchema.getDeviceSchemaByClientId(clientId);
      if (config.isIS_CLIENT_BIND()) {
        return new SyntheticDataWorkLoad(deviceSchemas);
      } else {
        return SingletonWorkDataWorkLoad.getInstance();
      }
    }
  }

  @Override
  public long getRecentTimestamp() {
    return recentTimestamp;
  }
}
