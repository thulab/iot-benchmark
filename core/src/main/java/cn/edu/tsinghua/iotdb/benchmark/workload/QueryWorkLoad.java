package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IQueryWorkLoad;

public abstract class QueryWorkLoad implements IQueryWorkLoad {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  public static IQueryWorkLoad getInstance() {
    // Get Query workload according to config
    return new GenerateQueryWorkLoad();
  }
}
