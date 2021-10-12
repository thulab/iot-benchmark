package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public abstract class SchemaWriter {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  /**
   * Get Basic Writer
   *
   * @return
   */
  public static SchemaWriter getBasicWriter() {
    return new CSVSchemaWriter();
  }

  /**
   * Write Schema to the file
   *
   * @param deviceSchemaList
   * @return
   */
  public abstract boolean writeSchema(List<DeviceSchema> deviceSchemaList);
}
