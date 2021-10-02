package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public abstract class BasicWriter {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  /**
   * Get Basic Writer
   *
   * @return
   */
  public static BasicWriter getBasicWriter() {
    return new CSVWriter();
  }

  /**
   * Write Schema to the file
   *
   * @param deviceSchemaList
   * @return
   */
  public abstract boolean writeSchema(List<DeviceSchema> deviceSchemaList);

  /**
   * Write Batch to the file
   *
   * @param batch
   * @param insertLoopIndex loop index of batch
   * @return
   */
  public abstract boolean writeBatch(Batch batch, long insertLoopIndex);
}
