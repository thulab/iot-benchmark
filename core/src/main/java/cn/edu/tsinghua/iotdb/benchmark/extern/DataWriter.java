package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;

public abstract class DataWriter {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  public static DataWriter getDataWriter() {
    return new CSVDataWriter();
  }

  /**
   * Write Batch to the file
   *
   * @param batch
   * @param insertLoopIndex loop index of batch
   * @return
   */
  public abstract boolean writeBatch(Batch batch, long insertLoopIndex);
}
