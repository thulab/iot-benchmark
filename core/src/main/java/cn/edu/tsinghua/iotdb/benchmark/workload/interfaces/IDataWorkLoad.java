package cn.edu.tsinghua.iotdb.benchmark.workload.interfaces;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;

/** interface of data workload */
public interface IDataWorkLoad extends IWorkLoad {
  /**
   * Insert one batch into database NOTICE: every row contains data from all sensors
   *
   * @return
   * @throws WorkloadException
   */
  Batch getOneBatch() throws WorkloadException;

  /**
   * Get Batch Number
   *
   * @return
   */
  long getBatchNumber();
}
