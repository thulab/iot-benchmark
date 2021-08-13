package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;

/** IRealDataWorkload is a workload using for real data */
public interface IRealDataWorkload extends IWorkLoad {
  /**
   * Return a batch from real data return null if there is no data
   *
   * @return
   * @throws WorkloadException
   */
  Batch getOneBatch() throws WorkloadException;

  /**
   * Return a verified Query
   *
   * @return
   * @throws WorkloadException
   */
  VerificationQuery getVerifiedQuery() throws WorkloadException;
}
