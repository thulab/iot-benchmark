package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.*;

import java.util.List;

public class RealDataWorkload implements IRealDataWorkload {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private BasicReader basicReader;

  /** Init reader of real dataset write test */
  public RealDataWorkload(int threadId) {
    List<String> files = MetaUtil.getThreadFiles().get(threadId);
    basicReader = new GenerateReader(files);
  }

  /**
   * Return a batch from real data return null if there is no data
   *
   * @return
   * @throws WorkloadException
   */
  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (basicReader.hasNextBatch()) {
      return basicReader.nextBatch();
    } else {
      return null;
    }
  }

  /**
   * Return a verified Query
   *
   * @return
   * @throws WorkloadException
   */
  @Override
  public VerificationQuery getVerifiedQuery() throws WorkloadException {
    Batch batch = getOneBatch();
    if (batch == null) {
      return null;
    }
    return new VerificationQuery(batch);
  }
}
