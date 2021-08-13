package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.*;

import java.util.List;

/** @Author stormbroken Create by 2021/08/10 @Version 1.0 */
public class RealDataWorkload implements IRealDataWorkload {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private BasicReader basicReader;

  /** Init reader of real dataset write test */
  public RealDataWorkload(int threadId) {
    // file -> device
    // a file -> a batch
    // Precise should do the same thing to read files
    List<String> files = MetaUtil.getThreadFiles().get(threadId);
    switch (config.getDATA_SET()) {
      case TDRIVE:
        basicReader = new TDriveReader(files);
        break;
      case REDD:
        basicReader = new ReddReader(files);
        break;
      case GEOLIFE:
        basicReader = new GeolifeReader(files);
        break;
      case NOAA:
        basicReader = new NOAAReader(files);
        break;
      default:
        throw new RuntimeException(config.getDATA_SET() + " not supported");
    }
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
