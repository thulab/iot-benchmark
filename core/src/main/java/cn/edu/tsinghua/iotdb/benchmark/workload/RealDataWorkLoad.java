package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.source.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RealDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealDataWorkLoad.class);
  private DataReader dataReader;
  private final long batchNumber;

  public RealDataWorkLoad(List<String> files) {
    dataReader = DataReader.getInstance(files);
    batchNumber = files.size() * config.getBIG_BATCH_SIZE();
  }

  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (dataReader.hasNextBatch()) {
      return dataReader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public long getBatchNumber() {
    return batchNumber;
  }
}
