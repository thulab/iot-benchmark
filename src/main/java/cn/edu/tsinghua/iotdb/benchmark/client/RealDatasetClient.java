package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import java.util.List;

public class RealDatasetClient extends Client implements Runnable{

  private DBWrapper dbWrapper;
  RealDatasetWorkLoad workload;
  private Measurement measurement;

  public RealDatasetClient(List<String> files, Config config) {
    workload = new RealDatasetWorkLoad(files, config);
    measurement = new Measurement();
    dbWrapper = new DBWrapper(measurement);
  }

  @Override
  public void run() {
    long totalTime = 1;

    try {

      long recordNum = 0;

      while(true) {
        Batch batch = workload.getOneBatch();
        if (batch == null) {
          break;
        }
        Status status = dbWrapper.insertOneBatch(batch);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Measurement getMeasurement() {
    return measurement;
  }
}
