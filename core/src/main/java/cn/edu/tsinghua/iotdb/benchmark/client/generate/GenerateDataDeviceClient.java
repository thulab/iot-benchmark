package cn.edu.tsinghua.iotdb.benchmark.client.generate;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class GenerateDataDeviceClient extends GenerateBaseClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataDeviceClient.class);

  public GenerateDataDeviceClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected void doTest() {
    try {
      for (int i = 0; i < config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER() + 1; i++) {
        DeviceQuery deviceQuery = queryWorkLoad.getDeviceQuery();
        if (deviceQuery == null) {
          return;
        }
        dbWrapper.deviceQuery(deviceQuery);
      }
    } catch (SQLException sqlException) {
      LOGGER.error("Failed DeviceQuery: " + sqlException.getMessage());
    }
  }
}
