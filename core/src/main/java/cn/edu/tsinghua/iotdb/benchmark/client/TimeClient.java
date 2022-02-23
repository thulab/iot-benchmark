package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TimeClient extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeClient.class);
  private Long testMaxTime = ConfigDescriptor.getInstance().getConfig().getTEST_MAX_TIME();

  private List<Client> clients;

  public TimeClient(List<Client> clients) {
    this.clients = clients;
  }

  @Override
  public void run() {
    if (testMaxTime != 0) {
      super.run();
      boolean finished = false;
      try {
        Thread.sleep(testMaxTime);
      } catch (InterruptedException e) {
        finished = true;
      }
      if (!finished) {
        LOGGER.info("It has been tested for " + testMaxTime + "ms, start to stop all clients.");
        for (Client client : clients) {
          client.stopClient();
        }
      }
    }
  }
}
