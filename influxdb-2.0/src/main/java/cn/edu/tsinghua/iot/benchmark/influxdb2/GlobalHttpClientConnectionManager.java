package cn.edu.tsinghua.iot.benchmark.influxdb2;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class GlobalHttpClientConnectionManager {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static PoolingHttpClientConnectionManager manager =
      new PoolingHttpClientConnectionManager();

  static {
    manager.setMaxTotal(config.getHTTP_CLIENT_POOL_SIZE());
    manager.setDefaultMaxPerRoute(config.getHTTP_CLIENT_POOL_SIZE());
  }

  private static CloseableHttpClient client =
      HttpClients.custom().setConnectionManager(manager).build();

  private static PoolingHttpClientConnectionManager getManager() {
    return manager;
  }

  public static CloseableHttpClient getHttpClient() {
    return client;
  }

  public static void setMaxTotal(int num) {
    manager.setMaxTotal(num);
  }

  public static void setDefaultMaxPerRoute(int num) {
    manager.setDefaultMaxPerRoute(num);
  }
}
