package cn.edu.tsinghua.iot.benchmark.cnosdb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.TimeUnit;

public class CnosConnection {
  private final String url;
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  OkHttpClient client;

  CnosConnection(String urlString, String cnosDbName) throws MalformedURLException {
    ConnectionPool connectionPool =
        new ConnectionPool(config.getCLIENT_NUMBER(), 5, TimeUnit.MINUTES);
    client = new OkHttpClient().newBuilder().connectionPool(connectionPool).build();
    url = urlString + "/api/v1/sql?db=" + cnosDbName;
  }

  public Response execute(String sql) throws IOException {
    MediaType mediaType = MediaType.parse("application/nd-json");
    RequestBody requestBody = RequestBody.create(mediaType, sql.getBytes());
    Request request =
        new Request.Builder()
            .url(url)
            .addHeader("Accept", "application/nd-json")
            .addHeader("Authorization", "Basic cm9vdDo=")
            .post(requestBody)
            .build();
    return client.newCall(request).execute();
  }
}
