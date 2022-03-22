package cn.edu.tsinghua.iotdb.benchmark.influxdb2;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import java.nio.charset.StandardCharsets;

public class HttpRequestUtil {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public static int writeData(String url, String body, String contentType, String token)
      throws Exception {
    CloseableHttpClient httpClient = GlobalHttpClientConnectionManager.getHttpClient();
    int responseCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    CloseableHttpResponse httpResponse = null;
    try {
      HttpPost httpPost = new HttpPost(url);
      // 设置请求和传输超时时间
      RequestConfig requestConfig =
          RequestConfig.custom()
              .setSocketTimeout(config.getWRITE_OPERATION_TIMEOUT_MS() / 2)
              .setConnectTimeout(config.getWRITE_OPERATION_TIMEOUT_MS() / 2)
              .build();
      httpPost.setConfig(requestConfig);
      httpPost.addHeader("User-Agent", "Mozilla/5.0");
      httpPost.addHeader("accept", "*/*");
      httpPost.addHeader("connection", "Keep-Alive");
      httpPost.addHeader("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
      httpPost.addHeader("Content-Type", contentType);
      httpPost.addHeader("Authorization", "Token " + token);

      ByteArrayEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
      httpPost.setEntity(entity);

      httpResponse = httpClient.execute(httpPost);
      responseCode = httpResponse.getStatusLine().getStatusCode();
    } catch (Exception var) {
      var.printStackTrace();
      throw var;
    } finally {
      if (httpResponse != null) {
        httpResponse.close();
      }
    }
    return responseCode;
  }
}
