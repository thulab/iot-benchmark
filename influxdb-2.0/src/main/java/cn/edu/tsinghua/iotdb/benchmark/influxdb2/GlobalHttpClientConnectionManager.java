package cn.edu.tsinghua.iotdb.benchmark.influxdb2;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class GlobalHttpClientConnectionManager {

    private static PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();

    final static int DEFAULT = 100;
    static {
        manager.setMaxTotal(DEFAULT);
        manager.setDefaultMaxPerRoute(DEFAULT);
    }

    private static CloseableHttpClient client = HttpClients.custom().setConnectionManager(manager).build();


    private static  PoolingHttpClientConnectionManager getManager(){

        return manager;
    }

    public static CloseableHttpClient getHttpClient(){
        return client;
    }

    public static void setMaxTotal(int num){
        manager.setMaxTotal(num);
    }

    public static void setDefaultMaxPerRoute(int num){
        manager.setDefaultMaxPerRoute(num);
    }
}
