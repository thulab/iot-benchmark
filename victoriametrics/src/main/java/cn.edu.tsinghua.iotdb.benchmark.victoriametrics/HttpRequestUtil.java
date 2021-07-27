package cn.edu.tsinghua.iotdb.benchmark.victoriametrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

/**
 * @Author stormbroken
 * Create by 2021/07/27
 * @Version 1.0
 **/

public class HttpRequestUtil {
    /**
     * Send Get Request to target URL
     * @param url
     * @param params
     * @return
     */
    public static String sendGet(String url, Map<String, String> params) throws Exception{
        StringBuffer result = new StringBuffer();
        BufferedReader in = null;
        StringBuffer paramsString = new StringBuffer();
        for(Map.Entry<String, String> param: params.entrySet()){
            paramsString.append("&");
            paramsString.append(param.getKey() + "=" + param.getValue());
        }
        paramsString.replace(0, 1, "?");
        try{
            URL urlWithParams = new URL(url + paramsString.toString());
            // open connection
            URLConnection connection = urlWithParams.openConnection();
            // setup property of request
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty(
                    "user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // create connection
            connection.connect();
            // use BufferReader to read response
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while((line = in.readLine()) != null){
                result.append(line);
            }
        }catch (Exception e){
            throw e;
        }finally {
            try{
                if(in != null){
                    in.close();
                }
            }catch (Exception e2){
                throw e2;
            }
        }
        return result.toString();
    }

    /**
     * Send Post request to target url
     * @param url
     * @param body
     * @return
     */
    public static String sendPost(String url, String body) throws Exception{
        PrintWriter out = null;
        BufferedReader in = null;
        StringBuffer result = new StringBuffer();
        try{
            URL realUrl = new URL(url);
            // open url
            URLConnection connection = realUrl.openConnection();
            // setup property of connection
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty(
                    "user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setDoInput(true);
            connection.setDoOutput(true);
            // get output stream
            out = new PrintWriter(connection.getOutputStream());
            // send body
            if(body != null){
                out.print(body);
            }
            // flush buffer
            out.flush();
            // define BufferReader to read response from url
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while((line = in.readLine()) != null){
                result.append(line);
            }
        }catch (Exception e){
            throw e;
        }finally {
            try{
                if(out != null){
                    out.close();
                }
                if(in != null){
                    in.close();
                }
            }catch (IOException e2){
                throw e2;
            }
        }
        return result.toString();
    }

}