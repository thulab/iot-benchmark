package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * 采集磁盘IO使用率
 */
public class IoUsage {

    private static Logger log = LoggerFactory.getLogger(IoUsage.class);
    private static IoUsage INSTANCE = new IoUsage();

    private IoUsage(){

    }

    public static IoUsage getInstance(){
        return INSTANCE;
    }

    /**
     * @Purpose:采集磁盘IO使用率
     * @param
     * @return float,磁盘IO使用率,小于1
     */

    public float get() {
        //log.info("开始收集磁盘IO使用率");
        float ioUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -d -x";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            while((line=in.readLine()) != null){
                if(++count >= 4){
                  //log.info(line);
                    String[] temp = line.split("\\s+");
                    if(temp.length > 1){
                        float util =  Float.parseFloat(temp[temp.length-1]);
                        //返回设备中利用率最大的
                        ioUsage = (ioUsage>util)?ioUsage:util;
                    }
                }
            }
            if(ioUsage > 0){
                //log.info("磁盘IO使用率,{}%" , ioUsage);
                ioUsage /= 100.0;
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return ioUsage;
    }

}