package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * 采集内存使用率
 */
public class MemUsage {

    private static Logger log = LoggerFactory.getLogger(MemUsage.class);
    private static MemUsage INSTANCE = new MemUsage();
    private final double KB2GB = 1024 * 1024f;

    private MemUsage(){

    }

    public static MemUsage getInstance(){
        return INSTANCE;
    }

    /**
     * Purpose:采集内存使用率
     * @param
     * @return float,内存使用率,小于1
     */
    public float get(){
        //log.info("开始收集内存使用率");
        float memUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "free";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            while((line=in.readLine()) != null) {
                //log.info(line);
                String[] temp = line.split("\\s+");
                if (temp[0].startsWith("M")) {
                    float memTotal = Float.parseFloat(temp[1]);
                    float memUsed = Float.parseFloat(temp[2]);
                    memUsage = memUsed / memTotal;
                }

            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return memUsage;
    }

    public double getProcessMemUsage(){
        double processMemUsage = 0.0d;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        int pid = OpenFileNumber.getInstance().getPid();
        if(pid > 0) {
            //String command = "pmap -d " + String.valueOf(pid) + " | grep write | cut -d ' ' -f 7";
            String command = "pmap -d " + String.valueOf(pid) ;
            try {
                pro = r.exec(command);
                BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line = null;
                while ((line = in.readLine()) != null) {
                    if(line.startsWith("map")) {
                        String[] temp = line.split(" ");
                        String[] tmp = temp[6].split("K");
                        String size = tmp[0];
                        processMemUsage = Long.parseLong(size) / KB2GB;
                    }
                }
            } catch (IOException e) {
                log.error("Get Process Memory Usage failed.");
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
            }
        }
        return processMemUsage;
    }

}