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
    public float getUsageBasedOnFreemem() {
        //通过1-freeMem/totalMem的方式计算Usage
        float memUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "cat /proc/meminfo";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count = 0;
            long totalMem = 0, freeMem = 0;
            while((line=in.readLine()) != null){
                //log.info(line);
                String[] memInfo = line.split("\\s+");
                if(memInfo[0].startsWith("MemTotal")){
                    totalMem = Long.parseLong(memInfo[1]);
                }
                if(memInfo[0].startsWith("MemFree")){
                    freeMem = Long.parseLong(memInfo[1]);
                }
                memUsage = 1 - (float)freeMem/(float)totalMem;
                //log.info("本节点内存使用率为: {}" ,memUsage);
                if(++count == 2){
                    break;
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

    /*
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
            int count =  0;

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
            log.info("pid:"+ String.valueOf(pid));
            //String command = "pmap -d " + String.valueOf(pid) + " | grep write | cut -d ' ' -f 7";
            String command = "pmap -d " + String.valueOf(pid) ;
            try {
                pro = r.exec(command);
                BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                log.info("line:"+in.readLine());
                String line = null;
                while ((line = in.readLine()) != null) {
                    String[] temp = line.split("K");
                    log.info("line:"+in.readLine());
                    processMemUsage = Long.parseLong(temp[0]) / KB2GB;
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