package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 采集磁盘IO使用率
 */
public class IoUsage {

    private static Logger log = LoggerFactory.getLogger(IoUsage.class);
    private static IoUsage INSTANCE = new IoUsage();
    private final int  BEGIN_LINE = 10;
    public enum IOStatistics {
        TPS(1,0),
        MB_READ(2,0),
        MB_WRTN(3,0);

        public int pos;
        public float max;

        IOStatistics(int p,float m){
            this.pos = p;
            this.max = m;
        }
    };

    private static class IoUsageHolder{
        private static final IoUsage INSTANCE = new IoUsage();
    }

    private IoUsage(){

    }

    public static IoUsage getInstance(){
        return IoUsageHolder.INSTANCE;
    }

    public HashMap<IOStatistics,Float> getIOStatistics(){
        HashMap<IOStatistics,Float> ioStaMap = new HashMap<>();
        for(IOStatistics iostat : IOStatistics.values()) {
            iostat.max = 0;
        }
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -m 1 2";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            int flag = 1;
            while((line=in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (++count >= BEGIN_LINE) {
                    if(temp.length > 1 && temp[0].startsWith("s")) {
                        //返回设备中最大的
                        for(IOStatistics iostat : IOStatistics.values()) {
                            float t = Float.parseFloat(temp[iostat.pos]);
                            iostat.max = (iostat.max > t) ? iostat.max : t;
                            ioStaMap.put(iostat, iostat.max);
                        }
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return ioStaMap;
    }

    /**
     * @Purpose:采集磁盘IO使用率
     * @param
     * @return float,磁盘IO使用率,小于1
     */
    public ArrayList<Float> get() {
        //log.info("开始收集磁盘IO使用率");
        ArrayList<Float> list = new ArrayList<>();
        float ioUsage = 0.0f;
        float cpuUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -x 1 2";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            int flag = 1;
            while((line=in.readLine()) != null) {
                //log.info(line);
                String[] temp = line.split("\\s+");
                if (++count >= 8) {
                    if (temp[0].startsWith("a") && flag == 1) {
                        flag = 0 ;
                    }else  if (flag == 0){
                        cpuUsage = Float.parseFloat(temp[temp.length - 1]);
                        cpuUsage = 1 - cpuUsage/100.0f;
                        flag = 1;
                    } else if(temp.length > 1 && temp[0].startsWith("s")) {
                        float util = Float.parseFloat(temp[temp.length - 1]);
                        //返回设备中利用率最大的
                        ioUsage = (ioUsage > util) ? ioUsage : util;
                    }
                }
            }
            if(ioUsage > 0){
                //log.info("磁盘IO使用率,{}%" , ioUsage);
                ioUsage /= 100.0;
            }
            list.add(cpuUsage);
            list.add(ioUsage);
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return list;
    }

}