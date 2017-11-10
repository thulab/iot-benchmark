package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 采集CPU使用率
 */
public class CpuUsage  {

    private static Logger log = LoggerFactory.getLogger(CpuUsage.class);
    private static CpuUsage INSTANCE = new CpuUsage();

    private CpuUsage(){

    }

    public static CpuUsage getInstance(){
        return INSTANCE;
    }

    /**
     * Purpose:采集CPU使用率
     * @param
     * @return float,CPU使用率,小于1
     */
/*
    public float shortget() {
        //log.info("开始收集cpu使用率");
        float cpuUsage = 0;
        Process pro1,pro2;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "cat /proc/stat";
            //第一次采集CPU时间
            long startTime = System.currentTimeMillis();
            pro1 = r.exec(command);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            long idleCpuTime1 = 0, totalCpuTime1 = 0;   //分别为系统启动后空闲的CPU时间和总的CPU时间
            while((line=in1.readLine()) != null){
                if(line.startsWith("cpu")){
                    line = line.trim();
                    //log.info(line);
                    String[] temp = line.split("\\s+");
                    idleCpuTime1 = Long.parseLong(temp[4]);
                    for(String s : temp){
                        if(!s.equals("cpu")){
                            totalCpuTime1 += Long.parseLong(s);
                        }
                    }
                    //log.info("IdleCpuTime: {}, TotalCpuTime: {}" ,idleCpuTime1 ,totalCpuTime1);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                //log.error("CpuUsage休眠时发生InterruptedException. " + e.getMessage());
                //log.error(sw.toString());
            }
            //第二次采集CPU时间
            long endTime = System.currentTimeMillis();
            pro2 = r.exec(command);
            BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            long idleCpuTime2 = 0, totalCpuTime2 = 0;   //分别为系统启动后空闲的CPU时间和总的CPU时间
            while((line=in2.readLine()) != null){
                if(line.startsWith("cpu")){
                    line = line.trim();
                    //log.info(line);
                    String[] temp = line.split("\\s+");
                    idleCpuTime2 = Long.parseLong(temp[4]);
                    for(String s : temp){
                        if(!s.equals("cpu")){
                            totalCpuTime2 += Long.parseLong(s);
                        }
                    }
                    //log.info("IdleCpuTime: {}, TotalCpuTime: {}" ,idleCpuTime2 , totalCpuTime2);
                    break;
                }
            }
            if(idleCpuTime1 != 0 && totalCpuTime1 !=0 && idleCpuTime2 != 0 && totalCpuTime2 !=0){
                cpuUsage = 1.0f - ((float)(idleCpuTime2 - idleCpuTime1))/((float)(totalCpuTime2 - totalCpuTime1));
                //log.info("本节点CPU使用率为: {}" , cpuUsage);
            }
            in2.close();
            pro2.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            //log.error("CpuUsage发生InstantiationException. " + e.getMessage());
            //log.error(sw.toString());
        }
        return cpuUsage;
    }
*/

    public float get() {
        float cpuUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            //-n 2 表示刷新2轮后退出top，因为1轮的话cpu利用率不会变，默认刷新间隔1s
            String command = "iostat -c";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            while((line=in.readLine()) != null) {
                //log.info(line);
                if(++count >= 4){
                    //log.info(line);
                    String[] temp = line.split("\\s+");
                    if(temp.length > 1){
                        //avg-cpu
                        cpuUsage =  Float.parseFloat(temp[temp.length-1]);
                    }
                    break;
                }
            }
            cpuUsage = 1 - cpuUsage/100.0f;
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return cpuUsage;
    }
}