package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * 采集网络带宽使用率
 */
public class NetUsage {

    private static Logger log = LoggerFactory.getLogger(NetUsage.class);
    private static NetUsage INSTANCE = new NetUsage();
    private final static float TotalBandwidth = 1000;   //网口带宽,Mbps,假设是前兆以太网
    private Config config;

    private NetUsage() {
        config = ConfigDescriptor.getInstance().getConfig();
    }

    public static NetUsage getInstance() {
        return INSTANCE;
    }

    /**
     * @param
     * @return float, 网络带宽使用率, 小于1
     * @Purpose:采集网络带宽使用率
     */
    public float getTotalRate() {
        //log.info("开始收集网络带宽使用率");
        //float netUsage = 0.0f;
        float curRate = 0.0f;
        Process pro1, pro2;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "cat /proc/net/dev";
            //第一次采集流量数据

            pro1 = r.exec(command);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            long inSize1 = 0, outSize1 = 0;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(config.NET_DEVICE)) {
                    String[] temp = line.split("\\s+");
                    inSize1 = Long.parseLong(temp[1]); //Receive bytes,单位为Byte
                    outSize1 = Long.parseLong(temp[9]);             //Transmit bytes,单位为Byte
                    //log.info("inSize1={},outSize1={}",inSize1,outSize1);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
            long startTime = System.currentTimeMillis();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.info("NetUsage休眠时发生InterruptedException. " + e.getMessage());
                log.error(sw.toString());
            }
            //第二次采集流量数据

            pro2 = r.exec(command);
            BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            long inSize2 = 0, outSize2 = 0;
            while ((line = in2.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(config.NET_DEVICE)) {
                    String[] temp = line.split("\\s+");
                    inSize2 = Long.parseLong(temp[1]);
                    outSize2 = Long.parseLong(temp[9]);
                    //log.info("inSize2={},outSize2={}",inSize2,outSize2);
                    break;
                }
            }
            long endTime = System.currentTimeMillis();
            float interval = (float) (endTime - startTime) / 1000;
            if (inSize1 != 0 && outSize1 != 0 && inSize2 != 0 && outSize2 != 0 && interval>0) {

                //网口传输速度,单位为bps
                //curRate = (float) (inSize2 - inSize1 + outSize2 - outSize1) * 8 / (1000000 * interval);
                curRate = (float) (inSize2 - inSize1 + outSize2 - outSize1) / (1000 * interval);
                //netUsage = curRate / TotalBandwidth;
                //log.info("当前eth0网口接受和发送总速率为: " + curRate + "Mbps");
                //log.info("本节点网络带宽使用率为: " + netUsage);

                //log.info("当前eth0网口接受和发送总速率为: " + curRate + "KB/s");
            }
            in2.close();
            pro2.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("NetUsage发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        //return netUsage;
        //网口传输速度,单位为KB/s
        return curRate;
    }

    public ArrayList<Float> get() {
        //log.info("开始收集网络带宽使用率");
        //float netUsage = 0.0f;
        ArrayList<Float> list = new ArrayList<>();
        float curInRate = 0.0f;
        float curOutRate = 0.0f;
        Process pro1, pro2;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "cat /proc/net/dev";
            //第一次采集流量数据

            pro1 = r.exec(command);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            long inSize1 = 0, outSize1 = 0;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(config.NET_DEVICE)) {
                    String[] temp = line.split("\\s+");
                    inSize1 = Long.parseLong(temp[1]); //Receive bytes,单位为Byte
                    outSize1 = Long.parseLong(temp[9]);             //Transmit bytes,单位为Byte
                    //log.info("inSize1={},outSize1={}",inSize1,outSize1);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
            long startTime = System.currentTimeMillis();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.info("NetUsage休眠时发生InterruptedException. " + e.getMessage());
                log.error(sw.toString());
            }
            //第二次采集流量数据

            pro2 = r.exec(command);
            BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            long inSize2 = 0, outSize2 = 0;
            while ((line = in2.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(config.NET_DEVICE)) {
                    String[] temp = line.split("\\s+");
                    inSize2 = Long.parseLong(temp[1]);
                    outSize2 = Long.parseLong(temp[9]);
                    //log.info("inSize2={},outSize2={}",inSize2,outSize2);
                    break;
                }
            }
            long endTime = System.currentTimeMillis();
            float interval = (float) (endTime - startTime) / 1000;
            if (inSize1 != 0 && outSize1 != 0 && inSize2 != 0 && outSize2 != 0 && interval>0) {
                //网口传输速度,单位为bps
                curInRate = (float) (inSize2 - inSize1) / (1000 * interval);
                curOutRate = (float) (outSize2 - outSize1) / (1000 * interval);
            }
            list.add(curInRate);
            list.add(curOutRate);
            in2.close();
            pro2.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("NetUsage发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        //return netUsage;
        //网口传输速度,单位为KB/s
        return list;
    }

}