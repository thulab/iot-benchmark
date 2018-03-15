package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;

public class OpenFileNumber {

    private static Logger log = LoggerFactory.getLogger(OpenFileNumber.class);
    private Config config;
    private static int pid = -1;
    private static int port = -1;

    private static final String SEARCH_PID = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private static String cmds[] = {"/bin/bash", "-c", ""};
    //private static String passward = "";

    private static class OpenFileNumberHolder {
        private static final OpenFileNumber INSTANCE = new OpenFileNumber();
    }

    private OpenFileNumber() {
        config = ConfigDescriptor.getInstance().getConfig();
        pid = getPID(config.DB_SWITCH);
    }

    public static final OpenFileNumber getInstance() {
        return OpenFileNumberHolder.INSTANCE;
    }

    /**
     * @param
     * @return int, pid
     * @Purpose:获得当前指定的数据库服务器的PID
     */
    public int getPID(String dbName) {
        int pid = -1;
        Process pro1;
        Runtime r = Runtime.getRuntime();
        String filter = "";
        switch (dbName) {
            case Constants.DB_IOT:
                filter = "IOTDB_HOME";
                break;
            case Constants.DB_INFLUX:
                filter = "/usr/bin/influxd";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "iotdb-benchmark";
                break;
        }
        try {
            String command = String.format(SEARCH_PID, filter);
            //System.out.println(command);
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                //System.out.println(line);
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("统计打开文件数时getPid()发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        return pid;
    }

    /**
     * 返回打开的文件数目的列表，
     * 其中:
     * list[0]表示该进程一共打开的文件数目，
     * 1ist[1]表示该进程打开的数据文件和写前日志的数目
     * 1ist[2]表示该进程打开的socket的数目
     * <p>
     * list[3]表示该进程打开delta文件的数目
     * 1ist[4]表示该进程打开derby文件的数目
     * 1ist[5]表示该进程打开digest文件的数目
     * 1ist[6]表示该进程打开metadata文件的数目
     * 1ist[7]表示该进程打开overflow文件的数目
     * 1ist[8]表示该进程打开wals文件的数目
     */
    private ArrayList<Integer> getOpenFile(int pid) throws SQLException {
        //log.info("开始收集打开的socket数目：");
        ArrayList<Integer> list = new ArrayList<Integer>();
        int dataFileNum = 0;
        int totalFileNum = 0;
        int socketNum = 0;

        int deltaNum = 0;
        int derbyNum = 0;
        int digestNum = 0;
        int metadataNum = 0;
        int overflowNum = 0;
        int walsNum = 0;

        String filter = "";
        switch (config.DB_SWITCH) {
            case Constants.DB_IOT:
                filter = "/data/";
                break;
            case Constants.DB_INFLUX:
                filter = ".influxdb";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "iotdb-benchmark";
                break;
            default:
                throw new SQLException("unsupported db name :" + config.DB_SWITCH);
        }
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
            cmds[2] = command;
            pro = r.exec(cmds);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;

            while ((line = in.readLine()) != null) {
                //System.out.println(line);
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    totalFileNum++;
                    if (temp[8].contains(filter)) {
                        dataFileNum++;
                        if (temp[8].contains("delta")) {
                            deltaNum++;
                        } else if (temp[8].contains("derby")) {
                            derbyNum++;
                        } else if (temp[8].contains("digest")) {
                            digestNum++;
                        } else if (temp[8].contains("metadata")) {
                            metadataNum++;
                        } else if (temp[8].contains("overflow")) {
                            overflowNum++;
                        } else if (temp[8].contains("wals")) {
                            walsNum++;
                        }
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        socketNum++;
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("统计打开文件数时getOpenFile()发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        list.add(totalFileNum);
        list.add(dataFileNum);
        list.add(socketNum);

        list.add(deltaNum);
        list.add(derbyNum);
        list.add(digestNum);
        list.add(metadataNum);
        list.add(overflowNum);
        list.add(walsNum);
        return list;
    }

    /**
     * 返回打开的文件数目的列表，其中：
     * list[0]表示该进程一共打开的文件数目
     * 1ist[1]表示该进程打开的数据文件和写前日志的数目
     * 1ist[2]表示该进程打开的socket的数目
     * <p>
     * list[3]表示该进程打开delta文件的数目，
     * 1ist[4]表示该进程打开derby文件的数目
     * 1ist[5]表示该进程打开digest文件的数目
     * 1ist[6]表示该进程打开metadata文件的数目
     * 1ist[7]表示该进程打开overflow文件的数目
     * 1ist[8]表示该进程打开wals文件的数目
     */
    public ArrayList<Integer> get() {
        //System.out.println("port :"+port + " ; pid :"+pid);

        ArrayList<Integer> list = null;
        //如果port和pid不合理，再次尝试获取
        if (pid < 0) {
            pid = getPid();
        }
        //如果pid合理，则加入打开文件总数和数据文件数目以及socket数目
        if (pid > 0) {
            try {
                list = getOpenFile(pid);
            } catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        } else {
            //pid 不合理，赋不合法的值
            if (list == null)
                list = new ArrayList<Integer>();
            for (int i = 0; i < 9; i++) {
                list.add(-1);
            }
        }
        return list;
    }

    //检验一个字符串是否是整数
    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public int getPid() {
        return pid;
    }


}
